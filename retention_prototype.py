"""Retention analysis prototype — all four components in one file.

Usage:
    python retention_prototype.py

Flow:
    EventSchema.validate(raw)  →  Dedup.to_df()  →  CohortMatrix.to_df()  →  RetentionCurve.to_df()
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pandas as pd

from dataengkit._base import Renderable
from dataengkit._dialect import transpile
from dataengkit._exceptions import ValidationError


# ---------------------------------------------------------------------------
# EventSchema
# ---------------------------------------------------------------------------

class EventSchema:
    """Validates a raw events DataFrame against a declared schema.

    Args:
        required_columns: Column names that must be present and non-null.
        allowed_events:   If provided, the event_name column must contain only
                          these values. Pass None to allow anything.
        event_col:        Name of the event name column (default: "event_name").
        allow_extra_events: If allowed_events is set, whether unknown values are
                            allowed (default: False).

    Usage:
        schema = EventSchema(
            required_columns=["user_id", "event_name", "timestamp"],
            allowed_events=["signup", "activated", "payment_succeeded"],
        )
        schema.validate(raw_df)   # raises ValidationError listing all violations
    """

    def __init__(
        self,
        required_columns: list[str],
        allowed_events: list[str] | None = None,
        event_col: str = "event_name",
        allow_extra_events: bool = False,
    ) -> None:
        self.required_columns = required_columns
        self.allowed_events = allowed_events
        self.event_col = event_col
        self.allow_extra_events = allow_extra_events

    def validate(self, df: pd.DataFrame) -> None:
        """Validate df. Raises ValidationError listing ALL violations (not fail-fast)."""
        violations: list[str] = []

        # 1. Required columns present
        missing_cols = [c for c in self.required_columns if c not in df.columns]
        for c in missing_cols:
            violations.append(f"Missing required column: '{c}'")

        # 2. Required columns non-null (only for columns that exist)
        present_required = [c for c in self.required_columns if c in df.columns]
        for c in present_required:
            null_count = df[c].isna().sum()
            if null_count > 0:
                violations.append(f"Column '{c}' has {null_count} null value(s)")

        # 3. Allowed events check (only if event_col exists)
        if self.allowed_events is not None and not self.allow_extra_events:
            if self.event_col in df.columns:
                unknown = set(df[self.event_col].dropna().unique()) - set(self.allowed_events)
                if unknown:
                    violations.append(
                        f"Unrecognized event names in '{self.event_col}': "
                        + ", ".join(f"'{e}'" for e in sorted(unknown))
                    )
            # missing event_col already caught above if it's in required_columns

        if violations:
            raise ValidationError(violations)


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

class Dedup(Renderable):
    """Deduplicates an events DataFrame by a set of id columns.

    Keeps the first occurrence when duplicates are found (stable sort).

    Args:
        df:      Input events DataFrame (or Delta path / SQL string).
        id_cols: Column(s) that together form the unique row identity.

    Usage:
        clean = Dedup(raw_df, id_cols=["event_id"]).to_df()
    """

    def __init__(self, df: pd.DataFrame, *, id_cols: str | list[str]) -> None:
        self._df = df
        self._id_cols: list[str] = [id_cols] if isinstance(id_cols, str) else list(id_cols)

        missing = [c for c in self._id_cols if c not in df.columns]
        if missing:
            raise ValidationError([f"id_col not found in DataFrame: '{c}'" for c in missing])

    def to_df(self) -> pd.DataFrame:
        return self._df.drop_duplicates(subset=self._id_cols, keep="first").reset_index(drop=True)

    def to_sql(self, source_table: str, dialect: str = "duckdb") -> str:
        id_list = ", ".join(self._id_cols)
        sql = f"""
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {id_list} ORDER BY (SELECT NULL)) AS _rn
    FROM {source_table}
)
SELECT * EXCLUDE (_rn)
FROM ranked
WHERE _rn = 1
""".strip()
        return transpile(sql, dialect)

    @classmethod
    def example(cls) -> pd.DataFrame:
        return pd.DataFrame({
            "event_id": ["e1", "e2", "e2", "e3"],
            "user_id":  ["u1", "u2", "u2", "u3"],
            "event_name": ["signup", "activated", "activated", "payment_succeeded"],
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03"]),
        })

    def __repr__(self) -> str:
        return f"Dedup(id_cols={self._id_cols!r}, rows={len(self._df)})"


# ---------------------------------------------------------------------------
# CohortMatrix
# ---------------------------------------------------------------------------

class CohortMatrix(Renderable):
    """Row-level cohort membership table: one row per (user, period).

    Output columns:
        user_col       — user identifier
        cohort_date    — the user's signup/cohort date (date-truncated)
        period         — number of days since cohort date (always includes 0)
        had_event      — bool: did the user have any event in this period?

    Period 0 is always True for every user (they signed up).
    Periods beyond the user's cohort age are excluded (not NaN, not False).
    Multi-events on the same day are deduplicated before counting.

    Args:
        df:              Events DataFrame.
        user_col:        Column with user identifier.
        event_date_col:  Column with event date (date or datetime).
        signup_date_col: Column with user signup/cohort date.
        periods:         Day offsets to compute. 0 is always forced in.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        *,
        user_col: str,
        event_date_col: str,
        signup_date_col: str,
        periods: list[int] = [0, 7, 14, 30, 60, 90],
    ) -> None:
        for col in (user_col, event_date_col, signup_date_col):
            if col not in df.columns:
                raise ValidationError([f"Column not found: '{col}'"])

        if df[signup_date_col].isna().any():
            raise ValidationError([f"Column '{signup_date_col}' contains null values"])

        self._df = df.copy()
        self._user_col = user_col
        self._event_date_col = event_date_col
        self._signup_date_col = signup_date_col

        # Force period 0 in
        self._periods = sorted(set([0] + [p for p in periods if p >= 0]))

    def to_df(self) -> pd.DataFrame:
        df = self._df.copy()

        # Normalize to date
        df[self._event_date_col] = pd.to_datetime(df[self._event_date_col]).dt.normalize()
        df[self._signup_date_col] = pd.to_datetime(df[self._signup_date_col]).dt.normalize()

        # Warn and drop rows where event_date < signup_date
        bad = df[self._event_date_col] < df[self._signup_date_col]
        if bad.any():
            warnings.warn(
                f"{bad.sum()} row(s) where event_date < signup_date — skipped.",
                UserWarning,
                stacklevel=2,
            )
            df = df[~bad]

        if df.empty:
            return pd.DataFrame(columns=[self._user_col, "cohort_date", "period", "had_event"])

        # Deduplicate events: one row per (user, event_date)
        df = df.drop_duplicates(subset=[self._user_col, self._event_date_col])

        # Cohort date = signup date (date-truncated, per-user)
        cohort_dates = (
            df[[self._user_col, self._signup_date_col]]
            .drop_duplicates(subset=[self._user_col])
            .rename(columns={self._signup_date_col: "cohort_date"})
        )

        today = pd.Timestamp.now().normalize()

        rows = []
        for _, user_row in cohort_dates.iterrows():
            user = user_row[self._user_col]
            cohort_date = user_row["cohort_date"]

            user_events = df[df[self._user_col] == user][self._event_date_col]
            event_dates = set(user_events)

            for period in self._periods:
                target_date = cohort_date + pd.Timedelta(days=period)
                # Skip periods beyond cohort age
                if target_date > today:
                    continue
                had_event = target_date in event_dates or period == 0
                rows.append({
                    self._user_col: user,
                    "cohort_date": cohort_date,
                    "period": period,
                    "had_event": had_event,
                })

        return pd.DataFrame(rows)

    def to_sql(self, source_table: str, dialect: str = "duckdb") -> str:
        periods_list = ", ".join(str(p) for p in self._periods)
        sql = f"""
WITH periods AS (
    SELECT unnest([{periods_list}]) AS period
),
cohorts AS (
    SELECT
        {self._user_col},
        DATE_TRUNC('day', MIN({self._signup_date_col})) AS cohort_date
    FROM {source_table}
    GROUP BY {self._user_col}
),
user_periods AS (
    SELECT
        c.{self._user_col},
        c.cohort_date,
        p.period,
        c.cohort_date + INTERVAL (p.period * 1) DAY AS target_date
    FROM cohorts c
    CROSS JOIN periods p
),
event_days AS (
    SELECT DISTINCT
        {self._user_col},
        DATE_TRUNC('day', {self._event_date_col}) AS event_date
    FROM {source_table}
    WHERE {self._event_date_col} >= {self._signup_date_col}
)
SELECT
    up.{self._user_col},
    up.cohort_date,
    up.period,
    CASE
        WHEN up.period = 0 THEN TRUE
        WHEN ed.event_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS had_event
FROM user_periods up
LEFT JOIN event_days ed
    ON up.{self._user_col} = ed.{self._user_col}
    AND up.target_date = ed.event_date
WHERE up.target_date <= CURRENT_DATE
ORDER BY up.cohort_date, up.period, up.{self._user_col}
""".strip()
        return transpile(sql, dialect)

    @classmethod
    def example(cls) -> pd.DataFrame:
        return pd.DataFrame({
            "user_id": ["u1", "u1", "u2", "u2", "u3"],
            "event_name": ["signup", "activated", "signup", "activated", "signup"],
            "event_date": pd.to_datetime(["2024-01-01", "2024-01-08", "2024-01-01", "2024-01-15", "2024-02-01"]),
            "signup_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01", "2024-02-01"]),
        })

    def __repr__(self) -> str:
        return f"CohortMatrix(periods={self._periods!r})"


# ---------------------------------------------------------------------------
# RetentionCurve
# ---------------------------------------------------------------------------

class RetentionCurve(Renderable):
    """Cohort-level retention percentages over time.

    Aggregates CohortMatrix output into one row per (cohort_date, period).

    Output columns:
        cohort_date    — cohort date
        period         — day offset
        cohort_size    — number of users in the cohort
        retained       — number of users who had an event on this period
        retained_pct   — retained / cohort_size * 100 (D0 always 100.0)

    Args:
        df:              Events DataFrame (same input as CohortMatrix).
        user_col:        Column with user identifier.
        event_date_col:  Column with event date.
        signup_date_col: Column with signup/cohort date.
        periods:         Day offsets to compute.
    """

    def __init__(
        self,
        df: pd.DataFrame,
        *,
        user_col: str,
        event_date_col: str,
        signup_date_col: str,
        periods: list[int] = [0, 7, 14, 30, 60, 90],
    ) -> None:
        self._matrix = CohortMatrix(
            df,
            user_col=user_col,
            event_date_col=event_date_col,
            signup_date_col=signup_date_col,
            periods=periods,
        )
        self._user_col = user_col

    def to_df(self) -> pd.DataFrame:
        matrix = self._matrix.to_df()

        if matrix.empty:
            return pd.DataFrame(
                columns=["cohort_date", "period", "cohort_size", "retained", "retained_pct"]
            )

        cohort_sizes = (
            matrix[matrix["period"] == 0]
            .groupby("cohort_date")[self._user_col]
            .count()
            .rename("cohort_size")
        )

        agg = (
            matrix.groupby(["cohort_date", "period"])
            .agg(retained=(self._user_col, "count"), had_event=("had_event", "sum"))
            .reset_index()
        )

        agg = agg.merge(cohort_sizes, on="cohort_date")
        agg["retained_pct"] = (agg["had_event"] / agg["cohort_size"] * 100).round(1)

        return agg[["cohort_date", "period", "cohort_size", "had_event", "retained_pct"]].rename(
            columns={"had_event": "retained"}
        )

    def cohort_sizes(self) -> pd.Series:
        """Return cohort sizes as a Series indexed by cohort_date."""
        matrix = self._matrix.to_df()
        sizes = (
            matrix[matrix["period"] == 0]
            .groupby("cohort_date")[self._user_col]
            .count()
        )
        sizes.name = "cohort_size"
        return sizes

    def to_sql(self, source_table: str, dialect: str = "duckdb") -> str:
        matrix_sql = self._matrix.to_sql(source_table, dialect="duckdb")
        sql = f"""
WITH matrix AS (
{matrix_sql}
),
cohort_sizes AS (
    SELECT cohort_date, COUNT({self._user_col}) AS cohort_size
    FROM matrix
    WHERE period = 0
    GROUP BY cohort_date
)
SELECT
    m.cohort_date,
    m.period,
    cs.cohort_size,
    SUM(CASE WHEN m.had_event THEN 1 ELSE 0 END) AS retained,
    ROUND(
        SUM(CASE WHEN m.had_event THEN 1 ELSE 0 END) * 100.0 / cs.cohort_size,
        1
    ) AS retained_pct
FROM matrix m
JOIN cohort_sizes cs ON m.cohort_date = cs.cohort_date
GROUP BY m.cohort_date, m.period, cs.cohort_size
ORDER BY m.cohort_date, m.period
""".strip()
        return transpile(sql, dialect)

    @classmethod
    def example(cls) -> pd.DataFrame:
        return CohortMatrix.example()

    def __repr__(self) -> str:
        return f"RetentionCurve(periods={self._matrix._periods!r})"


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Synthetic events: 3 users, 2 cohorts
    raw = pd.DataFrame({
        "event_id":   ["e1", "e2", "e3", "e4", "e5", "e6", "e7", "e7"],  # e7 duplicated
        "user_id":    ["u1", "u1", "u2", "u2", "u3", "u3", "u3", "u3"],
        "event_name": ["signup", "activated", "signup", "activated", "signup", "activated", "activated", "activated"],
        "timestamp":  pd.to_datetime([
            "2024-01-01", "2024-01-08",   # u1: cohort Jan 1, returns D7
            "2024-01-01", "2024-01-31",   # u2: cohort Jan 1, returns D30
            "2024-02-01", "2024-02-08",   # u3: cohort Feb 1, returns D7
            "2024-02-08", "2024-02-08",   # u3: duplicate events same day
        ]),
        "signup_date": pd.to_datetime([
            "2024-01-01", "2024-01-01",
            "2024-01-01", "2024-01-01",
            "2024-02-01", "2024-02-01",
            "2024-02-01", "2024-02-01",
        ]),
    })

    print("=" * 60)
    print("1. EventSchema.validate()")
    print("=" * 60)
    schema = EventSchema(
        required_columns=["user_id", "event_name", "timestamp", "signup_date"],
        allowed_events=["signup", "activated", "payment_succeeded"],
    )
    schema.validate(raw)
    print("  OK — no violations\n")

    print("=" * 60)
    print("2. Dedup.to_df()  (deduplicate on event_id)")
    print("=" * 60)
    clean = Dedup(raw, id_cols="event_id").to_df()
    print(clean[["event_id", "user_id", "event_name", "timestamp"]].to_string(index=False))
    print()

    print("=" * 60)
    print("3. CohortMatrix.to_df()")
    print("=" * 60)
    matrix = CohortMatrix(
        clean,
        user_col="user_id",
        event_date_col="timestamp",
        signup_date_col="signup_date",
        periods=[0, 7, 14, 30],
    )
    print(matrix.to_df().to_string(index=False))
    print()

    print("=" * 60)
    print("4. RetentionCurve.to_df()")
    print("=" * 60)
    curve = RetentionCurve(
        clean,
        user_col="user_id",
        event_date_col="timestamp",
        signup_date_col="signup_date",
        periods=[0, 7, 14, 30],
    )
    print(curve.to_df().to_string(index=False))
    print()

    print("=" * 60)
    print("5. RetentionCurve.to_sql(dialect='snowflake')  [first 10 lines]")
    print("=" * 60)
    sql = curve.to_sql("events", dialect="snowflake")
    for line in sql.splitlines()[:10]:
        print(" ", line)
    print("  ...")
