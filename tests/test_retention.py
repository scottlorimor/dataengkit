"""Retention analysis end-to-end tests using a synthetic Mixpanel export.

The fixture shape mirrors Mixpanel raw exports joined with user properties:
  distinct_id, $insert_id, event, time, $signup_date, mp_country_code, $os, plan

Column mapping for our components:
  user_col        = "distinct_id"
  event_date_col  = "time"
  signup_date_col = "$signup_date"
  id_cols         = "$insert_id"    (Mixpanel's stable dedup key)
"""

from __future__ import annotations

import pytest
import pandas as pd

from tests.fixtures.mixpanel import make_mixpanel_export

# Import prototype components (move to proper modules once structure is finalised)
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from retention_prototype import EventSchema, Dedup, CohortMatrix, RetentionCurve


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def raw() -> pd.DataFrame:
    return make_mixpanel_export(seed=42)


@pytest.fixture(scope="module")
def clean(raw: pd.DataFrame) -> pd.DataFrame:
    return Dedup(raw, id_cols="$insert_id").to_df()


@pytest.fixture(scope="module")
def matrix(clean: pd.DataFrame) -> CohortMatrix:
    return CohortMatrix(
        clean,
        user_col="distinct_id",
        event_date_col="time",
        signup_date_col="$signup_date",
        periods=[0, 7, 14, 30],
    )


@pytest.fixture(scope="module")
def curve(clean: pd.DataFrame) -> RetentionCurve:
    return RetentionCurve(
        clean,
        user_col="distinct_id",
        event_date_col="time",
        signup_date_col="$signup_date",
        periods=[0, 7, 14, 30],
    )


# ---------------------------------------------------------------------------
# EventSchema
# ---------------------------------------------------------------------------

class TestEventSchema:
    def test_valid_export_passes(self, raw: pd.DataFrame) -> None:
        schema = EventSchema(
            required_columns=["distinct_id", "event", "time", "$signup_date"],
            allowed_events=["Sign Up", "Onboarding Completed", "Feature Used"],
        )
        schema.validate(raw)  # must not raise

    def test_missing_column_raises(self, raw: pd.DataFrame) -> None:
        from dataengkit._exceptions import ValidationError
        schema = EventSchema(required_columns=["distinct_id", "missing_col"])
        with pytest.raises(ValidationError) as exc_info:
            schema.validate(raw)
        assert "missing_col" in str(exc_info.value)

    def test_unknown_event_raises(self, raw: pd.DataFrame) -> None:
        from dataengkit._exceptions import ValidationError
        schema = EventSchema(
            required_columns=["distinct_id"],
            allowed_events=["Sign Up"],  # excludes real events in the fixture
            event_col="event",
        )
        with pytest.raises(ValidationError) as exc_info:
            schema.validate(raw)
        assert "Feature Used" in str(exc_info.value) or "Onboarding Completed" in str(exc_info.value)

    def test_all_violations_reported_at_once(self) -> None:
        from dataengkit._exceptions import ValidationError
        df = pd.DataFrame({"a": [1, None]})
        schema = EventSchema(required_columns=["a", "b", "c"])
        with pytest.raises(ValidationError) as exc_info:
            schema.validate(df)
        msg = str(exc_info.value)
        assert "b" in msg
        assert "c" in msg


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

class TestDedup:
    def test_removes_duplicates(self, raw: pd.DataFrame) -> None:
        clean = Dedup(raw, id_cols="$insert_id").to_df()
        assert clean["$insert_id"].is_unique

    def test_dedup_reduces_row_count(self, raw: pd.DataFrame) -> None:
        clean = Dedup(raw, id_cols="$insert_id").to_df()
        # fixture injects ~10% dupes
        assert len(clean) < len(raw)

    def test_dedup_preserves_all_unique_rows(self, raw: pd.DataFrame) -> None:
        clean = Dedup(raw, id_cols="$insert_id").to_df()
        assert len(clean) == raw["$insert_id"].nunique()

    def test_missing_id_col_raises(self, raw: pd.DataFrame) -> None:
        from dataengkit._exceptions import ValidationError
        with pytest.raises(ValidationError):
            Dedup(raw, id_cols="nonexistent_col")

    def test_to_sql_contains_source_table(self, clean: pd.DataFrame) -> None:
        sql = Dedup(clean, id_cols="$insert_id").to_sql("mp_events")
        assert "mp_events" in sql


# ---------------------------------------------------------------------------
# CohortMatrix
# ---------------------------------------------------------------------------

class TestCohortMatrix:
    def test_d0_always_true(self, matrix: CohortMatrix) -> None:
        df = matrix.to_df()
        d0 = df[df["period"] == 0]
        assert d0["had_event"].all(), "Every user must have had_event=True at D0"

    def test_three_cohorts_present(self, matrix: CohortMatrix) -> None:
        df = matrix.to_df()
        assert df["cohort_date"].nunique() == 3

    def test_ten_users_per_cohort(self, matrix: CohortMatrix) -> None:
        df = matrix.to_df()
        d0 = df[df["period"] == 0]
        cohort_counts = d0.groupby("cohort_date")["distinct_id"].count()
        assert (cohort_counts == 10).all(), f"Expected 10 users per cohort:\n{cohort_counts}"

    def test_periods_present(self, matrix: CohortMatrix) -> None:
        df = matrix.to_df()
        assert set(df["period"].unique()) ==w {0, 7, 14, 30}

    def test_period_0_forced_in_even_if_omitted(self, clean: pd.DataFrame) -> None:
        m = CohortMatrix(
            clean,
            user_col="distinct_id",
            event_date_col="time",
            signup_date_col="$signup_date",
            periods=[7, 14, 30],  # 0 omitted intentionally
        )
        df = m.to_df()
        assert 0 in df["period"].unique()

    def test_no_nan_in_had_event(self, matrix: CohortMatrix) -> None:
        df = matrix.to_df()
        assert not df["had_event"].isna().any()

    def test_missing_column_raises(self, clean: pd.DataFrame) -> None:
        from dataengkit._exceptions import ValidationError
        with pytest.raises(ValidationError):
            CohortMatrix(clean, user_col="distinct_id", event_date_col="time", signup_date_col="no_such_col")

    def test_to_sql_contains_source_table(self, matrix: CohortMatrix) -> None:
        sql = matrix.to_sql("mp_events")
        assert "mp_events" in sql

    def test_empty_dataframe_returns_empty(self, clean: pd.DataFrame) -> None:
        empty = clean.iloc[0:0].copy()
        m = CohortMatrix(
            empty,
            user_col="distinct_id",
            event_date_col="time",
            signup_date_col="$signup_date",
        )
        result = m.to_df()
        assert result.empty


# ---------------------------------------------------------------------------
# RetentionCurve
# ---------------------------------------------------------------------------

class TestRetentionCurve:
    def test_d0_retention_always_100(self, curve: RetentionCurve) -> None:
        df = curve.to_df()
        d0 = df[df["period"] == 0]
        assert (d0["retained_pct"] == 100.0).all()

    def test_three_cohorts_present(self, curve: RetentionCurve) -> None:
        df = curve.to_df()
        assert df["cohort_date"].nunique() == 3

    def test_cohort_size_is_ten(self, curve: RetentionCurve) -> None:
        df = curve.to_df()
        assert (df["cohort_size"] == 10).all()

    def test_retained_lte_cohort_size(self, curve: RetentionCurve) -> None:
        df = curve.to_df()
        assert (df["retained"] <= df["cohort_size"]).all()

    def test_d7_retention_in_plausible_range(self, curve: RetentionCurve) -> None:
        df = curve.to_df()
        d7 = df[df["period"] == 7]["retained_pct"]
        # fixture targets ~70%; allow 40-100% given 10-user cohorts
        assert (d7 >= 40).all() and (d7 <= 100).all(), f"D7 retention out of range:\n{d7}"

    def test_d30_lte_d7_on_average(self, curve: RetentionCurve) -> None:
        df = curve.to_df()
        avg_d7 = df[df["period"] == 7]["retained_pct"].mean()
        avg_d30 = df[df["period"] == 30]["retained_pct"].mean()
        assert avg_d30 <= avg_d7, "Average D30 retention should not exceed D7"

    def test_cohort_sizes_series_name(self, curve: RetentionCurve) -> None:
        sizes = curve.cohort_sizes()
        assert sizes.name == "cohort_size"

    def test_cohort_sizes_indexed_by_cohort_date(self, curve: RetentionCurve) -> None:
        sizes = curve.cohort_sizes()
        assert sizes.index.name == "cohort_date"
        assert len(sizes) == 3

    def test_to_sql_contains_source_table(self, curve: RetentionCurve) -> None:
        sql = curve.to_sql("mp_events")
        assert "mp_events" in sql

    def test_to_sql_snowflake_transpiles(self, curve: RetentionCurve) -> None:
        sql = curve.to_sql("mp_events", dialect="snowflake")
        assert len(sql) > 0
        assert "mp_events" in sql

    def test_empty_dataframe_returns_empty(self, clean: pd.DataFrame) -> None:
        empty = clean.iloc[0:0].copy()
        c = RetentionCurve(
            empty,
            user_col="distinct_id",
            event_date_col="time",
            signup_date_col="$signup_date",
        )
        assert c.to_df().empty
