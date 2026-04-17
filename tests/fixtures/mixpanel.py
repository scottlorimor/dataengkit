"""Synthetic Mixpanel event export.

Mimics the shape of a Mixpanel raw export joined with user properties:

  distinct_id      — Mixpanel's user identifier
  $insert_id       — unique per-event ID used for deduplication
  event            — event name (Mixpanel's "event" field, not "event_name")
  time             — UTC timestamp (Mixpanel exports as ISO 8601)
  $signup_date     — user property: date the user signed up (join from People)
  mp_country_code  — Mixpanel standard property
  $os              — Mixpanel standard property
  plan             — custom property set at signup

Retention scenario:
  3 cohorts (Jan, Feb, Mar 2024), 10 users each.
  D7 retention:  ~70%  (realistic SaaS early-stage)
  D30 retention: ~45%

Duplicates: ~10% of events are re-sent (Mixpanel at-least-once delivery).
  The $insert_id is stable across duplicates — correct dedup key.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def make_mixpanel_export(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    countries = ["US", "GB", "CA", "DE", "AU"]
    oses = ["iOS", "Android", "Web"]
    plans = ["free", "starter", "pro"]

    rows: list[dict] = []

    cohorts = pd.date_range("2024-01-01", periods=3, freq="MS")

    for cohort_date in cohorts:
        for user_idx in range(10):
            distinct_id = f"mp_{cohort_date.strftime('%Y%m')}_{user_idx:02d}"
            country = rng.choice(countries)
            os_ = rng.choice(oses)
            plan = rng.choice(plans)
            signup_date = cohort_date.date()

            def _event(name: str, ts: pd.Timestamp, extra_props: dict | None = None) -> dict:
                return {
                    "distinct_id": distinct_id,
                    "$insert_id": f"{rng.integers(0, 2**63):016x}{rng.integers(0, 2**63):016x}",
                    "event": name,
                    "time": ts.isoformat(),
                    "$signup_date": str(signup_date),
                    "mp_country_code": country,
                    "$os": os_,
                    "plan": plan,
                    **(extra_props or {}),
                }

            # D0 — Sign Up (every user)
            rows.append(_event("Sign Up", cohort_date))

            # D0 — Onboarding Completed (80% complete same day)
            if rng.random() < 0.80:
                rows.append(_event("Onboarding Completed", cohort_date + pd.Timedelta(hours=rng.integers(1, 6))))

            # D7 — Active (~70%)
            if rng.random() < 0.70:
                rows.append(_event("Feature Used", cohort_date + pd.Timedelta(days=7)))

            # D14 — Active (~55%)
            if rng.random() < 0.55:
                rows.append(_event("Feature Used", cohort_date + pd.Timedelta(days=14)))

            # D30 — Active (~45%)
            if rng.random() < 0.45:
                rows.append(_event("Feature Used", cohort_date + pd.Timedelta(days=30)))

    # Inject ~10% duplicates — same event, same $insert_id, re-sent by Mixpanel
    originals = rows.copy()
    n_dupes = max(1, int(len(originals) * 0.10))
    dupe_indices = rng.choice(len(originals), size=n_dupes, replace=False)
    for idx in dupe_indices:
        rows.append(originals[idx].copy())

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"])
    df["$signup_date"] = pd.to_datetime(df["$signup_date"])
    return df.sample(frac=1, random_state=seed).reset_index(drop=True)  # shuffle like a real export
