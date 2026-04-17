"""Shared pytest fixtures for data-kit tests."""

from __future__ import annotations

import pandas as pd
import pytest


@pytest.fixture
def events_sample() -> pd.DataFrame:
    """Minimal events DataFrame for retention testing.

    Schema: user_id (str), event_name (str), timestamp (datetime), signup_date (date).
    Covers 3 cohorts, 10 users each, with realistic retention dropoff.
    """
    import numpy as np

    rng = np.random.default_rng(42)
    rows = []

    cohorts = pd.date_range("2024-01-01", periods=3, freq="MS")
    for cohort_date in cohorts:
        for user_idx in range(10):
            user_id = f"u_{cohort_date.strftime('%Y%m')}_{user_idx:02d}"
            # D0 — everyone has signup event
            rows.append({
                "user_id": user_id,
                "event_name": "active",
                "timestamp": cohort_date,
                "signup_date": cohort_date.date(),
            })
            # D7 — ~80% retention
            if rng.random() < 0.8:
                rows.append({
                    "user_id": user_id,
                    "event_name": "active",
                    "timestamp": cohort_date + pd.Timedelta(days=7),
                    "signup_date": cohort_date.date(),
                })
            # D30 — ~60% retention
            if rng.random() < 0.6:
                rows.append({
                    "user_id": user_id,
                    "event_name": "active",
                    "timestamp": cohort_date + pd.Timedelta(days=30),
                    "signup_date": cohort_date.date(),
                })

    return pd.DataFrame(rows)


@pytest.fixture
def subscriptions_sample() -> pd.DataFrame:
    """Minimal subscriptions DataFrame for MRR/investor metrics testing."""
    return pd.DataFrame({
        "subscription_id": ["s1", "s2", "s3", "s4"],
        "account_id": ["a1", "a1", "a2", "a3"],
        "start_date": pd.to_datetime(["2024-01-01", "2024-03-01", "2024-01-01", "2024-02-01"]),
        "end_date": pd.to_datetime([None, None, "2024-03-31", None]),
        "mrr": [500.0, 200.0, 300.0, 800.0],
    })
