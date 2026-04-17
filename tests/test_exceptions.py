"""Tests for _exceptions.py — exception constructor contracts."""

from __future__ import annotations

import pytest

from dataengkit._exceptions import (
    DeltaConcurrentWriteError,
    DeltaWriteError,
    SCDGrainError,
    SQLRenderError,
    ValidationError,
)


def test_validation_error_lists_all_violations() -> None:
    err = ValidationError(violations=["missing user_id", "null timestamp"])
    assert "missing user_id" in str(err)
    assert "null timestamp" in str(err)
    assert err.violations == ["missing user_id", "null timestamp"]


def test_validation_error_single_violation() -> None:
    err = ValidationError(violations=["missing event_name"])
    assert "missing event_name" in str(err)


def test_sql_render_error_includes_sql() -> None:
    cause = RuntimeError("syntax error")
    err = SQLRenderError(sql="SELECT broken FROM", cause=cause)
    assert "SELECT broken FROM" in str(err)
    assert err.sql == "SELECT broken FROM"
    assert err.cause is cause


def test_delta_write_error_includes_path_and_mode() -> None:
    cause = OSError("permission denied")
    err = DeltaWriteError(path="/data/delta/table", mode="overwrite", cause=cause)
    assert "/data/delta/table" in str(err)
    assert "overwrite" in str(err)
    assert err.path == "/data/delta/table"
    assert err.mode == "overwrite"


def test_delta_concurrent_write_error_includes_path() -> None:
    err = DeltaConcurrentWriteError(path="/data/delta/table")
    assert "/data/delta/table" in str(err)
    assert err.path == "/data/delta/table"


def test_scd_grain_error_includes_natural_key_and_timestamp() -> None:
    err = SCDGrainError(natural_key="account_123", effective_from="2024-01-01")
    assert "account_123" in str(err)
    assert "2024-01-01" in str(err)
