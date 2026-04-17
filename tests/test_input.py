"""Tests for _input.py — input resolution dispatch."""

from __future__ import annotations

import pathlib

import pandas as pd
import pytest

from dataengkit._input import resolve_input


def test_pandas_passthrough() -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = resolve_input(df)
    assert result is df


def test_unknown_type_raises_typeerror() -> None:
    with pytest.raises(TypeError, match="Expected pd.DataFrame"):
        resolve_input(42)  # type: ignore[arg-type]


def test_unknown_type_error_message_includes_type() -> None:
    with pytest.raises(TypeError) as exc_info:
        resolve_input([1, 2, 3])  # type: ignore[arg-type]
    assert "list" in str(exc_info.value)


def test_sql_string_dispatches_to_duckdb() -> None:
    """A SELECT string is executed via DuckDB, not treated as a Delta path."""
    result = resolve_input("SELECT 1 AS n, 'hello' AS s")
    assert isinstance(result, pd.DataFrame)
    assert result["n"].iloc[0] == 1


def test_sql_string_with_prefix() -> None:
    result = resolve_input("  SELECT 42 AS answer")
    assert result["answer"].iloc[0] == 42


def test_invalid_delta_path_raises_delta_read_error() -> None:
    from dataengkit._exceptions import DeltaReadError

    with pytest.raises(DeltaReadError):
        resolve_input("/nonexistent/path/to/delta")


def test_invalid_delta_path_object_raises_delta_read_error() -> None:
    from dataengkit._exceptions import DeltaReadError

    with pytest.raises(DeltaReadError):
        resolve_input(pathlib.Path("/nonexistent/delta"))
