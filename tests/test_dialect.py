"""Tests for _dialect.py — SQL transpilation."""

from __future__ import annotations

import pytest

from datakit._dialect import SUPPORTED_DIALECTS, transpile
from datakit._exceptions import DialectTranspilationError

SIMPLE_SQL = "SELECT user_id, COUNT(*) AS cnt FROM events GROUP BY user_id"


def test_duckdb_passthrough() -> None:
    """dialect=duckdb returns the SQL unchanged — no SQLGlot call."""
    result = transpile(SIMPLE_SQL, "duckdb")
    assert result == SIMPLE_SQL


def test_all_supported_dialects_accepted() -> None:
    """Every dialect in SUPPORTED_DIALECTS must not raise on simple SQL."""
    for dialect in SUPPORTED_DIALECTS:
        result = transpile(SIMPLE_SQL, dialect)
        assert isinstance(result, str)
        assert len(result) > 0


def test_unknown_dialect_raises() -> None:
    with pytest.raises(DialectTranspilationError) as exc_info:
        transpile(SIMPLE_SQL, "oracle")
    assert "oracle" in str(exc_info.value)
    assert "Supported:" in str(exc_info.value)


def test_spark_and_databricks_both_accepted() -> None:
    """Both 'spark' and 'databricks' are accepted aliases."""
    result_spark = transpile(SIMPLE_SQL, "spark")
    result_databricks = transpile(SIMPLE_SQL, "databricks")
    assert isinstance(result_spark, str)
    assert isinstance(result_databricks, str)
