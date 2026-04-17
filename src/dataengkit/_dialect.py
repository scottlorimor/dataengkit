"""SQLGlot-based SQL dialect transpilation.

dialect="duckdb" is a zero-cost passthrough — no SQLGlot call, no syntax normalization.
All other dialects go through SQLGlot. "spark" and "databricks" both map to SQLGlot's
"databricks" dialect internally.
"""

from __future__ import annotations

from dataengkit._exceptions import DialectTranspilationError

# Closed set of supported dialects. Unknown dialects raise DialectTranspilationError.
SUPPORTED_DIALECTS: frozenset[str] = frozenset(
    {"duckdb", "snowflake", "bigquery", "spark", "databricks", "postgres"}
)

# Both "spark" and "databricks" map to SQLGlot's "databricks" dialect.
_SQLGLOT_DIALECT_MAP: dict[str, str] = {
    "spark": "databricks",
    "databricks": "databricks",
}


def transpile(sql: str, dialect: str) -> str:
    """Transpile DuckDB SQL to the target dialect.

    Args:
        sql: A valid DuckDB SQL string.
        dialect: Target dialect. Must be in SUPPORTED_DIALECTS.

    Returns:
        SQL string in the target dialect.

    Raises:
        DialectTranspilationError: If dialect is unsupported or SQLGlot fails.
    """
    if dialect == "duckdb":
        return sql  # passthrough — no SQLGlot, no normalization

    if dialect not in SUPPORTED_DIALECTS:
        raise DialectTranspilationError(
            dialect=dialect,
            original_sql=sql,
            cause=ValueError(
                f"Unknown dialect: '{dialect}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_DIALECTS))}"
            ),
        )

    target = _SQLGLOT_DIALECT_MAP.get(dialect, dialect)

    try:
        import sqlglot

        results = sqlglot.transpile(sql, read="duckdb", write=target)
        if not results:
            raise DialectTranspilationError(
                dialect=dialect,
                original_sql=sql,
                cause=ValueError("SQLGlot returned empty output for this SQL"),
            )
        return results[0]
    except Exception as exc:
        raise DialectTranspilationError(
            dialect=dialect, original_sql=sql, cause=exc
        ) from exc
