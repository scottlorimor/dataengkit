# Dialect Compatibility

`dataengkit` uses [SQLGlot](https://github.com/tobymao/sqlglot) to transpile DuckDB SQL to other warehouse dialects. Most standard SQL works across all dialects. This document tracks known limitations.

## Known limitations

### DuckDB-specific functions (no cross-dialect equivalent)

| Function | Issue | Workaround |
|----------|-------|-----------|
| `LIST_AGG` / `ARRAY_AGG` | Syntax differs across dialects | SQLGlot handles most cases; `ARRAY_AGG(... ORDER BY ...)` may fail on some targets |
| `STRPTIME` | DuckDB-only | Transpiles to `STR_TO_DATE` (MySQL) or `TO_TIMESTAMP` (Snowflake); check output |
| `EPOCH` / `EPOCH_MS` | DuckDB-only | No universal equivalent; use `EXTRACT(EPOCH FROM ...)` for portability |
| `READ_PARQUET` / `delta_scan` | DuckDB table functions | Only valid in `dialect="duckdb"` context; not transpilable |

### Dialect-specific notes

**Snowflake**
- `QUALIFY` clause: supported in SQLGlot → Snowflake transpilation.
- `ILIKE`: transpiles correctly.
- `DATE_TRUNC` with week: Snowflake uses `WEEK` not `ISODOW` — verify output.

**BigQuery**
- Backtick identifiers: SQLGlot handles quoting.
- `ARRAY` constructors: generally OK but verify complex nested cases.

**Spark / Databricks**
- Both `dialect="spark"` and `dialect="databricks"` map to SQLGlot's `databricks` target.
- `PIVOT` syntax: not supported in transpilation.

**PostgreSQL**
- `DATE_TRUNC`: maps correctly.
- `ILIKE`: PostgreSQL-native, transpiles correctly.

## Reporting a transpilation failure

If a component produces incorrect SQL for your dialect, open an issue with:
1. The dialect you're targeting
2. The error or unexpected output
3. A minimal reproducer

All transpilation failures raise `DialectTranspilationError` with the original DuckDB SQL attached.
