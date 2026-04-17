"""Input resolution: converts any supported input type to a pandas DataFrame.

Dispatch order:
  1. pd.DataFrame           → passthrough
  2. str matching SQL regex → execute via DuckDB
  3. str / pathlib.Path     → read as Delta table via DuckDB Delta extension
  4. pyspark.sql.DataFrame  → .toPandas() + UserWarning (no hard pyspark import)
  5. anything else          → TypeError
"""

from __future__ import annotations

import pathlib
import re
import warnings

import pandas as pd

from dataengkit._exceptions import DeltaReadError

# Matches SQL query strings (case-insensitive). File paths never start with SELECT/WITH.
_SQL_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def resolve_input(x: object) -> pd.DataFrame:
    """Resolve any supported input to a pandas DataFrame.

    Args:
        x: A pandas DataFrame, a Delta table path (str or Path), a SQL string,
           or a PySpark DataFrame.

    Returns:
        pandas DataFrame.

    Raises:
        TypeError: Input type is not supported.
        DeltaReadError: Delta path is invalid or cannot be read.
        ImportError: PySpark type detected but pyspark is not installed.
    """
    if isinstance(x, pd.DataFrame):
        return x

    if isinstance(x, (str, pathlib.Path)):
        s = str(x)
        if _SQL_RE.match(s):
            return _run_sql(s)
        return _read_delta(s)

    # PySpark detection without a hard import at module level.
    # See: pyspark_no_hard_import learning (confidence 9/10).
    if type(x).__module__.startswith("pyspark"):
        return _convert_pyspark(x)

    raise TypeError(
        f"Expected pd.DataFrame, str, or pathlib.Path. Got {type(x).__name__}.\n"
        "For Delta tables pass the path as a string. "
        "For PySpark DataFrames run: pip install data-kit[spark]"
    )


def _run_sql(query: str) -> pd.DataFrame:
    import duckdb

    try:
        return duckdb.query(query).df()
    except Exception as exc:
        from dataengkit._exceptions import SQLRenderError

        raise SQLRenderError(sql=query, cause=exc) from exc


def _read_delta(path: str) -> pd.DataFrame:
    import duckdb

    try:
        conn = duckdb.connect()
        conn.execute("LOAD delta")
        return conn.execute(f"SELECT * FROM delta_scan('{path}')").df()
    except Exception as exc:
        raise DeltaReadError(path=path, cause=exc) from exc


def _convert_pyspark(df: object) -> pd.DataFrame:
    try:
        import pyspark  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        raise ImportError(
            "PySpark input detected but pyspark is not installed. "
            "Run: pip install data-kit[spark]"
        )

    warnings.warn(
        "Converting PySpark DataFrame to pandas. "
        "For DataFrames >1M rows, consider saving to a Delta table first "
        "and using the path-based input.",
        UserWarning,
        stacklevel=3,
    )
    result: pd.DataFrame = df.toPandas()  # type: ignore[attr-defined]
    return result
