"""Renderable ABC — the contract every data-kit component implements."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Renderable(ABC):
    """Base class for all dataengkit components.

    Every component exposes:
      .to_df()                        — compute and return a pandas DataFrame
      .to_sql(source_table, dialect)  — return SQL string in the target dialect
      .example()                      — return sample input DataFrame for tutorials

    to_sql() design:
      If input was a DataFrame, you must supply source_table so the SQL has a
      table reference. If input was a SQL string or Delta path, source_table is
      used as an alias in the generated SQL.
    """

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Compute the component and return a pandas DataFrame."""
        ...

    @abstractmethod
    def to_sql(self, source_table: str, dialect: str = "duckdb") -> str:
        """Return a SQL string that computes this component.

        Args:
            source_table: The table or view name to reference in the SQL.
                          Required — the SQL must have a concrete table reference.
            dialect: Target SQL dialect. Default is "duckdb" (passthrough).
                     Supported: duckdb, snowflake, bigquery, spark, databricks, postgres.
        """
        ...

    @classmethod
    @abstractmethod
    def example(cls) -> pd.DataFrame:
        """Return a sample input DataFrame with the correct schema.

        Returns 5-10 representative rows. Use this for tutorials and local
        testing without production data.
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
