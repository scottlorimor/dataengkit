"""Renderable ABC — the contract every data-kit component implements."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Renderable(ABC):
    """Base class for all data-kit components.

    Every component exposes:
      .to_df()               — compute and return a pandas DataFrame
      .to_sql(dialect)       — return SQL string in the target dialect
      .example()             — return sample input DataFrame for tutorials
    """

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """Compute the component and return a pandas DataFrame."""
        ...

    @abstractmethod
    def to_sql(self, dialect: str = "duckdb") -> str:
        """Return a SQL string that computes this component.

        Args:
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
