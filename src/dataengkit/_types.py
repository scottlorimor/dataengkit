"""Shared type aliases."""

from __future__ import annotations

import pathlib

import pandas as pd

# Accepted input types for all component constructors.
# Widened in v0.2 to include Delta paths and PySpark DataFrames via resolve_input().
DataInput = pd.DataFrame | str | pathlib.Path
