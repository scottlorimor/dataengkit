"""All public exception classes for data-kit."""

from __future__ import annotations


class DataKitError(Exception):
    """Base class for all data-kit errors."""


class ValidationError(DataKitError):
    """Raised when input data fails schema or grain validation.

    Always lists all violations at once — not fail-fast.
    """

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        super().__init__("\n".join(f"  - {v}" for v in violations))


class SQLRenderError(DataKitError):
    """Raised when DuckDB fails to execute the generated SQL."""

    def __init__(self, sql: str, cause: BaseException) -> None:
        self.sql = sql
        self.cause = cause
        super().__init__(f"SQL execution failed: {cause}\n\nSQL:\n{sql}")


class DialectTranspilationError(DataKitError):
    """Raised when SQLGlot cannot transpile DuckDB SQL to the target dialect."""

    def __init__(self, dialect: str, original_sql: str, cause: BaseException | None) -> None:
        self.dialect = dialect
        self.original_sql = original_sql
        self.cause = cause
        msg = f"Cannot transpile to dialect '{dialect}'"
        if cause:
            msg += f": {cause}"
        msg += f"\n\nOriginal SQL:\n{original_sql}"
        msg += "\n\nHint: see DIALECT_COMPAT.md for known unsupported functions."
        super().__init__(msg)


class DeltaReadError(DataKitError):
    """Raised when a Delta table cannot be read from the given path."""

    def __init__(self, path: str, cause: BaseException) -> None:
        self.path = path
        self.cause = cause
        super().__init__(
            f"Cannot read Delta table at '{path}': {cause}\n"
            "Hint: for cloud paths (s3://, gs://, abfss://) ensure environment credentials are set."
        )


class DeltaWriteError(DataKitError):
    """Raised when writing to a Delta table fails (non-concurrency)."""

    def __init__(self, path: str, mode: str, cause: BaseException) -> None:
        self.path = path
        self.mode = mode
        self.cause = cause
        super().__init__(f"Delta write failed at '{path}' (mode={mode}): {cause}")


class DeltaConcurrentWriteError(DataKitError):
    """Raised when a concurrent writer conflicts with an in-progress Delta write."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(
            f"Concurrent write conflict at '{path}'. "
            "Retry or ensure only one writer at a time."
        )


class SCDGrainError(DataKitError):
    """Raised when SCDType2 source has duplicate (natural_key, effective_from) pairs."""

    def __init__(self, natural_key: str, effective_from: object) -> None:
        super().__init__(
            f"Duplicate (natural_key={natural_key!r}, effective_from={effective_from!r}) "
            "detected. Each natural key must have unique effective_from values."
        )
