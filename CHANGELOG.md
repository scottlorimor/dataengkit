# Changelog

All notable changes to data-kit are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: semver `0.MINOR.PATCH`. Pre-v1.0: minor API breaks allowed with entry here.

---

## [Unreleased]

### Added
- Initial package skeleton
- `_base.py` — `Renderable` ABC with `.to_df()`, `.to_sql()`, `.example()`
- `_dialect.py` — SQLGlot dialect transpilation (duckdb passthrough + 5 supported dialects)
- `_input.py` — `resolve_input()` dispatch: pandas / Delta path / SQL string / PySpark
- `_exceptions.py` — `ValidationError`, `SQLRenderError`, `DialectTranspilationError`, `DeltaReadError`, `DeltaWriteError`, `DeltaConcurrentWriteError`, `SCDGrainError`
- `datakit.analytics` sub-package skeleton
- `datakit.engineering` sub-package skeleton

---

## [0.1.0] — TBD

First PyPI publish. Architecture and internal layer finalized.
