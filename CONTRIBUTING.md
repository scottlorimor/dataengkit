# Contributing

## Proposing a component

Before writing code, open a **Component Request** issue. Include:
- What the component does (one sentence)
- Input schema (column names + types)
- Output schema (column names + types)
- Use case: where would you use this at your company?

The maintainer reviews requests and decides what ships next. Requests with clear input/output schemas and real use cases move faster.

## Implementing a component

Every component must:

1. Subclass `Renderable` from `datakit._base`
2. Implement `.to_df()`, `.to_sql(dialect)`, and `.example()`
3. Use `resolve_input()` from `datakit._input` for all DataFrame inputs
4. Use `transpile()` from `datakit._dialect` for SQL dialect output
5. Raise typed exceptions from `datakit._exceptions` — no bare `raise ValueError()`
6. Ship with tests covering: happy path, empty input, missing columns, SQL snapshot

## What makes a good component

The library encodes **judgment calls** — decisions that every data engineer debates and rebuilds. A good component:
- Makes a decision (D0 handling, MRR grain, SCD2 effective_from logic) so the user doesn't have to
- Has clear correctness criteria (you can verify the output against a hand-calculated example)
- Works on any warehouse dialect via `.to_sql()`

A bad component is one where the "right" answer depends entirely on the user's business logic. Those belong in the user's codebase, not here.

## Development setup

```bash
git clone https://github.com/theoncegreatchamp/data-kit
cd data-kit
uv sync --extra dev
uv run pytest
```

## Running tests

```bash
uv run pytest                    # all tests
uv run pytest tests/test_dialect.py  # one file
uv run pytest -x                 # stop on first failure
```

## Code style

```bash
uv run ruff check .
uv run mypy src/
```

Both must pass before opening a PR.
