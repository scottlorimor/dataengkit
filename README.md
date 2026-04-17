# data-kit

The scikit-learn of data engineering and analytics.

One library. Two sub-packages. Composable, opinionated Python objects that encode the judgment calls data engineers rebuild at every company.

```python
pip install data-kit
```

```python
from datakit.analytics import RetentionCurve
from datakit.engineering import EventSchema, Dedup

# Validate your events data
schema = EventSchema(required_columns=["user_id", "event_name", "timestamp"])
schema.validate(raw_events_df)

# Deduplicate
clean = Dedup(raw_events_df, id_cols=["event_id"]).to_df()

# Retention curve — D0 handling, cohort grain, denominator logic all encoded
curve = RetentionCurve(clean, cohort_by="signup_date")
curve.to_df()
curve.to_sql(dialect="snowflake")  # ready for your dbt model
```

## Why

Every B2B SaaS data team rebuilds the same patterns — retention curves, MRR tables, SCD2 dimensions, event funnels — from scratch at every company. `data-kit` encodes those judgment calls once, correctly, and makes them available to anyone with `pip install`.

Like scikit-learn for ML, `data-kit` gives you the algorithm with sane defaults. You describe your data. The library handles the rest.

## Sub-packages

| Sub-package | Components |
|-------------|-----------|
| `datakit.analytics` | `RetentionCurve`, `CohortMatrix` (v0.1) — `InvestorMetrics`, `MRRMovement`, `ActivationFunnel` (planned) |
| `datakit.engineering` | `EventSchema`, `Dedup` (v0.2) — `SCDType2`, `Sessionize`, `DateSpine` (planned) |

## Every component exports

```python
component.to_df()                    # pandas DataFrame
component.to_sql(dialect="snowflake") # SQL string for any warehouse
Component.example()                  # sample input DataFrame for tutorials
```

Supported dialects: `duckdb` (default), `snowflake`, `bigquery`, `spark`, `databricks`, `postgres`.

## Install

```bash
pip install data-kit                 # core
pip install data-kit[spark]          # + PySpark DataFrame input
pip install data-kit[delta]          # + Delta table write support
```

Delta table **reading** works with the base install (DuckDB bundles the Delta extension).

## Status

**v0.1 — alpha.** Components are being built. The architecture and API contract are locked.

See [ROADMAP.md](ROADMAP.md) for what ships next.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to propose and implement a new component.
