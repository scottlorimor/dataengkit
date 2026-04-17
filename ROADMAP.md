# Roadmap

## v0.1 — Foundation (in progress)

Package skeleton, shared internal layer, PyPI publish.

- [x] `_base.py` — Renderable ABC
- [x] `_dialect.py` — multi-dialect SQL via SQLGlot
- [x] `_input.py` — pandas / Delta / PySpark input dispatch
- [x] `_exceptions.py` — typed exceptions
- [ ] `dataengkit.analytics.CohortMatrix`
- [ ] `dataengkit.analytics.RetentionCurve`
- [ ] PyPI publish under `dataengkit`

## v0.2 — Retention modeling end-to-end

Prove the two-sub-package thesis with a complete story: validate, deduplicate, retain.

```python
from dataengkit.modeling import EventSchema, Dedup
from dataengkit.analytics import CohortMatrix, RetentionCurve

schema = EventSchema(required_columns=["user_id", "event_name", "timestamp"])
schema.validate(raw_events_df)
clean = Dedup(raw_events_df, id_cols=["event_id"]).to_df()
curve = RetentionCurve(clean, cohort_by="signup_date")
curve.to_sql(dialect="snowflake")
```

- [ ] `dataengkit.modeling.EventSchema`
- [ ] `dataengkit.modeling.Dedup`
- [ ] Delta table read/write
- [ ] `datakit.catalog()` — discover all components
- [ ] `.example()` on every component
- [ ] Community infrastructure

Gate: end-to-end retention story works.

## v0.3 — Business analytics layer

- [ ] `dataengkit.analytics.MRRMovement`
- [ ] `dataengkit.analytics.InvestorMetrics` (NDR, GRR, LTV, CAC payback, Magic Number)
- [ ] `dataengkit.modeling.SCDType2`
- [ ] `dataengkit.analytics.ActivationFunnel`

Gate: community validates the retention pattern.

## v1.0

- [ ] `dataengkit.analytics.ABTest`
- [ ] `.to_dbt()` — generate dbt model YAML + ref()
- [ ] Ibis backend (evaluate demand first)

Gate: 500+ stars, 1+ company using in production.

---

## Component Requests

Open an issue using the Component Request template. The maintainer decides what ships next based on domain judgment and real usage signal.
