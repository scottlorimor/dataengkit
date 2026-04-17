"""data-kit — the scikit-learn of data engineering and analytics.

Two sub-packages:
  dataengkit.analytics    — retention, cohort, MRR, investor metrics
  dataengkit.modeling  — event schema, dedup, SCD2, sessionize

Quick start:
    import dataengkit as dk

    # Analytics (v0.1):
    from dataengkit.analytics import CohortMatrix, RetentionCurve

    # Modeling (v0.2):
    from dataengkit.modeling import EventSchema, Dedup

Flat imports also work once components are implemented:
    from dataengkit import RetentionCurve, EventSchema
"""

__version__ = "0.1.0"

# Re-export all public components here as sub-packages are populated.
# Example:
#   from dataengkit.analytics import CohortMatrix, RetentionCurve
#   from dataengkit.modeling import EventSchema, Dedup
#
#   __all__ = ["CohortMatrix", "RetentionCurve", "EventSchema", "Dedup"]

__all__ = ["__version__"]
