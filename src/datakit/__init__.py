"""data-kit — the scikit-learn of data engineering and analytics.

Two sub-packages:
  datakit.analytics    — retention, cohort, MRR, investor metrics
  datakit.engineering  — event schema, dedup, SCD2, sessionize

Quick start:
    import datakit as dk

    # Analytics (v0.1):
    from datakit.analytics import CohortMatrix, RetentionCurve

    # Engineering (v0.2):
    from datakit.engineering import EventSchema, Dedup

Flat imports also work once components are implemented:
    from datakit import RetentionCurve, EventSchema
"""

__version__ = "0.1.0"

# Re-export all public components here as sub-packages are populated.
# Example:
#   from datakit.analytics import CohortMatrix, RetentionCurve
#   from datakit.engineering import EventSchema, Dedup
#
#   __all__ = ["CohortMatrix", "RetentionCurve", "EventSchema", "Dedup"]

__all__ = ["__version__"]
