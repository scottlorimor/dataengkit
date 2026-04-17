"""datakit.engineering — composable data modeling and transformation components.

v0.2 components (planned):
  EventSchema   — validates event DataFrames against a taxonomy schema
  Dedup         — deduplicates rows by id columns

v0.3 (planned):
  SCDType2      — Slowly Changing Dimension Type 2
  Sessionize    — session attribution from event streams
  DateSpine     — generate a complete date spine for join operations
"""

# Components are imported here as they are implemented.
# from datakit.engineering.event_schema import EventSchema
# from datakit.engineering.dedup import Dedup

__all__: list[str] = []
