"""Session-level aggregates with watermarking.

Watermark = 10 minutes. Late events beyond that are dropped from the streaming
view (still preserved in landed Parquet for backfill).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SESSION_WATERMARK = "10 minutes"
SESSION_GAP = "10 minutes"


def session_aggregates(events: DataFrame) -> DataFrame:
    """One row per session with start/end and basic counts.

    Uses session_window so a session naturally closes after 10 minutes of
    inactivity even if the producer never sends an explicit end event.
    """
    return (
        events.withWatermark("ts", SESSION_WATERMARK)
        .groupBy(
            "tenant_id",
            "session_id",
            F.session_window("ts", SESSION_GAP).alias("sw"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.min("ts").alias("session_start"),
            F.max("ts").alias("session_end"),
            F.approx_count_distinct("device.fingerprint").alias("distinct_fingerprints"),
        )
        .select(
            "tenant_id",
            "session_id",
            "session_start",
            "session_end",
            "event_count",
            "distinct_fingerprints",
        )
    )
