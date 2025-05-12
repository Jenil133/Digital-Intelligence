"""PySpark schema mirroring the Go signal contract in di-collector/internal/signal/types.go.

Keep this file in lockstep with the Go struct. Additive changes only at minor
schema versions; breaking changes require a major bump (see docs/schema-evolution.md).
"""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEVICE = StructType(
    [
        StructField("os", StringType(), nullable=True),
        StructField("os_version", StringType(), nullable=True),
        StructField("ip", StringType(), nullable=True),
        StructField("fingerprint", StringType(), nullable=True),
        StructField("battery", DoubleType(), nullable=True),
        StructField("geo_lat", DoubleType(), nullable=True),
        StructField("geo_lon", DoubleType(), nullable=True),
    ]
)

BEHAVIORAL = StructType(
    [
        StructField("kind", StringType(), nullable=False),
        StructField("dwell_ms", LongType(), nullable=True),
        StructField("typing_cps", DoubleType(), nullable=True),
        StructField("mouse_entropy", DoubleType(), nullable=True),
        StructField("target_elem", StringType(), nullable=True),
    ]
)

SIGNAL = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("tenant_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("ts", TimestampType(), nullable=False),
        StructField("device", DEVICE, nullable=True),
        StructField("behavioral", ArrayType(BEHAVIORAL), nullable=True),
    ]
)

# Schema version emitted by the collector in X-DI-Schema-Version.
SCHEMA_VERSION = "1.0.0"
