"""Feature store writer.

Default format is Delta Lake. Set FEATURE_STORE_FORMAT=iceberg to switch.
The table is keyed by (tenant_id, session_id, feature_window) and partitioned
by (tenant_id, dt) for cheap pruning on per-tenant reads.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def write_features(features: DataFrame, table: str, fmt: str | None = None) -> None:
    fmt = fmt or os.environ.get("FEATURE_STORE_FORMAT", "delta")
    out = features.withColumn("dt", F.to_date("window_end"))

    if fmt == "delta":
        _merge_delta(out, table)
    elif fmt == "iceberg":
        _merge_iceberg(out, table)
    elif fmt == "parquet":
        # Useful for tests / local-only runs without Delta or Iceberg jars.
        out.write.mode("append").partitionBy("tenant_id", "dt").parquet(table)
    else:
        raise ValueError(f"unknown FEATURE_STORE_FORMAT: {fmt}")


def _merge_delta(df: DataFrame, table: str) -> None:
    spark = df.sparkSession
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            tenant_id STRING,
            session_id STRING,
            window_end TIMESTAMP,
            feature_window STRING,
            events_1m LONG,
            events_5m LONG,
            events_1h LONG,
            distinct_fingerprints LONG,
            distinct_ips LONG,
            distinct_os LONG,
            typing_cps_variance DOUBLE,
            typing_cps_mean DOUBLE,
            mouse_entropy_avg DOUBLE,
            geo_distance_km_prior DOUBLE,
            dt DATE
        )
        USING DELTA
        PARTITIONED BY (tenant_id, dt)
        """
    )
    df.createOrReplaceTempView("_incoming_features")
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING _incoming_features s
        ON t.tenant_id = s.tenant_id
           AND t.session_id = s.session_id
           AND t.window_end = s.window_end
           AND t.feature_window = s.feature_window
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _merge_iceberg(df: DataFrame, table: str) -> None:
    spark = df.sparkSession
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            tenant_id STRING,
            session_id STRING,
            window_end TIMESTAMP,
            feature_window STRING,
            events_1m BIGINT,
            events_5m BIGINT,
            events_1h BIGINT,
            distinct_fingerprints BIGINT,
            distinct_ips BIGINT,
            distinct_os BIGINT,
            typing_cps_variance DOUBLE,
            typing_cps_mean DOUBLE,
            mouse_entropy_avg DOUBLE,
            geo_distance_km_prior DOUBLE,
            dt DATE
        )
        USING ICEBERG
        PARTITIONED BY (tenant_id, dt)
        """
    )
    df.createOrReplaceTempView("_incoming_features")
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING _incoming_features s
        ON t.tenant_id = s.tenant_id
           AND t.session_id = s.session_id
           AND t.window_end = s.window_end
           AND t.feature_window = s.feature_window
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
