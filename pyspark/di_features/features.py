"""Feature transforms (set v1) for fraud-relevant signals.

Inputs are the flattened landing table — one row per Signal — with the SIGNAL
schema columns (event_id, tenant_id, session_id, ts, device, behavioral, dt).

Outputs are session-window features keyed by (tenant_id, session_id, window_end).
"""

from __future__ import annotations

import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


WINDOWS = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "1h": "1 hour",
}


def explode_behavioral(events: DataFrame) -> DataFrame:
    """One row per behavioral event with the parent tenant/session/ts attached.

    Events with an empty/null `behavioral` array still surface as a single row
    with `b` = null, which keeps velocity counts correct (one signal = one
    interaction even if no behavioral payload).
    """
    return events.select(
        "event_id",
        "tenant_id",
        "session_id",
        "ts",
        "device",
        F.explode_outer("behavioral").alias("b"),
    )


def velocity_counts(events: DataFrame, window: str) -> DataFrame:
    """Events per (tenant, session, time-window). Uses tumbling windows."""
    return (
        events.withWatermark("ts", "10 minutes")
        .groupBy(
            "tenant_id",
            "session_id",
            F.window("ts", window).alias("w"),
        )
        .agg(F.count("*").alias(f"events_{_label(window)}"))
        .select(
            "tenant_id",
            "session_id",
            F.col("w.end").alias("window_end"),
            f"events_{_label(window)}",
        )
    )


def _label(window: str) -> str:
    # "1 minute" -> "1m" / "5 minutes" -> "5m" / "1 hour" -> "1h"
    n, unit = window.split()
    return f"{n}{unit[0]}"


def device_ip_fp_cardinality(events: DataFrame) -> DataFrame:
    """Distinct device fingerprints, IPs, and OS strings observed per session.

    A surge in cardinality within a single session is a strong fraud signal
    (device farms, proxy rotation).
    """
    return (
        events.groupBy("tenant_id", "session_id")
        .agg(
            F.approx_count_distinct("device.fingerprint").alias("distinct_fingerprints"),
            F.approx_count_distinct("device.ip").alias("distinct_ips"),
            F.approx_count_distinct("device.os").alias("distinct_os"),
            F.max("ts").alias("window_end"),
        )
    )


def typing_cadence_variance(events: DataFrame) -> DataFrame:
    """Variance of typing cadence per session. Low variance = bot-like."""
    exploded = explode_behavioral(events).filter(F.col("b.kind") == "type")
    return (
        exploded.groupBy("tenant_id", "session_id")
        .agg(
            F.var_samp("b.typing_cps").alias("typing_cps_variance"),
            F.avg("b.typing_cps").alias("typing_cps_mean"),
            F.max("ts").alias("window_end"),
        )
    )


def mouse_entropy_avg(events: DataFrame) -> DataFrame:
    exploded = explode_behavioral(events)
    return (
        exploded.groupBy("tenant_id", "session_id")
        .agg(
            F.avg("b.mouse_entropy").alias("mouse_entropy_avg"),
            F.max("ts").alias("window_end"),
        )
    )


# --- Geo distance vs. prior session -----------------------------------------

_EARTH_KM = 6371.0


@F.udf(DoubleType())
def haversine_km(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * _EARTH_KM * math.asin(math.sqrt(a))


def geo_distance_vs_prior(events: DataFrame) -> DataFrame:
    """Distance (km) from this session's first geo to the same tenant's prior
    session geo. Implausibly large jumps in short time windows are fraud signals.
    """
    from pyspark.sql.window import Window

    sessions = (
        events.filter(F.col("device.geo_lat").isNotNull() & F.col("device.geo_lon").isNotNull())
        .groupBy("tenant_id", "session_id")
        .agg(
            F.min("ts").alias("session_start"),
            F.first("device.geo_lat", ignorenulls=True).alias("lat"),
            F.first("device.geo_lon", ignorenulls=True).alias("lon"),
        )
    )
    w = Window.partitionBy("tenant_id").orderBy("session_start")
    with_prior = sessions.withColumn("prior_lat", F.lag("lat").over(w)).withColumn(
        "prior_lon", F.lag("lon").over(w)
    )
    return with_prior.select(
        "tenant_id",
        "session_id",
        F.col("session_start").alias("window_end"),
        haversine_km("prior_lat", "prior_lon", "lat", "lon").alias("geo_distance_km_prior"),
    )


# --- Top-level orchestration ------------------------------------------------


def build_feature_set(events: DataFrame) -> DataFrame:
    """Join all feature components into one row per (tenant, session, window_end).

    Uses outer joins so a session missing one component (e.g. no typing events)
    still produces a feature row with NULLs.
    """
    velocity = (
        velocity_counts(events, WINDOWS["1m"])
        .join(velocity_counts(events, WINDOWS["5m"]), ["tenant_id", "session_id", "window_end"], "outer")
        .join(velocity_counts(events, WINDOWS["1h"]), ["tenant_id", "session_id", "window_end"], "outer")
    )

    cardinality = device_ip_fp_cardinality(events)
    typing = typing_cadence_variance(events)
    mouse = mouse_entropy_avg(events)
    geo = geo_distance_vs_prior(events)

    keys = ["tenant_id", "session_id", "window_end"]
    return (
        velocity.join(cardinality, keys, "outer")
        .join(typing, keys, "outer")
        .join(mouse, keys, "outer")
        .join(geo, keys, "outer")
        .withColumn("feature_window", F.lit("session"))
    )
