from datetime import datetime, timezone

from chispa import assert_df_equality
from pyspark.sql import Row

from di_features.features import (
    device_ip_fp_cardinality,
    geo_distance_vs_prior,
    typing_cadence_variance,
)


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def _row(event_id, tenant, session, ts, device=None, behavioral=None):
    return Row(
        event_id=event_id,
        tenant_id=tenant,
        session_id=session,
        ts=_ts(ts),
        device=device,
        behavioral=behavioral or [],
    )


def test_cardinality_counts_distinct_fingerprints(spark):
    events = spark.createDataFrame(
        [
            _row("e1", "t1", "s1", "2026-05-01T12:00:00", Row(os="iOS", os_version=None, ip="1.1.1.1", fingerprint="fp_a", battery=None, geo_lat=None, geo_lon=None)),
            _row("e2", "t1", "s1", "2026-05-01T12:00:01", Row(os="iOS", os_version=None, ip="1.1.1.2", fingerprint="fp_b", battery=None, geo_lat=None, geo_lon=None)),
            _row("e3", "t1", "s1", "2026-05-01T12:00:02", Row(os="iOS", os_version=None, ip="1.1.1.2", fingerprint="fp_b", battery=None, geo_lat=None, geo_lon=None)),
        ]
    )
    out = device_ip_fp_cardinality(events).collect()
    assert len(out) == 1
    assert out[0].distinct_fingerprints == 2
    assert out[0].distinct_ips == 2


def test_typing_variance_excludes_non_type_events(spark):
    behavioral_type = [Row(kind="type", dwell_ms=None, typing_cps=4.0, mouse_entropy=None, target_elem=None)]
    behavioral_other = [Row(kind="click", dwell_ms=100, typing_cps=None, mouse_entropy=None, target_elem="btn")]
    events = spark.createDataFrame(
        [
            _row("e1", "t1", "s1", "2026-05-01T12:00:00", behavioral=behavioral_type),
            _row("e2", "t1", "s1", "2026-05-01T12:00:01", behavioral=behavioral_other),
            _row("e3", "t1", "s1", "2026-05-01T12:00:02", behavioral=[Row(kind="type", dwell_ms=None, typing_cps=6.0, mouse_entropy=None, target_elem=None)]),
        ]
    )
    out = typing_cadence_variance(events).collect()
    assert len(out) == 1
    # Variance of [4.0, 6.0] = 2.0
    assert abs(out[0].typing_cps_variance - 2.0) < 1e-9
    assert abs(out[0].typing_cps_mean - 5.0) < 1e-9


def test_geo_distance_vs_prior(spark):
    # Two sessions for the same tenant: SF then NYC. Expected ~4140 km.
    sf = Row(os=None, os_version=None, ip=None, fingerprint=None, battery=None, geo_lat=37.7749, geo_lon=-122.4194)
    nyc = Row(os=None, os_version=None, ip=None, fingerprint=None, battery=None, geo_lat=40.7128, geo_lon=-74.0060)
    events = spark.createDataFrame(
        [
            _row("e1", "t1", "s1", "2026-05-01T10:00:00", device=sf),
            _row("e2", "t1", "s2", "2026-05-01T11:00:00", device=nyc),
        ]
    )
    rows = {r.session_id: r for r in geo_distance_vs_prior(events).collect()}
    assert rows["s1"].geo_distance_km_prior is None
    assert 4000 < rows["s2"].geo_distance_km_prior < 4200
