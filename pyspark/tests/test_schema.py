import json

from pyspark.sql import functions as F

from di_features.schema import SIGNAL


def test_parse_valid_signal(spark):
    payload = {
        "event_id": "e1",
        "tenant_id": "t1",
        "session_id": "s1",
        "ts": "2026-05-01T12:00:00Z",
        "device": {"os": "iOS", "ip": "1.2.3.4", "fingerprint": "fp"},
        "behavioral": [{"kind": "click", "dwell_ms": 100}],
    }
    df = spark.createDataFrame([(json.dumps(payload),)], ["data"])
    parsed = df.select(F.from_json("data", SIGNAL).alias("p")).select("p.*")
    row = parsed.collect()[0]
    assert row.event_id == "e1"
    assert row.tenant_id == "t1"
    assert row.behavioral[0].kind == "click"


def test_malformed_signal_yields_null(spark):
    df = spark.createDataFrame([("not-json",)], ["data"])
    parsed = df.select(F.from_json("data", SIGNAL).alias("p"))
    assert parsed.collect()[0].p is None
