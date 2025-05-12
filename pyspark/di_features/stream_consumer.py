"""Redis Streams → Parquet landing job.

Reads `di:signals:*` via XREADGROUP (consumer group `di-features`), parses each
entry against the SIGNAL schema, lands raw events to object storage as Parquet
partitioned by `tenant_id` / `dt`, and quarantines malformed rows to
`bad_records/`.

This is implemented as a polling micro-batch driver rather than Structured
Streaming because Redis is not a first-class Spark source. Each tick:
  1. XREADGROUP a batch from each tenant stream (BLOCK, COUNT bounded).
  2. Build a Spark DataFrame from the JSON payloads.
  3. from_json → SIGNAL; bad rows go to bad_records/.
  4. XACK on success.

For high-throughput tenants, scale by running multiple consumers in the same
group — Redis distributes pending entries across them.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable

import redis
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from di_features.schema import SIGNAL

log = logging.getLogger(__name__)

CONSUMER_GROUP = "di-features"


@dataclass(frozen=True)
class ConsumerConfig:
    redis_url: str
    output_path: str  # s3a://bucket/raw or file:///tmp/raw
    bad_records_path: str
    batch_count: int = 5_000
    block_ms: int = 2_000
    consumer_name: str = "consumer-0"
    tenant_glob: str = "di:signals:*"
    poll_idle_sleep_s: float = 0.5

    @classmethod
    def from_env(cls) -> "ConsumerConfig":
        return cls(
            redis_url=os.environ["REDIS_URL"],
            output_path=os.environ["OUTPUT_PATH"],
            bad_records_path=os.environ.get(
                "BAD_RECORDS_PATH", os.path.join(os.environ["OUTPUT_PATH"], "_bad_records")
            ),
            batch_count=int(os.environ.get("BATCH_COUNT", "5000")),
            block_ms=int(os.environ.get("BLOCK_MS", "2000")),
            consumer_name=os.environ.get("HOSTNAME", "consumer-0"),
        )


def discover_streams(rdb: redis.Redis, glob: str) -> list[str]:
    return [k.decode() if isinstance(k, bytes) else k for k in rdb.scan_iter(match=glob)]


def ensure_groups(rdb: redis.Redis, streams: Iterable[str]) -> None:
    for s in streams:
        try:
            rdb.xgroup_create(name=s, groupname=CONSUMER_GROUP, id="$", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise


def read_batch(
    rdb: redis.Redis, streams: list[str], cfg: ConsumerConfig
) -> dict[str, list[tuple[bytes, dict[bytes, bytes]]]]:
    if not streams:
        return {}
    streams_arg = {s: ">" for s in streams}
    resp = rdb.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=cfg.consumer_name,
        streams=streams_arg,
        count=cfg.batch_count,
        block=cfg.block_ms,
    )
    out: dict[str, list[tuple[bytes, dict[bytes, bytes]]]] = {}
    for stream_name, entries in resp or []:
        key = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
        out[key] = entries
    return out


def to_dataframe(spark: SparkSession, batch: dict[str, list]) -> DataFrame | None:
    rows: list[tuple[str, str]] = []
    for stream_name, entries in batch.items():
        tenant = stream_name.rsplit(":", 1)[-1]
        for _entry_id, fields in entries:
            payload = fields.get(b"data") or fields.get("data")
            if payload is None:
                continue
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="replace")
            rows.append((tenant, payload))
    if not rows:
        return None
    raw = spark.createDataFrame(rows, schema="tenant_id_hint string, data string")
    return raw


def parse_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (good, bad). Bad rows preserve the raw JSON for forensics."""
    parsed = df.withColumn("parsed", F.from_json(F.col("data"), SIGNAL))
    good = (
        parsed.filter(F.col("parsed").isNotNull() & F.col("parsed.event_id").isNotNull())
        .select("parsed.*")
        .withColumn("dt", F.to_date("ts"))
    )
    bad = parsed.filter(F.col("parsed").isNull() | F.col("parsed.event_id").isNull()).select(
        F.col("tenant_id_hint"),
        F.col("data").cast(StringType()).alias("raw"),
        F.current_timestamp().alias("quarantined_at"),
    )
    return good, bad


def write_parquet(good: DataFrame, output_path: str) -> None:
    (
        good.write.mode("append")
        .partitionBy("tenant_id", "dt")
        .parquet(output_path)
    )


def write_bad(bad: DataFrame, bad_path: str) -> None:
    if bad.rdd.isEmpty():
        return
    bad.write.mode("append").json(bad_path)


def ack(rdb: redis.Redis, batch: dict[str, list]) -> int:
    n = 0
    for stream_name, entries in batch.items():
        if not entries:
            continue
        ids = [eid for eid, _ in entries]
        n += rdb.xack(stream_name, CONSUMER_GROUP, *ids)
    return n


def run(spark: SparkSession, cfg: ConsumerConfig, max_iterations: int | None = None) -> None:
    rdb = redis.Redis.from_url(cfg.redis_url, decode_responses=False)
    iters = 0
    while True:
        streams = discover_streams(rdb, cfg.tenant_glob)
        ensure_groups(rdb, streams)
        batch = read_batch(rdb, streams, cfg)
        total = sum(len(v) for v in batch.values())
        if total == 0:
            time.sleep(cfg.poll_idle_sleep_s)
        else:
            df = to_dataframe(spark, batch)
            if df is not None:
                good, bad = parse_and_split(df)
                write_parquet(good, cfg.output_path)
                write_bad(bad, cfg.bad_records_path)
            acked = ack(rdb, batch)
            log.info(
                json.dumps(
                    {"event": "batch_committed", "rows": total, "acked": acked, "streams": len(batch)}
                )
            )
        iters += 1
        if max_iterations is not None and iters >= max_iterations:
            return


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    cfg = ConsumerConfig.from_env()
    spark = (
        SparkSession.builder.appName("di-features-stream-consumer")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    run(spark, cfg)


if __name__ == "__main__":
    main()
