"""Nightly batch / replay job.

Reads landed Parquet from `RAW_PATH`, optionally bounded by a date range, runs
the same feature transforms as the streaming path, and merges results into the
feature store.

Usage:
    python -m di_features.backfill \
        --raw s3a://bucket/raw \
        --table di_features.session_features \
        --start 2026-04-01 --end 2026-04-30
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from di_features.feature_store import write_features
from di_features.features import build_feature_set

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", required=True, help="raw landing path (s3a://… or file://…)")
    p.add_argument("--table", required=True, help="feature store table identifier")
    p.add_argument("--start", help="inclusive start date YYYY-MM-DD")
    p.add_argument("--end", help="inclusive end date YYYY-MM-DD")
    p.add_argument("--tenant", help="restrict to a single tenant_id")
    p.add_argument("--format", default=None, help="delta | iceberg | parquet")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_args()
    spark = (
        SparkSession.builder.appName("di-features-backfill")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    df = spark.read.parquet(args.raw)
    if args.start:
        df = df.filter(F.col("dt") >= F.to_date(F.lit(args.start)))
    if args.end:
        df = df.filter(F.col("dt") <= F.to_date(F.lit(args.end)))
    if args.tenant:
        df = df.filter(F.col("tenant_id") == args.tenant)

    features = build_feature_set(df)
    write_features(features, args.table, fmt=args.format)
    log.info("backfill complete: table=%s rows=%d", args.table, features.count())


if __name__ == "__main__":
    main()
