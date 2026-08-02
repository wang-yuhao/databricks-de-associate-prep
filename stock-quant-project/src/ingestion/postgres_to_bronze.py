"""Ingestion: Postgres (or synthetic data) -> Delta Lake bronze tables.

This is an APPEND-ONLY landing zone -- raw data goes in exactly as received, with
just an ingestion timestamp added. No cleansing happens here on purpose: bronze
should always be re-derivable from source, so silver/gold can be rebuilt from
scratch if transform logic changes. This mirrors the medallion architecture
pattern tested throughout Section 1 and Section 2 of the exam.

Modes:
  synthetic  -- generate fake data locally, no DB needed. Use this first.
  postgres   -- pull real data from your configured Postgres database.
  incremental-- like postgres, but only pulls rows newer than what's already in
                bronze (watermark = max(timestamp) already ingested). This is the
                pattern you'd wire into a scheduled job to keep bronze current, and
                is the batch-side equivalent of what a streaming source does.

Usage:
  python -m src.ingestion.postgres_to_bronze --mode synthetic
  python -m src.ingestion.postgres_to_bronze --mode postgres --table bars
  python -m src.ingestion.postgres_to_bronze --mode incremental --table bars
"""
import argparse
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.ingestion.synthetic_data import generate_all
from src.utils import db
from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark

TABLE_KEYS = ["bars", "quotes", "trades"]


def _write_bronze(spark, pdf, table_key, cfg, mode="append"):
    if pdf is None or len(pdf) == 0:
        print(f"[bronze:{table_key}] nothing to write")
        return 0

    sdf = spark.createDataFrame(pdf)
    sdf = sdf.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source", F.lit("synthetic" if cfg.get("_synthetic") else "postgres")
    )

    bronze_root = resolve_path(cfg, "delta.bronze_path")
    table_path = f"{bronze_root}/{table_key}"

    (
        sdf.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(table_path)
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{table_key} USING DELTA LOCATION '{table_path}'")
    n = sdf.count()
    print(f"[bronze:{table_key}] wrote {n} rows -> {table_path}")
    return n


def ingest_synthetic(spark, cfg, years=3):
    symbols = cfg["symbols"]
    bars, quotes, trades = generate_all(symbols, years=years)
    cfg["_synthetic"] = True
    for table_key, pdf in zip(TABLE_KEYS, [bars, quotes, trades]):
        _write_bronze(spark, pdf, table_key, cfg, mode="overwrite")


def ingest_postgres(spark, cfg, table_key, incremental=False):
    symbols = cfg["symbols"]
    since_ts = None

    if incremental:
        bronze_root = resolve_path(cfg, "delta.bronze_path")
        table_path = f"{bronze_root}/{table_key}"
        try:
            existing = spark.read.format("delta").load(table_path)
            ts_col = cfg["postgres"]["tables"][table_key]["timestamp_col"]
            since_ts = existing.agg(F.max(ts_col)).collect()[0][0]
            print(f"[bronze:{table_key}] incremental watermark = {since_ts}")
        except Exception:
            print(f"[bronze:{table_key}] no existing bronze table found, doing a full load")

    total = 0
    for chunk in db.fetch_table(cfg, table_key, symbols=symbols, since_ts=since_ts):
        total += _write_bronze(spark, chunk, table_key, cfg, mode="append")
    print(f"[bronze:{table_key}] total rows ingested this run: {total}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["synthetic", "postgres", "incremental"], required=True)
    parser.add_argument("--table", choices=TABLE_KEYS, default=None, help="required for postgres/incremental")
    parser.add_argument("--years", type=int, default=3, help="years of synthetic history to generate")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)

    if args.mode == "synthetic":
        ingest_synthetic(spark, cfg, years=args.years)
    else:
        tables = [args.table] if args.table else TABLE_KEYS
        for t in tables:
            ingest_postgres(spark, cfg, t, incremental=(args.mode == "incremental"))

    spark.stop()


if __name__ == "__main__":
    main()
