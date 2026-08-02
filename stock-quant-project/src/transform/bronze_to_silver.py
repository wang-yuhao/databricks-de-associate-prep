"""Bronze -> Silver for the `bars` table.

Reads raw bronze bars, runs the quality checks from quality_checks.py, sends bad rows
to a quarantine Delta table (with the reason attached, so you can go audit *why*
something was rejected), and MERGEs good rows into silver on (symbol, timestamp).

Using MERGE INTO here rather than a plain overwrite is the open-source equivalent of
what `APPLY CHANGES INTO` gives you in Lakeflow Declarative Pipelines: idempotent
upserts, safe to re-run, and the pattern the exam's CDC objective is testing.
"""
import argparse
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.monitoring.pipeline_monitor import log_run, maybe_alert
from src.transform.quality_checks import (
    add_quality_flags,
    add_quote_quality_flags,
    add_trade_quality_flags,
    deduplicate,
    quarantine_rate_pct,
    split_good_bad,
)
from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark

FLAG_FUNCTIONS = {
    "bars": lambda df, cfg: add_quality_flags(df, max_abs_return=cfg["quality"]["max_abs_return"]),
    "quotes": lambda df, cfg: add_quote_quality_flags(df),
    "trades": lambda df, cfg: add_trade_quality_flags(df),
}

MERGE_KEYS = {
    "bars": ("symbol", "timestamp"),
    "quotes": ("symbol", "timestamp", "bid_price", "ask_price"),
    "trades": ("symbol", "timestamp", "price", "size"),
}


def run(spark, cfg, table_key="bars"):
    bronze_root = resolve_path(cfg, "delta.bronze_path")
    silver_root = resolve_path(cfg, "delta.silver_path")
    quarantine_root = resolve_path(cfg, "delta.quarantine_path")

    bronze_path = f"{bronze_root}/{table_key}"
    silver_path = f"{silver_root}/{table_key}"
    quarantine_path = f"{quarantine_root}/{table_key}"

    raw = spark.read.format("delta").load(bronze_path)
    keys = MERGE_KEYS[table_key]
    deduped = deduplicate(raw, keys=keys)
    flagged = FLAG_FUNCTIONS[table_key](deduped, cfg)
    good, bad = split_good_bad(flagged)

    rate = quarantine_rate_pct(flagged)
    n_good, n_bad = good.count(), bad.count()
    print(f"[silver:{table_key}] good={n_good} bad={n_bad} quarantine_rate={rate:.2f}%")

    # --- quarantine (append-only audit trail of rejected rows) ---
    if n_bad > 0:
        bad.write.format("delta").mode("append").option("mergeSchema", "true").save(quarantine_path)
        quarantine_uri = Path(quarantine_path).resolve().as_uri()
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS bronze.{table_key}_quarantine USING DELTA LOCATION '{quarantine_uri}'"
        )

    # --- upsert good rows into silver ---
    if not DeltaTable.isDeltaTable(spark, silver_path):
        good.write.format("delta").mode("overwrite").save(silver_path)
    else:
        target = DeltaTable.forPath(spark, silver_path)
        merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (
            target.alias("t")
            .merge(good.alias("s"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        silver_uri = Path(silver_path).resolve().as_uri()
        spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
        spark.sql(f"CREATE TABLE IF NOT EXISTS silver.{table_key} USING DELTA LOCATION '{silver_uri}'")

    log_run(spark, cfg, stage=f"silver:{table_key}", rows_in=raw.count(), rows_out=n_good, rows_quarantined=n_bad)
    maybe_alert(cfg, stage=f"silver:{table_key}", quarantine_rate=rate)

    return n_good, n_bad


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="bars", choices=["bars", "quotes", "trades"])
    parser.add_argument("--all", action="store_true", help="run bars, quotes, and trades in sequence")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)
    tables = ["bars", "quotes", "trades"] if args.all else [args.table]
    for t in tables:
        run(spark, cfg, table_key=t)
    spark.stop()


if __name__ == "__main__":
    main()
