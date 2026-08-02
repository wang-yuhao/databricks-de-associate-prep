"""Structured Streaming + Delta demo -- direct practice for Section 1's streaming
objectives (streaming tables vs materialized views, CDC merge patterns) without
needing Kafka or any external streaming source.

Delta tables support `spark.readStream.format("delta")` natively: once bronze has
data in it, you can treat it as an unbounded stream and process it incrementally,
which is exactly what a Lakeflow Declarative Pipelines streaming table does under
the hood. This script:
  1. Reads bronze bars as a stream (Trigger.AvailableNow -- process what's currently
     there and stop, the batch-friendly equivalent of a scheduled streaming job)
  2. Applies the same quality flags as the batch path
  3. Uses foreachBatch + MERGE INTO to upsert into silver -- this IS the open-source
     equivalent of `APPLY CHANGES INTO`: idempotent, safe to re-run, dedupes on key.

Run this INSTEAD OF (not in addition to) src/transform/bronze_to_silver.py --table
bars if you want to practice the streaming code path specifically. Both land in the
same silver table.
"""
import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from src.transform.quality_checks import add_quality_flags, deduplicate, split_good_bad
from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark


def upsert_batch_to_silver(batch_df: DataFrame, batch_id: int, spark, silver_path: str, quarantine_path: str, max_abs_return: float):
    print(f"[stream] processing micro-batch {batch_id}, {batch_df.count()} rows")

    deduped = deduplicate(batch_df)
    flagged = add_quality_flags(deduped, max_abs_return=max_abs_return)
    good, bad = split_good_bad(flagged)

    if bad.count() > 0:
        bad.write.format("delta").mode("append").option("mergeSchema", "true").save(quarantine_path)

    if not DeltaTable.isDeltaTable(spark, silver_path):
        good.write.format("delta").mode("overwrite").save(silver_path)
    else:
        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("t")
            .merge(good.alias("s"), "t.symbol = s.symbol AND t.timestamp = s.timestamp")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def run_streaming_silver(spark, cfg, table_key="bars"):
    bronze_root = resolve_path(cfg, "delta.bronze_path")
    silver_root = resolve_path(cfg, "delta.silver_path")
    quarantine_root = resolve_path(cfg, "delta.quarantine_path")
    checkpoint_root = resolve_path(cfg, "delta.checkpoint_path")

    bronze_path = f"{bronze_root}/{table_key}"
    silver_path = f"{silver_root}/{table_key}"
    quarantine_path = f"{quarantine_root}/{table_key}"
    checkpoint_path = f"{checkpoint_root}/{table_key}_stream"

    stream = spark.readStream.format("delta").load(bronze_path)

    query = (
        stream.writeStream.foreachBatch(
            lambda batch_df, batch_id: upsert_batch_to_silver(
                batch_df, batch_id, spark, silver_path, quarantine_path,
                cfg["quality"]["max_abs_return"],
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)  # process what's there now, then stop -- no infinite loop
        .start()
    )
    query.awaitTermination()
    print(f"[stream] finished processing available bronze data for {table_key}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="bars", choices=["bars"])
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)
    run_streaming_silver(spark, cfg, table_key=args.table)
    spark.stop()


if __name__ == "__main__":
    main()
