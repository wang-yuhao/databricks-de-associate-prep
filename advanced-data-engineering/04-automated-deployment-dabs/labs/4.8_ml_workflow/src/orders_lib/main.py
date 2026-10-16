"""Entry points of the wheel (see [project.scripts] in pyproject.toml)."""
import argparse

from pyspark.sql import SparkSession

from orders_lib.transforms import daily_summary as summarize


def daily_summary() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    orders = spark.createDataFrame(
        [("o1", 10.0, "2026-10-01 10:00:00"), ("o2", -1.0, "2026-10-01 11:00:00"),
         ("o3", 25.5, "2026-10-02 09:30:00"), ("o3", 25.5, "2026-10-02 09:30:00")],
        "order_id STRING, amount DOUBLE, order_ts STRING")
    summarize(orders).write.mode("overwrite").saveAsTable(f"{args.catalog}.{args.schema}.daily_summary")
    print(f"wrote {args.catalog}.{args.schema}.daily_summary")
