"""Pure DataFrame-in / DataFrame-out functions: easy to unit test, reused by jobs and pipelines."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_orders(df: DataFrame) -> DataFrame:
    """Drop rows without an id, non-positive amounts, and duplicate order ids."""
    return (df.filter(F.col("order_id").isNotNull() & (F.col("amount") > 0))
              .dropDuplicates(["order_id"]))


def add_order_date(df: DataFrame) -> DataFrame:
    return df.withColumn("order_date", F.to_date("order_ts"))


def daily_summary(df: DataFrame) -> DataFrame:
    return (add_order_date(clean_orders(df))
            .groupBy("order_date")
            .agg(F.count("*").alias("orders"),
                 F.round(F.sum("amount"), 2).alias("revenue"),
                 F.round(F.avg("amount"), 2).alias("avg_order_value")))
