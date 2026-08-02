"""Silver -> Gold: builds the feature table used for ML training.

Combines:
  - Technical indicators computed from silver `bars` (src/ml/feature_engineering.py)
  - Daily bid/ask spread + trade-volume aggregates computed from silver `quotes` and
    `trades`, joined on (symbol, date) -- this is deliberately a plain daily
    aggregation-then-join rather than a true as-of join, since Spark's DataFrame API
    doesn't have a native as-of join and implementing one from scratch is out of
    scope here; daily-granularity microstructure features are still genuinely useful
    signal (average spread, trade intensity) without that complexity.
"""
import argparse

from pyspark.sql import functions as F
from pathlib import Path

from src.ml.feature_engineering import add_technical_indicators
from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark


def _quote_daily_aggregates(quotes_df):
    return (
        quotes_df.withColumn("date", F.to_date("timestamp"))
        .withColumn("spread", F.col("ask_price") - F.col("bid_price"))
        .withColumn("mid", (F.col("ask_price") + F.col("bid_price")) / 2)
        .groupBy("symbol", "date")
        .agg(
            F.avg("spread").alias("avg_spread"),
            (F.avg("spread") / F.avg("mid")).alias("avg_spread_pct"),
            F.avg("bid_size").alias("avg_bid_size"),
            F.avg("ask_size").alias("avg_ask_size"),
        )
    )


def _trade_daily_aggregates(trades_df):
    return (
        trades_df.withColumn("date", F.to_date("timestamp"))
        .groupBy("symbol", "date")
        .agg(
            F.count("*").alias("trade_tick_count"),
            F.sum("size").alias("trade_tick_volume"),
            F.avg("price").alias("trade_avg_price"),
            F.stddev("price").alias("trade_price_stddev"),
        )
    )


def run(spark, cfg):
    silver_root = resolve_path(cfg, "delta.silver_path")
    gold_root = resolve_path(cfg, "delta.gold_path")

    bars = spark.read.format("delta").load(f"{silver_root}/bars")
    features = add_technical_indicators(spark, bars)
    features = features.withColumn("date", F.to_date("timestamp"))

    gold = features
    try:
        quotes = spark.read.format("delta").load(f"{silver_root}/quotes")
        gold = gold.join(_quote_daily_aggregates(quotes), on=["symbol", "date"], how="left")
    except Exception as e:
        print(f"[gold] no silver quotes table found, skipping quote features ({e})")

    try:
        trades = spark.read.format("delta").load(f"{silver_root}/trades")
        gold = gold.join(_trade_daily_aggregates(trades), on=["symbol", "date"], how="left")
    except Exception as e:
        print(f"[gold] no silver trades table found, skipping trade features ({e})")

    gold = gold.drop("date")

    gold_path = f"{gold_root}/features"
    (
        gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("symbol")
        .save(gold_path)
    )
    gold_uri = Path(gold_path).resolve().as_uri()
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.features USING DELTA LOCATION '{gold_uri}'")

    n = gold.count()
    print(f"[gold:features] wrote {n} rows, {len(gold.columns)} columns -> {gold_path}")
    return n


def main():
    cfg = load_config()
    spark = get_spark(cfg)
    run(spark, cfg)
    spark.stop()


if __name__ == "__main__":
    main()
