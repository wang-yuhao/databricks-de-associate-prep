"""Pure data-quality functions for bars data: no I/O, just DataFrame in -> DataFrame out.
Kept separate from the pipeline glue code (bronze_to_silver.py) so they're trivial to
unit test with plain PySpark DataFrames -- this mirrors the intent of Lakeflow
Declarative Pipelines' `EXPECT ... ON VIOLATION` expectations, just implemented by hand
since we're running open-source Spark instead of the Databricks runtime.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_quote_quality_flags(quotes: DataFrame) -> DataFrame:
    checks = (
        F.when(F.col("symbol").isNull() | F.col("timestamp").isNull(), "missing_key")
        .when(F.col("bid_price") <= 0, "non_positive_bid")
        .when(F.col("ask_price") <= 0, "non_positive_ask")
        .when(F.col("ask_price") < F.col("bid_price"), "crossed_quote")
        .when((F.col("bid_size") < 0) | (F.col("ask_size") < 0), "negative_size")
        .otherwise(F.lit(None).cast("string"))
    )
    return quotes.withColumn("_quality_reason", checks)


def add_trade_quality_flags(trades: DataFrame) -> DataFrame:
    checks = (
        F.when(F.col("symbol").isNull() | F.col("timestamp").isNull(), "missing_key")
        .when(F.col("price") <= 0, "non_positive_price")
        .when(F.col("size") <= 0, "non_positive_size")
        .otherwise(F.lit(None).cast("string"))
    )
    return trades.withColumn("_quality_reason", checks)


def add_quality_flags(bars: DataFrame, max_abs_return: float = 0.5) -> DataFrame:
    """Adds a `_quality_reason` column: null if the row passes all checks, otherwise
    a short string naming the first failed check. Does not drop or filter anything --
    that's the caller's job, so this function stays a pure, side-effect-free mapping.
    """
    w = Window.partitionBy("symbol").orderBy("timestamp")
    prev_close = F.lag("close").over(w)

    checks = (
        F.when(F.col("symbol").isNull() | F.col("timestamp").isNull(), "missing_key")
        .when(F.col("close") <= 0, "non_positive_close")
        .when(F.col("open") <= 0, "non_positive_open")
        .when(F.col("volume") < 0, "negative_volume")
        .when(F.col("high") < F.col("low"), "high_lt_low")
        .when(F.col("high") < F.col("open"), "high_lt_open")
        .when(F.col("high") < F.col("close"), "high_lt_close")
        .when(F.col("low") > F.col("open"), "low_gt_open")
        .when(F.col("low") > F.col("close"), "low_gt_close")
        .when(
            prev_close.isNotNull() & (F.abs(F.col("close") / prev_close - 1) > max_abs_return),
            "implausible_return",
        )
        .otherwise(F.lit(None).cast("string"))
    )

    return bars.withColumn("_quality_reason", checks)


def deduplicate(df: DataFrame, keys=("symbol", "timestamp")) -> DataFrame:
    """Keeps the most recently ingested row per key -- makes bronze->silver idempotent
    even if the same source rows get re-ingested (e.g. after an incremental-load retry).
    """
    w = Window.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def split_good_bad(flagged: DataFrame):
    """Splits a DataFrame produced by add_quality_flags() into (good, bad)."""
    good = flagged.filter(F.col("_quality_reason").isNull()).drop("_quality_reason")
    bad = flagged.filter(F.col("_quality_reason").isNotNull())
    return good, bad


def quarantine_rate_pct(flagged: DataFrame) -> float:
    total = flagged.count()
    if total == 0:
        return 0.0
    bad = flagged.filter(F.col("_quality_reason").isNotNull()).count()
    return 100.0 * bad / total
