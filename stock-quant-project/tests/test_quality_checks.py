from datetime import datetime

from chispa.dataframe_comparer import assert_df_equality

from src.transform.quality_checks import (
    add_quality_flags,
    add_quote_quality_flags,
    add_trade_quality_flags,
    deduplicate,
    split_good_bad,
)


def _row(symbol, ts, o, h, l, c, v, ingested="2026-01-01T00:00:00"):
    return {
        "symbol": symbol,
        "timestamp": datetime.fromisoformat(ts),
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
        "_ingested_at": datetime.fromisoformat(ingested),
    }


def test_valid_bar_passes_all_checks(spark):
    df = spark.createDataFrame(
        [_row("AAPL", "2026-01-02T00:00:00", 100.0, 102.0, 99.0, 101.0, 1_000_000)]
    )
    flagged = add_quality_flags(df)
    result = flagged.select("_quality_reason").collect()
    assert result[0]["_quality_reason"] is None


def test_high_less_than_low_is_caught(spark):
    df = spark.createDataFrame(
        [_row("AAPL", "2026-01-02T00:00:00", 100.0, 90.0, 99.0, 101.0, 1_000_000)]
    )
    flagged = add_quality_flags(df)
    reason = flagged.select("_quality_reason").collect()[0]["_quality_reason"]
    assert reason == "high_lt_low"


def test_negative_volume_is_caught(spark):
    df = spark.createDataFrame(
        [_row("AAPL", "2026-01-02T00:00:00", 100.0, 102.0, 99.0, 101.0, -5)]
    )
    flagged = add_quality_flags(df)
    reason = flagged.select("_quality_reason").collect()[0]["_quality_reason"]
    assert reason == "negative_volume"


def test_implausible_return_is_caught(spark):
    rows = [
        _row("AAPL", "2026-01-02T00:00:00", 100.0, 101.0, 99.0, 100.0, 1_000_000),
        _row("AAPL", "2026-01-03T00:00:00", 100.0, 500.0, 99.0, 400.0, 1_000_000),  # +300% jump
    ]
    df = spark.createDataFrame(rows)
    flagged = add_quality_flags(df, max_abs_return=0.5)
    reasons = [r["_quality_reason"] for r in flagged.orderBy("timestamp").collect()]
    assert reasons == [None, "implausible_return"]


def test_split_good_bad_partitions_correctly(spark):
    rows = [
        _row("AAPL", "2026-01-02T00:00:00", 100.0, 102.0, 99.0, 101.0, 1_000_000),  # good
        _row("AAPL", "2026-01-03T00:00:00", 100.0, 90.0, 99.0, 101.0, 1_000_000),   # bad: high<low
    ]
    df = spark.createDataFrame(rows)
    flagged = add_quality_flags(df)
    good, bad = split_good_bad(flagged)
    assert good.count() == 1
    assert bad.count() == 1
    assert "_quality_reason" not in good.columns


def test_deduplicate_keeps_most_recently_ingested(spark):
    rows = [
        _row("AAPL", "2026-01-02T00:00:00", 100.0, 102.0, 99.0, 101.0, 1_000_000, ingested="2026-01-01T00:00:00"),
        _row("AAPL", "2026-01-02T00:00:00", 100.0, 102.0, 99.0, 105.0, 1_000_000, ingested="2026-01-02T00:00:00"),
    ]
    df = spark.createDataFrame(rows)
    deduped = deduplicate(df)
    assert deduped.count() == 1
    assert deduped.collect()[0]["close"] == 105.0


def test_crossed_quote_is_caught(spark):
    df = spark.createDataFrame(
        [{"symbol": "AAPL", "timestamp": datetime(2026, 1, 2), "bid_price": 101.0, "ask_price": 100.0,
          "bid_size": 100, "ask_size": 100}]
    )
    flagged = add_quote_quality_flags(df)
    assert flagged.collect()[0]["_quality_reason"] == "crossed_quote"


def test_valid_quote_passes(spark):
    df = spark.createDataFrame(
        [{"symbol": "AAPL", "timestamp": datetime(2026, 1, 2), "bid_price": 100.0, "ask_price": 100.05,
          "bid_size": 100, "ask_size": 100}]
    )
    flagged = add_quote_quality_flags(df)
    assert flagged.collect()[0]["_quality_reason"] is None


def test_non_positive_trade_price_is_caught(spark):
    df = spark.createDataFrame(
        [{"symbol": "AAPL", "timestamp": datetime(2026, 1, 2), "price": -5.0, "size": 10}]
    )
    flagged = add_trade_quality_flags(df)
    assert flagged.collect()[0]["_quality_reason"] == "non_positive_price"
