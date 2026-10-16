from datetime import date

from orders_lib.transforms import clean_orders, daily_summary

SCHEMA = "order_id STRING, amount DOUBLE, order_ts STRING"


def test_clean_orders_drops_invalid_and_duplicates(spark):
    df = spark.createDataFrame(
        [("o1", 10.0, None), (None, 5.0, None), ("o2", 0.0, None), ("o3", 1.0, None), ("o3", 1.0, None)],
        SCHEMA)
    assert sorted(r.order_id for r in clean_orders(df).collect()) == ["o1", "o3"]


def test_daily_summary_aggregates_per_day(spark):
    df = spark.createDataFrame(
        [("o1", 10.0, "2026-10-01 10:00:00"), ("o2", 30.0, "2026-10-01 12:00:00"),
         ("o3", 5.0, "2026-10-02 08:00:00")], SCHEMA)
    rows = {r.order_date: r for r in daily_summary(df).collect()}
    assert rows[date(2026, 10, 1)].orders == 2 and rows[date(2026, 10, 1)].revenue == 40.0
    assert rows[date(2026, 10, 2)].avg_order_value == 5.0
