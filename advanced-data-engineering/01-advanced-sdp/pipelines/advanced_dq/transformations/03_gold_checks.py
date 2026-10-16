# Gold + table-level checks. Row-level rules can't prove "no record was lost"; aggregates can.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="daily_category_sales", comment="Revenue per day and category.")
@dp.expect_or_fail("non_negative_revenue", "revenue >= 0")
def daily_category_sales():
    return (spark.read.table("orders_silver").groupBy("order_date", "category")
            .agg(F.count("*").alias("orders"), F.round(F.sum("revenue"), 2).alias("revenue")))


def _rows(table: str, alias: str, condition: str | None = None):
    df = spark.read.table(table)
    return (df.filter(condition) if condition else df).agg(F.count(F.lit(1)).alias(alias))


@dp.materialized_view(name="dq_reconciliation", comment="Zero-data-loss audit across the layers.")
@dp.expect("bronze_fully_flagged", "bronze_rows = flagged_rows")
@dp.expect("flagged_split_complete", "flagged_rows = valid_rows + quarantined_rows")
def dq_reconciliation():
    return (_rows("orders_bronze", "bronze_rows")
            .crossJoin(_rows("orders_dq_flagged", "flagged_rows"))
            .crossJoin(_rows("orders_dq_flagged", "valid_rows", "NOT is_quarantined"))
            .crossJoin(_rows("orders_quarantine", "quarantined_rows"))
            .crossJoin(_rows("orders_silver", "silver_rows")))
