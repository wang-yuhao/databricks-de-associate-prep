# SOLUTION - Silver with expectations, quarantine and Liquid Clustering; Gold business views.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORDER_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "positive_quantity": "quantity IS NOT NULL AND quantity > 0",   # never NULL: explicit about missing values
    "valid_price": "unit_price IS NOT NULL AND unit_price > 0",
    "valid_timestamp": "order_ts IS NOT NULL",
}


@dp.table(name="orders_silver", comment="Valid orders from all channels.",
          cluster_by=["order_date", "source_region"])
@dp.expect_all_or_drop(ORDER_RULES)
def orders_silver():
    return (spark.readStream.table("orders_bronze")
            .withColumn("order_date", F.to_date("order_ts"))
            .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))


@dp.table(name="orders_quarantine", comment="Orders rejected by orders_silver, with the failed rules.")
def orders_quarantine():
    failed = [F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in ORDER_RULES.items()]
    return (spark.readStream.table("orders_bronze")
            .withColumn("failed_rules", F.array_compact(F.array(*failed)))
            .filter(F.size("failed_rules") > 0))


@dp.materialized_view(name="revenue_by_category_daily", comment="Daily revenue per product category.")
@dp.expect("known_category", "category IS NOT NULL")
def revenue_by_category_daily():
    return (spark.read.table("orders_silver")
            .join(spark.read.table("products").select("product_id", "category"), "product_id", "left")
            .groupBy("order_date", "category")
            .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue")))


@dp.materialized_view(name="revenue_by_customer_tier", comment="Revenue per CURRENT customer tier.")
def revenue_by_customer_tier():
    current = spark.read.table("customers").filter("__END_AT IS NULL").select("customer_id", "tier", "region")
    return (spark.read.table("orders_silver")
            .join(current, "customer_id", "left")
            .groupBy(F.coalesce("tier", F.lit("unknown")).alias("tier"))
            .agg(F.countDistinct("customer_id").alias("customers"),
                 F.round(F.sum("amount"), 2).alias("revenue")))
