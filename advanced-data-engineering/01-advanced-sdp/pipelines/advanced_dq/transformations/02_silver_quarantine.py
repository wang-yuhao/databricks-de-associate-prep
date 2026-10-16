# Rules live in a Delta table (governed, reusable across pipelines) instead of being hard-coded.
# Pattern: flag every row with is_quarantined, then split into valid / quarantined datasets,
# so no record is ever lost and the rejected ones stay auditable.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def get_rules(tag: str) -> dict[str, str]:
    rules = spark.read.table(spark.conf.get("rules_table")).filter(F.col("tag") == tag).collect()
    return {r["name"]: r["constraint"] for r in rules}


rules = get_rules("orders")
quarantine_rule = "NOT ({})".format(" AND ".join(f"({c})" for c in rules.values()))


@dp.table(name="orders_dq_flagged", comment="Every order with an is_quarantined flag.",
          partition_cols=["is_quarantined"])
@dp.expect_all(rules)   # warn: metrics per rule in the event log, rows kept
def orders_dq_flagged():
    return (spark.readStream.table("orders_bronze")
            .select(F.col("orderId").alias("order_id"), F.col("custId").alias("customer_id"),
                    F.col("sku").alias("product_id"), "quantity", "price",
                    F.try_to_timestamp("ts").alias("order_ts"), "_rescued_data")
            # Fail safe: if a rule ever returns NULL, quarantine the row instead of letting it through.
            .withColumn("is_quarantined", F.coalesce(F.expr(quarantine_rule), F.lit(True))))


@dp.table(name="orders_quarantine", comment="Rejected orders, with the rules they broke.")
def orders_quarantine():
    failed = [F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in rules.items()]
    return (spark.readStream.table("orders_dq_flagged").filter("is_quarantined")
            .withColumn("failed_rules", F.array_compact(F.array(*failed))))


@dp.temporary_view(name="orders_valid")
def orders_valid():
    return spark.readStream.table("orders_dq_flagged").filter("NOT is_quarantined")


@dp.table(name="orders_silver", comment="Valid orders enriched with product data.",
          cluster_by=["order_date", "category"])
@dp.expect_or_drop("known_product", "category IS NOT NULL")   # referential integrity (stream-static join)
def orders_silver():
    products = spark.read.table("products")
    return (spark.readStream.table("orders_valid")
            .join(products.select("product_id", "category"), "product_id", "left")
            .withColumn("order_date", F.to_date("order_ts"))
            .withColumn("revenue", F.round(F.col("quantity") * F.col("price"), 2))
            .drop("is_quarantined"))
