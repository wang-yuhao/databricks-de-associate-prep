# SOLUTION - Lab: Building a Multi-Source E-commerce Pipeline with SDP
# Two sources with different formats and column names -> ONE unified Bronze table (multi-flow).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

dp.create_streaming_table(
    name="orders_bronze",
    comment="Orders from all channels in one schema.",
    cluster_by=["source_region"],
)


@dp.append_flow(target="orders_bronze", name="orders_eu_flow")
def orders_eu_flow():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema("order_id STRING, customer_id STRING, product_id STRING, qty STRING, "
                    "unit_price STRING, order_ts STRING")
            .load(spark.conf.get("orders_eu_source"))
            .select("order_id",
                    F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
                    "product_id",
                    F.expr("try_cast(qty AS INT)").alias("quantity"),
                    F.expr("try_cast(unit_price AS DOUBLE)").alias("unit_price"),
                    F.try_to_timestamp("order_ts").alias("order_ts"),
                    F.lit("EU").alias("source_region"),
                    F.col("_metadata.file_path").alias("source_file")))


@dp.append_flow(target="orders_bronze", name="orders_us_flow")
def orders_us_flow():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema("orderId STRING, custId BIGINT, sku STRING, quantity INT, price DOUBLE, ts STRING")
            .load(spark.conf.get("orders_us_source"))
            .select(F.col("orderId").alias("order_id"),
                    F.col("custId").alias("customer_id"),
                    F.col("sku").alias("product_id"),
                    "quantity",
                    F.col("price").alias("unit_price"),
                    F.try_to_timestamp("ts").alias("order_ts"),
                    F.lit("US").alias("source_region"),
                    F.col("_metadata.file_path").alias("source_file")))
