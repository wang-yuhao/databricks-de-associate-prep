from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Raw orders from the landing volume.")
def orders_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema("order_id STRING, customer_id INT, amount DOUBLE, order_ts TIMESTAMP")
            .load(spark.conf.get("landing_path"))
            .withColumn("source_file", F.col("_metadata.file_path")))


@dp.table(comment="Valid orders.", cluster_by=["order_date"])
@dp.expect_or_drop("positive_amount", "amount > 0")
@dp.expect_or_fail("valid_id", "order_id IS NOT NULL")
def orders_silver():
    return spark.readStream.table("orders_bronze").withColumn("order_date", F.to_date("order_ts"))


@dp.materialized_view(comment="Revenue per day.")
def daily_revenue():
    return (spark.read.table("orders_silver").groupBy("order_date")
            .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue")))
