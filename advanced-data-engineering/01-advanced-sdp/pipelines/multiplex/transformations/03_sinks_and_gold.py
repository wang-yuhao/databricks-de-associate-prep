# Delta sink: stream records OUT of the pipeline to an external Delta location (e.g. for another team).
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dp.create_sink(
    name="clickstream_export",
    format="delta",
    options={"path": spark.conf.get("clickstream_sink_path")},
)


@dp.append_flow(target="clickstream_export", name="export_checkout_clicks")
def export_checkout_clicks():
    return (spark.readStream.table("silver_clickstream")
            .filter(F.col("page").isin("/cart", "/checkout"))
            .select("session_id", "page", "device", "event_ts"))


@dp.materialized_view(name="gold_orders_by_region", comment="Order revenue by customer region.")
def gold_orders_by_region():
    latest_customer = (spark.read.table("silver_customers")
                       .withColumn("rn", F.row_number().over(
                           Window.partitionBy("customer_id").orderBy(F.col("updated_ts").desc())))
                       .filter("rn = 1").drop("rn"))
    return (spark.read.table("silver_orders").alias("o")
            .join(latest_customer.alias("c"), "customer_id", "left")
            .groupBy(F.coalesce("c.region", F.lit("UNKNOWN")).alias("region"))
            .agg(F.count("*").alias("orders"), F.round(F.sum("o.total"), 2).alias("revenue")))
