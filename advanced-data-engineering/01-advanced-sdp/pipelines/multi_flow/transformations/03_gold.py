# Gold: a materialized view, fully recomputed (incrementally on serverless Databricks) each update.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_daily_metrics",
    comment="Daily event metrics per status.",
    cluster_by=["ingest_date"],
)
def gold_daily_metrics():
    return (spark.read.table("silver_events")
            .groupBy("ingest_date", "status")
            .agg(F.count("*").alias("event_count"),
                 F.countDistinct("device_id").alias("distinct_devices"),
                 F.round(F.sum("amount"), 2).alias("total_amount"),
                 F.round(F.avg("amount"), 2).alias("avg_amount")))
