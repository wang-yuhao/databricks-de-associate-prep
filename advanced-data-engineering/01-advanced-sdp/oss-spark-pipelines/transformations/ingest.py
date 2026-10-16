# Runs on the OPEN-SOURCE Spark Declarative Pipelines engine (Apache Spark 4.1+), not just Databricks.
# Only features the OSS engine supports are used here: streaming table + append flows, a temporary
# view, a materialized view, and a Delta sink.  (No expectations / cluster_by / AUTO CDC.)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

EVENT_SCHEMA = "event_id STRING, device_id STRING, status STRING, amount DOUBLE, event_ts STRING"

dp.create_streaming_table("bronze_events", comment="Both landing zones in one table.")


@dp.append_flow(target="bronze_events")
def flow_a():
    return (spark.readStream.schema(EVENT_SCHEMA).json(spark.conf.get("spark.adelab.source_a"))
            .withColumn("source_flow", F.lit("flow_a")))


@dp.append_flow(target="bronze_events")
def flow_b():
    return (spark.readStream.schema(EVENT_SCHEMA).json(spark.conf.get("spark.adelab.source_b"))
            .withColumn("source_flow", F.lit("flow_b")))


@dp.temporary_view
def clean_events():
    return (spark.readStream.table("bronze_events")
            .filter("event_ts IS NOT NULL AND status IN ('ACTIVE','INACTIVE','PENDING','ERROR')")
            .withColumn("event_date", F.to_date("event_ts")))


@dp.table(comment="Validated events (streaming table fed by a temporary view).")
def silver_events():
    return spark.readStream.table("clean_events")


dp.create_sink("error_events_export", format="delta",
               options={"path": spark.conf.get("spark.adelab.sink_path")})


@dp.append_flow(target="error_events_export")
def export_errors():
    return spark.readStream.table("silver_events").filter("status = 'ERROR'")
