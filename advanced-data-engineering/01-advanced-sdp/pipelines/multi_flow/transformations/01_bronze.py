# Bronze: two independent sources append into ONE streaming table (multi-flow).
# Identical code runs in a Databricks pipeline (Lakeflow Spark Declarative Pipelines).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

EVENT_SCHEMA = "event_id STRING, device_id STRING, status STRING, amount DOUBLE, event_ts STRING"

dp.create_streaming_table(
    name="bronze_events",
    comment="Raw device events from all landing zones (one append flow per source).",
)


def _read_source(path: str, flow_name: str):
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(EVENT_SCHEMA)
            .load(path)
            .withColumn("source_flow", F.lit(flow_name))
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("ingested_at", F.current_timestamp()))


@dp.append_flow(target="bronze_events", name="flow_a")
def flow_a():
    return _read_source(spark.conf.get("source_flow_a"), "flow_a")


@dp.append_flow(target="bronze_events", name="flow_b")
def flow_b():
    return _read_source(spark.conf.get("source_flow_b"), "flow_b")
