# Adding a NEW source to an existing streaming table = adding one more append flow.
# The existing flows keep their checkpoints: no full refresh, no reprocessing of flow_a/flow_b.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

EVENT_SCHEMA = "event_id STRING, device_id STRING, status STRING, amount DOUBLE, event_ts STRING"


@dp.append_flow(target="bronze_events", name="flow_c")
def flow_c():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(EVENT_SCHEMA)
            .load(spark.conf.get("source_flow_c"))
            .withColumn("source_flow", F.lit("flow_c"))
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("ingested_at", F.current_timestamp()))
