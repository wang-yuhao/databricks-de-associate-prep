# Multiplex Bronze: ONE streaming table holds every topic of a Kafka-like feed.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_SCHEMA = "topic STRING, key STRING, partition INT, offset BIGINT, timestamp STRING, value STRING"


@dp.table(
    name="bronze_multiplex",
    comment="All topics, raw payloads as JSON strings. Topic is the clustering key for fast fan-out.",
    cluster_by=["topic"],
)
def bronze_multiplex():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(RAW_SCHEMA)
            .load(spark.conf.get("multiplex_source"))
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("ingested_at", F.current_timestamp()))
