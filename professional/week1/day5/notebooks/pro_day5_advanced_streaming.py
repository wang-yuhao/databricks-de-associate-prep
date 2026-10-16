# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 — Advanced Structured Streaming
# MAGIC **Exam weight: ~10%**

# COMMAND ----------
# MAGIC %md ## Setup

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS training")
spark.sql("CREATE SCHEMA IF NOT EXISTS training.prep")
spark.sql("CREATE VOLUME IF NOT EXISTS training.prep.landing")

import time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

base_path     = "/Volumes/training/prep/landing"
events_path   = f"{base_path}/streaming_events"
chk_base      = f"{base_path}/checkpoints"

for p in [events_path]:
    dbutils.fs.mkdirs(p)

print("Paths ready")

# COMMAND ----------
# MAGIC %md ## 5.1 Generate streaming events

# COMMAND ----------
import json, random
from datetime import datetime, timedelta

regions = ["EU", "US", "APAC"]
base_ts = datetime(2024, 1, 15, 10, 0, 0)

events = []
for i in range(40):
    ts = base_ts + timedelta(minutes=i * 2)
    events.append({
        "event_id":        i + 1,
        "user_id":         random.randint(1, 5),
        "region":          random.choice(regions),
        "amount":          round(random.uniform(10, 500), 2),
        "event_timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    })

# Write in two batches to simulate arrival
for i, ev in enumerate(events):
    dbutils.fs.put(f"{events_path}/event_{i:03d}.json", json.dumps(ev), overwrite=True)

print(f"Written {len(events)} events")

# COMMAND ----------
# MAGIC %md ## 5.2 Window Aggregations — Tumbling Window

# COMMAND ----------
events_schema = StructType([
    StructField("event_id",        LongType()),
    StructField("user_id",         LongType()),
    StructField("region",          StringType()),
    StructField("amount",          DoubleType()),
    StructField("event_timestamp", StringType()),
])

events_stream = (
    spark.readStream
    .schema(events_schema)
    .json(events_path)
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
)

# 10-minute tumbling window
tumbling = (
    events_stream
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        F.window(F.col("event_timestamp"), "10 minutes"),
        F.col("region")
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum("amount").alias("total_amount")
    )
    .select("window.start", "window.end", "region", "event_count", "total_amount")
)

tumbling_query = (
    tumbling.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("tumbling_results")
    .option("checkpointLocation", f"{chk_base}/tumbling")
    .trigger(availableNow=True)
    .start()
)
tumbling_query.awaitTermination()
spark.sql("SELECT * FROM tumbling_results ORDER BY start, region").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 5.3 Sliding Window

# COMMAND ----------
sliding = (
    events_stream
    .withWatermark("event_timestamp", "10 minutes")
    .groupBy(
        F.window(F.col("event_timestamp"), "20 minutes", "10 minutes"),  # 20-min window, slide every 10
        F.col("region")
    )
    .agg(F.count("*").alias("event_count"))
    .select("window.start", "window.end", "region", "event_count")
)

sliding_query = (
    sliding.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("sliding_results")
    .option("checkpointLocation", f"{chk_base}/sliding")
    .trigger(availableNow=True)
    .start()
)
sliding_query.awaitTermination()
spark.sql("SELECT * FROM sliding_results ORDER BY start, region LIMIT 10").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 5.4 Output Modes Demo
# MAGIC
# MAGIC | Mode      | Description                                |
# MAGIC |-----------|--------------------------------------------|
# MAGIC | append    | New rows only; requires watermark for aggs |
# MAGIC | update    | Changed rows only; good for aggregations   |
# MAGIC | complete  | All rows; state grows unbounded            |

# COMMAND ----------
# Update mode aggregation
update_agg = (
    events_stream
    .withWatermark("event_timestamp", "5 minutes")
    .groupBy("region")
    .agg(F.sum("amount").alias("total_amount"))
)

update_query = (
    update_agg.writeStream
    .outputMode("update")
    .format("memory")
    .queryName("update_results")
    .option("checkpointLocation", f"{chk_base}/update_mode")
    .trigger(availableNow=True)
    .start()
)
update_query.awaitTermination()
spark.sql("SELECT * FROM update_results ORDER BY total_amount DESC").show()

# COMMAND ----------
# MAGIC %md ## 5.5 Triggers
# MAGIC
# MAGIC ```python
# MAGIC # Default: process as fast as possible
# MAGIC .trigger()
# MAGIC
# MAGIC # Fixed interval
# MAGIC .trigger(processingTime="30 seconds")
# MAGIC
# MAGIC # Process all available data then stop (modern replacement for once=True)
# MAGIC .trigger(availableNow=True)
# MAGIC
# MAGIC # Ultra-low latency (no aggregations)
# MAGIC .trigger(continuous="1 second")
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## 5.6 Stream-Static Join

# COMMAND ----------
# Static lookup
static_schema = "user_id BIGINT, user_name STRING, tier STRING"
static_users = spark.createDataFrame(
    [(1, "Alice", "gold"), (2, "Bob", "silver"), (3, "Carol", "gold"),
     (4, "Dave", "bronze"), (5, "Eve", "silver")],
    static_schema
)
static_users.createOrReplaceTempView("static_users")

# Join stream with static
enriched = (
    events_stream
    .join(
        static_users,
        events_stream["user_id"] == static_users["user_id"],
        "left"
    )
    .select(
        events_stream["event_id"],
        events_stream["region"],
        events_stream["amount"],
        static_users["user_name"],
        static_users["tier"]
    )
)

enriched_query = (
    enriched.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("enriched_events")
    .option("checkpointLocation", f"{chk_base}/enriched")
    .trigger(availableNow=True)
    .start()
)
enriched_query.awaitTermination()
spark.sql("SELECT * FROM enriched_events ORDER BY event_id LIMIT 10").show()

# COMMAND ----------
# MAGIC %md ## 5.7 Query Monitoring

# COMMAND ----------
# Re-run a simple stream and inspect progress
monitor_query = (
    events_stream.writeStream
    .outputMode("append")
    .format("memory")
    .queryName("monitor_test")
    .option("checkpointLocation", f"{chk_base}/monitor")
    .trigger(availableNow=True)
    .start()
)
monitor_query.awaitTermination()

progress = monitor_query.lastProgress
if progress:
    print("Batch ID         :", progress.get("batchId"))
    print("Input rows       :", progress.get("numInputRows"))
    print("Rows/sec (in)    :", progress.get("inputRowsPerSecond"))
    print("Rows/sec (proc)  :", progress.get("processedRowsPerSecond"))

# List all active (now finished) streams
print("Active streams:", [q.name for q in spark.streams.active])

# COMMAND ----------
# MAGIC %md ## ✅ Key Takeaways
# MAGIC - Watermark = `max_event_time - delay`; state dropped after threshold
# MAGIC - `append` mode: new rows only; needs watermark for windowed aggregations
# MAGIC - `update` mode: changed rows; good for aggregations
# MAGIC - `complete` mode: all rows every batch; state grows unboundedly
# MAGIC - `trigger(availableNow=True)` replaces deprecated `trigger(once=True)`
# MAGIC - Stream-stream joins require watermarks on **both** sides
# MAGIC - `query.lastProgress` exposes `numInputRows`, `inputRowsPerSecond`, `batchId`
# MAGIC - `continuous` trigger = millisecond latency but no aggregations
