# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 — Structured Streaming on Azure Databricks
# MAGIC
# MAGIC **Environment:** Azure Databricks (Unity Catalog enabled)  
# MAGIC **Cluster:** Single Node, DBR 15.x LTS, `Standard_DS3_v2`  
# MAGIC **Estimated time:** 3–4 hours
# MAGIC
# MAGIC ## What you will practice:
# MAGIC - Structured Streaming: readStream / writeStream fundamentals
# MAGIC - Trigger types: AvailableNow, ProcessingTime, Continuous
# MAGIC - Output modes: append, complete, update
# MAGIC - Windowed aggregations with watermarking
# MAGIC - Streaming checkpoints (stored in Unity Catalog Volumes)
# MAGIC - Streaming to/from Delta tables (Unity Catalog)
# MAGIC
# MAGIC ## Azure Note on Checkpoints
# MAGIC Checkpoint locations must be a persistent path. On Azure Databricks use:
# MAGIC - **Unity Catalog Volumes**: `/Volumes/<catalog>/<schema>/<volume>/checkpoints/` ✔️
# MAGIC - **ADLS Gen2** (abfss://): `abfss://<container>@<storage>.dfs.core.windows.net/checkpoints/` ✔️
# MAGIC - `/tmp/` is NOT persistent across cluster restarts — avoid for streaming checkpoints

# COMMAND ----------
# MAGIC %md ## Setup

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS training;
# MAGIC CREATE SCHEMA  IF NOT EXISTS training.day4
# MAGIC   COMMENT 'Day 4: Structured Streaming exercises';
# MAGIC
# MAGIC -- Volume for checkpoint storage (replaces /tmp/ paths)
# MAGIC CREATE VOLUME IF NOT EXISTS training.day4.checkpoints
# MAGIC   COMMENT 'Streaming checkpoint storage for Day 4 exercises';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS training.day4.files
# MAGIC   COMMENT 'Landing files for streaming source exercises';
# MAGIC
# MAGIC USE CATALOG training;
# MAGIC USE SCHEMA day4;
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------
# Define reusable path constants
CHECKPOINT_BASE = "/Volumes/training/day4/checkpoints"
FILES_BASE      = "/Volumes/training/day4/files"
print(f"Checkpoint base: {CHECKPOINT_BASE}")
print(f"Files base:      {FILES_BASE}")

# COMMAND ----------
# MAGIC %md ## Task 1 — Basic Streaming Read + Write (Delta → Delta)

# COMMAND ----------
# Step 1: Create and populate a source Delta table
from pyspark.sql.functions import current_timestamp, col
import random

random.seed(42)
event_types = ["click", "view", "purchase"]

data = [
    (i, f"user_{i % 10}", random.choice(event_types), round(i * 1.5, 2))
    for i in range(1, 101)
]

source_df = spark.createDataFrame(data, ["event_id", "user_id", "event_type", "value"])
source_df = source_df.withColumn("event_time", current_timestamp())

# Save as a Unity Catalog managed Delta table
source_df.write.mode("overwrite").saveAsTable("training.day4.events_source")
print(f"Source table created: training.day4.events_source ({source_df.count()} rows)")

# COMMAND ----------
# Step 2: Read the source as a stream
source_stream = (
    spark.readStream
    .format("delta")
    .table("training.day4.events_source")
)

print("Is streaming:", source_stream.isStreaming)
print("Schema:")
source_stream.printSchema()

# COMMAND ----------
# Step 3: Write stream to a sink Delta table
# - Checkpoint stored in Unity Catalog Volume (persistent, survives cluster restarts)
query1 = (
    source_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/task1")
    .trigger(availableNow=True)   # process all available data, then stop
    .toTable("training.day4.events_sink")  # UC managed table
)

query1.awaitTermination()
print("Stream finished.")

# COMMAND ----------
# Step 4: Verify
sink_df = spark.table("training.day4.events_sink")
print(f"Sink row count: {sink_df.count()} (expected: 100)")
sink_df.show(5)

# COMMAND ----------
# MAGIC %md ## Task 2 — Trigger Types Comparison

# COMMAND ----------
# Clear previous checkpoint so we can rerun with different triggers
dbutils.fs.rm(f"{CHECKPOINT_BASE}/task2_processingtime", recurse=True)

# ProcessingTime trigger — runs micro-batch every N seconds
query2 = (
    spark.readStream
    .format("delta")
    .table("training.day4.events_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/task2_processingtime")
    .trigger(processingTime="5 seconds")  # micro-batch every 5 seconds
    .toTable("training.day4.events_sink_pt")
)

import time
time.sleep(12)   # let it run 2–3 batches
query2.stop()
print("ProcessingTime query stopped.")
print(f"Rows in sink: {spark.table('training.day4.events_sink_pt').count()}")

# COMMAND ----------
# Trigger type summary:
print("""
Trigger Types on Azure Databricks:

  availableNow=True     → Process all pending data in one or more micro-batches, then stop.
                           Best for batch-style jobs scheduled by Lakeflow/Workflows.

  processingTime='Ns'   → Continuous micro-batches every N seconds.
                           Good for near-real-time with low overhead.

  once=True             → DEPRECATED. Use availableNow=True instead.

  continuous='500ms'    → True continuous processing (low-latency, experimental).
                           Requires a persistent cluster; not for Workflows.
""")

# COMMAND ----------
# MAGIC %md ## Task 3 — Windowed Aggregation with Watermarking

# COMMAND ----------
from pyspark.sql.functions import window, count, sum as spark_sum
from pyspark.sql.types import *
import datetime

# Create timestamped source data
rows = []
base_time = datetime.datetime(2024, 1, 1, 10, 0, 0)
for i in range(300):
    ts = base_time + datetime.timedelta(seconds=i * 20)
    rows.append((i, f"user_{i % 5}", "click", float(i % 50 + 1), ts))

schema_ts = StructType([
    StructField("id",         IntegerType()),
    StructField("user_id",    StringType()),
    StructField("event",      StringType()),
    StructField("amount",     DoubleType()),
    StructField("event_time", TimestampType()),
])

df_ts = spark.createDataFrame(rows, schema_ts)
df_ts.write.mode("overwrite").saveAsTable("training.day4.events_with_ts")
print(f"Created training.day4.events_with_ts: {df_ts.count()} rows")

# COMMAND ----------
# Windowed aggregation stream
dbutils.fs.rm(f"{CHECKPOINT_BASE}/task3_windowed", recurse=True)

stream_ts = (
    spark.readStream
    .format("delta")
    .table("training.day4.events_with_ts")
)

agg_stream = (
    stream_ts
    .withWatermark("event_time", "10 minutes")  # drop late data older than 10 min
    .groupBy(
        window(col("event_time"), "5 minutes"),  # 5-min tumbling windows
        col("user_id")
    )
    .agg(
        count("*").alias("event_count"),
        spark_sum("amount").alias("total_amount"),
    )
)

query3 = (
    agg_stream
    .writeStream
    .format("delta")
    .outputMode("append")  # use append with watermark (complete requires full re-agg)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/task3_windowed")
    .trigger(availableNow=True)
    .toTable("training.day4.windowed_agg")
)

query3.awaitTermination()
print("Windowed aggregation complete.")

# COMMAND ----------
result3 = spark.table("training.day4.windowed_agg")
print(f"Aggregated rows: {result3.count()}")
result3.orderBy("window.start", "user_id").show(20, truncate=False)

# COMMAND ----------
# MAGIC %md ## Task 4 — Streaming Metrics and Monitoring

# COMMAND ----------
# Start a ProcessingTime query and inspect its status/metrics
dbutils.fs.rm(f"{CHECKPOINT_BASE}/task4_monitor", recurse=True)

monitor_query = (
    spark.readStream
    .format("delta")
    .table("training.day4.events_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/task4_monitor")
    .trigger(processingTime="10 seconds")
    .toTable("training.day4.events_monitor_sink")
)

import time
time.sleep(15)

# Inspect query status
print("Query status:",  monitor_query.status)
print("Last progress:", monitor_query.lastProgress)
print("Is active:",     monitor_query.isActive)

monitor_query.stop()
print("Query stopped.")

# COMMAND ----------
# MAGIC %md ## Task 5 — Auto Loader as a Streaming Source (with Volume)

# COMMAND ----------
# Auto Loader on Azure Databricks — reads files from a Volume directory incrementally
import json

# Write sample JSON files to the volume (simulating files landing from an upstream process)
for batch_num in range(1, 4):
    batch_data = [
        {"order_id": f"B{batch_num}_{i:03d}", "amount": float(batch_num * 100 + i), "status": "completed"}
        for i in range(1, 6)
    ]
    batch_df = spark.createDataFrame(batch_data)
    batch_df.write.mode("overwrite").json(f"{FILES_BASE}/orders_landing/batch{batch_num}")

print("Written 3 batches of JSON files to volume")
dbutils.fs.ls(f"{FILES_BASE}/orders_landing")

# COMMAND ----------
# Auto Loader streaming read from the Volume
dbutils.fs.rm(f"{CHECKPOINT_BASE}/task5_autoloader", recurse=True)

auto_loader_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/task5_schema")
    .load(f"{FILES_BASE}/orders_landing/")  # Volume path
)

query5 = (
    auto_loader_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/task5_autoloader")
    .trigger(availableNow=True)
    .toTable("training.day4.orders_autoloader")
)

query5.awaitTermination()
result5 = spark.table("training.day4.orders_autoloader")
print(f"Auto Loader ingested {result5.count()} rows")
result5.show()

# COMMAND ----------
# MAGIC %md ## Task 6 — Output Modes Summary

# COMMAND ----------
print("""
Streaming Output Modes:

  append    → Only new rows added since last trigger are output.
               Works with: simple transforms, watermarked aggregations.
               Most common mode for Delta sink.

  complete  → Entire result table is output every trigger.
               Only works with aggregations (no watermark needed).
               Expensive for large result sets.

  update    → Only rows that changed since last trigger are output.
               Works with aggregations and foreachBatch.
               NOT supported by Delta Lake sink directly.

On Azure Databricks with Delta Lake sink:
  - Use 'append' with watermarked aggregations (most common)
  - Use 'complete' only for small aggregations (full rescan each batch)
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 4 Notebook Complete
# MAGIC
# MAGIC **What you practiced:**
# MAGIC - `readStream` from Delta table → `writeStream` to Delta table (Unity Catalog)
# MAGIC - Trigger types: `availableNow`, `processingTime`, `continuous`
# MAGIC - Windowed aggregation with `withWatermark` + `window()`
# MAGIC - Query monitoring: `.status`, `.lastProgress`, `.isActive`
# MAGIC - Auto Loader (`cloudFiles`) from a Unity Catalog Volume
# MAGIC - Output modes: append, complete, update
# MAGIC
# MAGIC **Azure-specific patterns:**
# MAGIC - Checkpoints in `/Volumes/training/day4/checkpoints/` (persistent across restarts)
# MAGIC - `toTable("catalog.schema.table")` for UC-managed streaming sinks
# MAGIC - No `/tmp/` paths (not persistent on Azure Databricks)
# MAGIC - Auto Loader reads from Volume path (no cloud event config needed for labs)
