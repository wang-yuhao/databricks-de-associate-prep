# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 — Structured Streaming
# MAGIC ### ☁️ Azure Databricks Edition
# MAGIC
# MAGIC **Catalog / Schema:** `training.prep`
# MAGIC **Checkpoints:** `/Volumes/training/prep/landing/checkpoints/<query>/`
# MAGIC **Source files:** `/Volumes/training/prep/landing/stream_events/`
# MAGIC
# MAGIC All streaming sinks write to Unity Catalog managed Delta tables.
# MAGIC Never use `/tmp/` for checkpoints — use Volumes instead.

# COMMAND ----------
spark.sql("USE CATALOG training")
spark.sql("USE SCHEMA prep")
print(spark.sql("SELECT current_catalog(), current_schema(), current_user()").collect()[0])

# COMMAND ----------
# MAGIC %md ## 1. Basic Stream: Delta Table → Delta Table

# COMMAND ----------
from pyspark.sql.functions import current_timestamp
import random

# Create source Delta table
random.seed(42)
data = [(i, f"user_{i%10}", random.choice(["click","view","purchase"]), float(i*1.5))
        for i in range(1, 101)]
df_src = spark.createDataFrame(data, ["event_id","user_id","event_type","value"])
df_src = df_src.withColumn("event_time", current_timestamp())
df_src.write.mode("overwrite").saveAsTable("training.prep.d4_stream_source")
print("Source rows:", spark.table("training.prep.d4_stream_source").count())

# COMMAND ----------
# Stream source Delta → sink Delta  (availableNow = process all, then stop)
CHK1 = "/Volumes/training/prep/landing/checkpoints/d4_basic_stream"

q1 = (
    spark.readStream
    .format("delta")
    .table("training.prep.d4_stream_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHK1)
    .trigger(availableNow=True)      # preferred over trigger(once=True)
    .toTable("training.prep.d4_stream_sink")
)
q1.awaitTermination()
print("Sink rows after run 1:", spark.table("training.prep.d4_stream_sink").count())  # 100

# COMMAND ----------
# Append 10 more rows to source; only those are processed on second run
new_data = [(i, f"user_{i%10}", "click", float(i*2.0)) for i in range(101, 111)]
df_new = spark.createDataFrame(new_data, ["event_id","user_id","event_type","value"])
df_new = df_new.withColumn("event_time", current_timestamp())
df_new.write.mode("append").saveAsTable("training.prep.d4_stream_source")

q2 = (
    spark.readStream.format("delta").table("training.prep.d4_stream_source")
    .writeStream.format("delta")
    .option("checkpointLocation", CHK1)  # same checkpoint → resumes from last offset
    .trigger(availableNow=True)
    .toTable("training.prep.d4_stream_sink")
)
q2.awaitTermination()
print("Sink rows after run 2:", spark.table("training.prep.d4_stream_sink").count())  # 110
# Note: exactly 110, no duplicates — checkpoint ensures exactly-once

# COMMAND ----------
# MAGIC %md ## 2. Windowed Aggregation with Watermark

# COMMAND ----------
import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

# Create timestamp-rich source
rows, base = [], datetime.datetime(2024, 1, 1, 10, 0, 0)
for i in range(200):
    rows.append((i, f"user_{i%5}", "click", float(i), base + datetime.timedelta(seconds=i*30)))

schema = StructType([
    StructField("id",         IntegerType()),
    StructField("user_id",    StringType()),
    StructField("event",      StringType()),
    StructField("amount",     DoubleType()),
    StructField("event_time", TimestampType()),
])
spark.createDataFrame(rows, schema).write.mode("overwrite").saveAsTable("training.prep.d4_stream_source_ts")
print("Timestamp source rows:", spark.table("training.prep.d4_stream_source_ts").count())

# COMMAND ----------
from pyspark.sql.functions import window, col, count, sum as spark_sum

CHK2 = "/Volumes/training/prep/landing/checkpoints/d4_windowed_agg"

q3 = (
    spark.readStream.format("delta").table("training.prep.d4_stream_source_ts")
    .withWatermark("event_time", "10 minutes")      # tolerate up to 10 min late data
    .groupBy(
        window(col("event_time"), "5 minutes"),     # 5-min tumbling window
        col("user_id")
    )
    .agg(
        count("*").alias("event_count"),
        spark_sum("amount").alias("total_amount"),
    )
    .writeStream
    .format("delta")
    .outputMode("append")                           # append required with watermark
    .option("checkpointLocation", CHK2)
    .trigger(availableNow=True)
    .toTable("training.prep.d4_windowed_agg")
)
q3.awaitTermination()

result = spark.table("training.prep.d4_windowed_agg")
print("Window rows:", result.count())
result.orderBy("window.start", "user_id").show(10, truncate=False)

# COMMAND ----------
# MAGIC %md ## 3. Trigger Types Reference

# COMMAND ----------
print("""
Trigger                         | Code                                     | Behaviour
--------------------------------|------------------------------------------|-------------------------------------------
availableNow (preferred)        | trigger(availableNow=True)               | Process all current data, stop. No duplicates.
processingTime (continuous run) | trigger(processingTime='30 seconds')     | New micro-batch every 30 s. Runs indefinitely.
once (DEPRECATED)               | trigger(once=True)                       | Deprecated in DBR 10.1. Use availableNow.
continuous (low latency)        | trigger(continuous='1 second')           | Sub-ms latency; limited operations supported.

Rule: availableNow replaces once. Use processingTime for true streaming pipelines.
""")

# COMMAND ----------
# MAGIC %md ## 4. Auto Loader (cloudFiles) — File Ingestion from Volume

# COMMAND ----------
import json

# Simulate files landing in a Volume folder
batch1 = [{"event_id": f"E{i:04d}", "user": f"user_{i%20}", "type": "purchase", "amount": i*5.0}
          for i in range(1, 21)]
for i, ev in enumerate(batch1):
    dbutils.fs.put(
        f"/Volumes/training/prep/landing/stream_events/ev_{i:04d}.json",
        json.dumps(ev), overwrite=True
    )
print("Batch 1 files written:", len(batch1))

# COMMAND ----------
CHK_AL = "/Volumes/training/prep/landing/checkpoints/d4_autoloader"

# Auto Loader — incrementally ingest new files without duplicates
q4 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", CHK_AL + "/schema")
    .load("/Volumes/training/prep/landing/stream_events/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHK_AL + "/data")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("training.prep.d4_bronze_events")
)
q4.awaitTermination()
print("After batch 1:", spark.table("training.prep.d4_bronze_events").count())  # 20

# COMMAND ----------
# Add batch 2 — Auto Loader only ingests NEW files
batch2 = [{"event_id": f"E{i:04d}", "user": f"user_{i%20}", "type": "view", "amount": i*2.0}
          for i in range(21, 31)]
for i, ev in enumerate(batch2):
    dbutils.fs.put(
        f"/Volumes/training/prep/landing/stream_events/new_ev_{i:04d}.json",
        json.dumps(ev), overwrite=True
    )

q5 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", CHK_AL + "/schema")
    .load("/Volumes/training/prep/landing/stream_events/")
    .writeStream.format("delta")
    .option("checkpointLocation", CHK_AL + "/data")  # same checkpoint = resume
    .trigger(availableNow=True)
    .toTable("training.prep.d4_bronze_events")
)
q5.awaitTermination()
print("After batch 2:", spark.table("training.prep.d4_bronze_events").count())  # 30, no duplicates

# COMMAND ----------
# MAGIC %md ## 5. Checkpoint Deep Dive

# COMMAND ----------
# Inspect checkpoint directory structure
display(dbutils.fs.ls(CHK1))
# You should see: commits/, offsets/, metadata
# commits/ → which micro-batches have been written to sink
# offsets/ → which Delta version was last read from source

print("""
Checkpoint Rules (critical for exam):
1. Every streaming query MUST have a unique checkpointLocation
2. Never share a checkpoint between two different queries
3. Changing the checkpointLocation = fresh start (reprocesses all data)
4. Deleting the checkpoint = reprocesses from the beginning
5. Always store in /Volumes/ (durable) — never /tmp/ (ephemeral)
""")

# COMMAND ----------
# MAGIC %md ## 6. Cleanup (Optional)

# COMMAND ----------
# Uncomment to clean up after this notebook
# for tbl in ["d4_stream_source","d4_stream_sink","d4_stream_source_ts",
#             "d4_windowed_agg","d4_bronze_events"]:
#     spark.sql(f"DROP TABLE IF EXISTS training.prep.{tbl}")
# dbutils.fs.rm("/Volumes/training/prep/landing/checkpoints/d4_", recurse=True)
# dbutils.fs.rm("/Volumes/training/prep/landing/stream_events/", recurse=True)
print("Cleanup skipped — tables preserved for reference")
