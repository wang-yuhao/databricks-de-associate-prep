# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4: Structured Streaming
# MAGIC **Import into Databricks Community Edition:**
# MAGIC 1. Workspace → Import → File → upload `day4_streaming.py`
# MAGIC 2. Attach to cluster (Runtime 13.x LTS)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Reading a Streaming Source (Rate Source)

# COMMAND ----------
# Rate source generates rows at a configurable rate — great for testing
streaming_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load()

# Check if it's a streaming DataFrame
print(f"Is streaming: {streaming_df.isStreaming}")
streaming_df.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Transformations on Streaming DataFrames

# COMMAND ----------
from pyspark.sql import functions as F

# Add computed columns (same API as batch)
transformed = streaming_df \
    .withColumn("event_type",
        F.when(F.col("value") % 3 == 0, "purchase")
         .when(F.col("value") % 3 == 1, "view")
         .otherwise("click")
    ) \
    .withColumn("user_id", (F.col("value") % 100).cast("int")) \
    .withColumn("amount", F.round(F.rand() * 100, 2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Writing Streams — Output Modes

# COMMAND ----------
# APPEND mode — write new rows to Delta (most common)
query_append = transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/streaming/checkpoints/events") \
    .trigger(processingTime="5 seconds") \
    .start("/tmp/streaming/events")

import time
time.sleep(15)  # let it run for 15 seconds
query_append.stop()

count = spark.read.format("delta").load("/tmp/streaming/events").count()
print(f"Events written: {count}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Trigger Types

# COMMAND ----------
# Trigger.AvailableNow: process all available data, then stop (preferred over Once)
source_df = spark.readStream \
    .format("delta") \
    .load("/tmp/streaming/events")

query_once = source_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/streaming/checkpoints/output_once") \
    .trigger(availableNow=True) \
    .start("/tmp/streaming/output_once")

query_once.awaitTermination()
print("AvailableNow query completed!")

# COMMAND ----------
# Continuous trigger (low latency — uses Databricks Enhanced Autoscaling)
# query_continuous = transformed.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/streaming/checkpoints/continuous") \
#     .trigger(continuous="1 second") \
#     .start("/tmp/streaming/continuous")
# query_continuous.stop()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Streaming Aggregations with Watermarking

# COMMAND ----------
# Re-create the rate stream for aggregation demo
stream2 = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load() \
    .withColumn("event_type",
        F.when(F.col("value") % 2 == 0, "purchase").otherwise("view")
    ) \
    .withColumn("amount", F.round(F.rand() * 100, 2))

# Watermark: tolerate up to 1 minute of late data
agg_df = stream2 \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        F.window(F.col("timestamp"), "30 seconds"),
        F.col("event_type")
    ) \
    .agg(
        F.count("*").alias("event_count"),
        F.sum("amount").alias("total_amount")
    )

# COMPLETE mode: rewrite the entire result table each trigger
query_agg = agg_df.writeStream \
    .format("memory") \
    .queryName("event_agg") \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .start()

time.sleep(35)  # wait for 2+ windows
display(spark.sql("SELECT * FROM event_agg ORDER BY window"))
query_agg.stop()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Streaming with Delta Lake Sink

# COMMAND ----------
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.stream_events;
# MAGIC CREATE TABLE default.stream_events (
# MAGIC   timestamp TIMESTAMP,
# MAGIC   value     BIGINT,
# MAGIC   event_type STRING,
# MAGIC   user_id   INT,
# MAGIC   amount    DOUBLE
# MAGIC ) USING DELTA;

# COMMAND ----------
# Stream to a Delta managed table
stream3 = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 5) \
    .load() \
    .withColumn("event_type", F.when(F.col("value") % 3 == 0, "purchase").when(F.col("value") % 3 == 1, "view").otherwise("click")) \
    .withColumn("user_id", (F.col("value") % 100).cast("int")) \
    .withColumn("amount", F.round(F.rand() * 100, 2))

query_delta = stream3.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/streaming/checkpoints/events_table") \
    .trigger(processingTime="5 seconds") \
    .toTable("default.stream_events")

time.sleep(20)
query_delta.stop()

display(spark.sql("SELECT event_type, COUNT(*) as cnt FROM default.stream_events GROUP BY event_type"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Monitoring Streaming Queries

# COMMAND ----------
# Check active streams
print("Active streams:", len(spark.streams.active))

# Get progress of a running stream
stream4 = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
q = stream4.writeStream.format("memory").queryName("monitor_demo").outputMode("append").start()
time.sleep(10)
print("Last progress:")
print(q.lastProgress)
q.stop()

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 4 Practice Challenges
# MAGIC
# MAGIC 1. Create a rate stream, add transformations, and write to Delta with `processingTime` trigger
# MAGIC 2. Use `trigger(availableNow=True)` to do a one-shot batch of a Delta source
# MAGIC 3. Implement watermarking with `withWatermark("timestamp", "2 minutes")` and group by time window
# MAGIC 4. Query a streaming table mid-stream using `spark.sql(...)` to verify data is arriving
# MAGIC 5. Check `query.lastProgress` to inspect batch statistics (numInputRows, processedRowsPerSecond)
