# Day 4 — Structured Streaming & Streaming with Delta Lake

> **Exam Weight:** ~10% of Domain 3 (Data Processing & Transformations)

---

## 1. Streaming Fundamentals

Databricks Structured Streaming is built on Apache Spark's micro-batch engine. Every streaming query reads from a **source**, applies transformations, and writes to a **sink**.

### Key Concepts

| Term | Definition |
|---|---|
| **Micro-batch** | Default mode — Spark processes data in small batches at a fixed interval |
| **Continuous processing** | Low-latency mode (~1ms) but limited operators |
| **DataFrame** | Streaming queries use the same DataFrame/SQL API as batch |
| **Checkpoint** | Saves query progress to recover from failures |
| **Watermark** | Threshold for how late data can arrive before being dropped |

---

## 2. Reading a Stream

```python
# Read from a Delta table as a stream
stream_df = (
    spark.readStream
    .format("delta")
    .load("/mnt/data/bronze/events")
)

# Read from Auto Loader (cloud files)
stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema")
    .load("/mnt/raw/landing/")
)
```

### Supported Sources
- **Delta tables** — most common in the exam
- **Auto Loader** (`cloudFiles`) — for raw files in cloud storage
- **Kafka** — event streaming
- **Rate source** — for testing only

---

## 3. Writing a Stream — Trigger Types 🔑

This is heavily tested. Memorize the 4 trigger types:

| Trigger | Code | Behavior |
|---|---|---|
| **Default (unset)** | `trigger()` not called | Processes as fast as possible, continuously |
| **ProcessingTime** | `.trigger(processingTime="1 minute")` | Fixed interval (every N seconds/minutes) |
| **Once** | `.trigger(once=True)` | Runs ONE micro-batch, then stops (**deprecated**, use AvailableNow) |
| **AvailableNow** | `.trigger(availableNow=True)` | Processes all available data, multiple micro-batches, then stops |
| **Continuous** | `.trigger(continuous="1 second")` | True continuous, low latency, very limited operators |

> ⚠️ **Exam tip:** `once=True` and `availableNow=True` are frequently confused. `availableNow` is the modern replacement for `once` and processes ALL backlogged data efficiently (not just one batch).

```python
# Example: write stream with AvailableNow trigger
query = (
    stream_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/my_stream")
    .trigger(availableNow=True)
    .start("/mnt/data/silver/events")
)
query.awaitTermination()
```

---

## 4. Output Modes

| Mode | When to Use |
|---|---|
| **append** | Only new rows added to result table (stateless or windowed append only) |
| **complete** | Entire result table rewritten each micro-batch (aggregations) |
| **update** | Only changed rows updated (aggregations without watermark) |

> ⚠️ **Exam tip:** `append` is most common for ETL pipelines. `complete` is for aggregations where you need the full result.

---

## 5. Watermarking

Watermarking defines how long to wait for late-arriving data before dropping it.

```python
from pyspark.sql.functions import window, col

# Accept events up to 10 minutes late
windowed = (
    stream_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .count()
)
```

### Watermark Rules
- Watermark = `max(event_time seen so far)` - `delay threshold`
- Events older than watermark are **silently dropped**
- Required for stateful aggregations with `outputMode("append")`
- Without watermark + aggregation → must use `outputMode("complete")`

---

## 6. Checkpointing

Checkpoints save exactly:
1. **Progress** — which data has been read (offsets)
2. **State** — aggregation state (e.g., running counts)

```python
.option("checkpointLocation", "/mnt/checkpoints/stream_name")
```

> ⚠️ **Never share a checkpoint directory between two streaming queries.** Each query must have its own unique checkpoint location.

---

## 7. Streaming Aggregations (Stateful Operations)

```python
from pyspark.sql.functions import window, sum, count, avg

# Tumbling window: non-overlapping 5-min windows
result = (
    stream_df
    .withWatermark("ts", "10 minutes")
    .groupBy(window("ts", "5 minutes"), "region")
    .agg(
        count("*").alias("event_count"),
        sum("revenue").alias("total_revenue")
    )
)

# Sliding window: 10-min windows, sliding every 5 min
result = (
    stream_df
    .withWatermark("ts", "20 minutes")
    .groupBy(window("ts", "10 minutes", "5 minutes"))
    .count()
)
```

---

## 8. Streaming with Delta Lake (Delta + Stream = ❤️)

Delta Lake is the preferred sink AND source for streaming in Databricks.

### Delta as Source (Change Data Feed)
```python
# Stream changes from a Delta table using CDF
stream = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")  # Requires CDF enabled on table
    .option("startingVersion", 0)
    .table("catalog.schema.my_table")
)
```

### Delta as Sink
```python
query = (
    df.writeStream
    .format("delta")
    .outputMode("append")  # or "complete" for aggregations
    .option("checkpointLocation", "/checkpoints/silver")
    .option("mergeSchema", "true")   # Allow schema evolution
    .table("catalog.schema.silver_events")
)
```

---

## 9. foreachBatch — Custom Sink Logic

Use `foreachBatch` when you need to apply arbitrary logic (e.g., MERGE/upsert) on each micro-batch:

```python
def upsert_to_delta(batch_df, batch_id):
    batch_df.createOrReplaceTempView("updates")
    batch_df._jdf.sparkSession().sql("""
        MERGE INTO silver.transactions t
        USING updates u ON t.id = u.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (
    stream_df.writeStream
    .foreachBatch(upsert_to_delta)
    .option("checkpointLocation", "/checkpoints/upsert")
    .start()
)
```

> 🔑 **Exam tip:** `foreachBatch` is the ONLY way to do MERGE/upsert in a streaming context. You cannot call `.merge()` directly on a streaming DataFrame.

---

## 10. Query Monitoring

```python
# Check status
print(query.status)
print(query.lastProgress)

# List all active streams
spark.streams.active

# Stop a stream
query.stop()

# Wait for termination (blocks)
query.awaitTermination(timeout=60)  # seconds
```

---

## 11. Common Exam Scenarios

### Scenario 1: When should you use `availableNow` vs `processingTime`?
- **availableNow:** Batch-style catch-up job (nightly pipeline that processes all accumulated data)
- **processingTime:** Continuous near-real-time pipeline with SLA requirements

### Scenario 2: What happens if checkpoint is lost?
- Stream restarts from the source beginning (reprocesses all data)
- For Delta source: can set `startingVersion` or `startingTimestamp` to avoid full replay

### Scenario 3: Why is my streaming aggregation failing?
- Likely missing watermark when using `outputMode("append")` with a groupBy

---

## 12. Quick Reference Card

```
READ STREAM:   spark.readStream.format("delta").load(path)
WRITE STREAM:  .writeStream.format("delta").outputMode("append")
                .option("checkpointLocation", path)
                .trigger(availableNow=True).start(table)

TRIGGERS:
  default         → continuous micro-batch
  processingTime  → every N seconds/minutes
  once            → 1 batch (DEPRECATED)
  availableNow    → all backlog, then stop
  continuous      → true streaming (limited ops)

OUTPUT MODES:
  append   → stateless ETL, windowed append
  complete → full aggregation result each batch
  update   → only changed rows

WATERMARK:  .withWatermark("ts", "10 minutes")
  → accept events up to 10 min late
  → required for append mode + aggregation

foreachBatch → only way to MERGE in streaming
```

---

## 📺 Recommended Videos

1. **[Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)** — official reference
2. **[Databricks: Streaming with Delta Lake](https://www.youtube.com/watch?v=hvmGfM1FNlk)** — YouTube (search: "Databricks Structured Streaming Delta Lake tutorial")
3. **[Auto Loader + Streaming](https://docs.databricks.com/en/ingestion/auto-loader/index.html)** — official docs
4. **[Delta + CDF Streaming](https://docs.databricks.com/en/delta/delta-change-data-feed.html)** — official docs

---

## ✅ Day 4 Checklist

- [ ] Explain the 5 trigger types and when to use each
- [ ] Write a streaming query with checkpoint + watermark
- [ ] Explain difference between append/complete/update output modes
- [ ] Implement `foreachBatch` for MERGE upsert pattern
- [ ] Read a Delta table as a stream with CDF enabled
- [ ] Monitor streaming query status
