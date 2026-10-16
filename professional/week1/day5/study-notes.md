# Day 5 — Advanced Structured Streaming (~10% exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day5_advanced_streaming.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review streaming scenario questions

---

## 5.1 Stateful Streaming Operations

Stateful operations maintain state across micro-batches (require checkpointing).

### Window Aggregations
```python
from pyspark.sql.functions import window, col, count, sum

# Tumbling window: non-overlapping, fixed size
windowed = (
    events_stream
    .groupBy(
        window(col("event_timestamp"), "10 minutes"),  # 10-min tumbling window
        col("region")
    )
    .agg(
        count("*").alias("event_count"),
        sum("amount").alias("total_amount")
    )
)

# Sliding window: overlapping
sliding = (
    events_stream
    .groupBy(
        window(col("event_timestamp"), "10 minutes", "5 minutes"),  # 10-min window, slide every 5 min
        col("region")
    )
    .agg(count("*").alias("event_count"))
)

# Session window: closes after inactivity gap
from pyspark.sql.functions import session_window
session = (
    events_stream
    .groupBy(
        session_window(col("event_timestamp"), "5 minutes"),  # 5-min inactivity gap
        col("user_id")
    )
    .agg(count("*").alias("events_in_session"))
)
```

---

## 5.2 Watermarks (Late Data Handling)

Watermarks tell Spark how long to wait for late data before dropping state.

```python
from pyspark.sql.functions import window, col

# Watermark + window aggregation
result = (
    events_stream
    .withWatermark("event_timestamp", "10 minutes")  # wait up to 10 mins for late data
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("region")
    )
    .agg(count("*").alias("event_count"))
)
```

### Watermark Rules
- State for a window is kept until: `max_event_time - watermark_delay`
- Late data arriving AFTER the watermark threshold is **dropped**
- Without watermark: state grows unboundedly
- Required for: Update mode and Complete mode with aggregations

---

## 5.3 Output Modes

| Mode | Description | Aggregation? | Requires Watermark? |
|------|-------------|-------------|--------------------|
| **Append** | Only new rows added since last trigger | No (or with watermark+window) | Yes (for windows) |
| **Update** | Only rows that changed since last trigger | Yes | Recommended |
| **Complete** | All rows in result table on every trigger | Yes | No (but state grows) |

```python
# Append mode: for non-aggregation or watermarked aggregations
query = (
    windowed_df
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", "/Volumes/training/prep/landing/checkpoints/streaming")
    .start()
)

# Update mode: for aggregations
query = (
    windowed_df
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/Volumes/training/prep/landing/checkpoints/streaming")
    .start()
)
```

---

## 5.4 Triggers

```python
# Default: micro-batch as fast as possible
.trigger()  

# Fixed interval
.trigger(processingTime="30 seconds")

# One-time: process all available data, then stop (deprecated, use availableNow)
.trigger(once=True)

# Available Now: like once, but with multiple micro-batches (better for large backlogs)
.trigger(availableNow=True)

# Continuous processing: low latency, limited operators
.trigger(continuous="1 second")
```

### Trigger Comparison
| Trigger | Latency | Use Case |
|---------|---------|----------|
| Default micro-batch | Seconds | General streaming |
| `processingTime="30s"` | Fixed interval | Controlled throughput |
| `availableNow=True` | N/A (batch-like) | Scheduled incremental loads |
| `continuous` | Milliseconds | Ultra-low latency (limited ops) |

---

## 5.5 Stream-Static Joins

```python
# Join streaming data with static lookup table
static_customers = spark.table("training.prep.customers")  # static DataFrame

enriched_stream = (
    orders_stream
    .join(
        static_customers,
        orders_stream["customer_id"] == static_customers["customer_id"],
        "left"
    )
    .select(
        orders_stream["order_id"],
        orders_stream["amount"],
        static_customers["customer_name"],
        static_customers["region"]
    )
)
```

### Stream-Stream Joins
```python
# Both sides must have watermarks for stream-stream joins
orders_with_wm = orders_stream.withWatermark("order_time", "10 minutes")
payments_with_wm = payments_stream.withWatermark("payment_time", "20 minutes")

joined = (
    orders_with_wm.join(
        payments_with_wm,
        expr("""
            order_id = payment_order_id AND
            payment_time BETWEEN order_time AND order_time + INTERVAL 30 MINUTES
        """)
    )
)
```

---

## 5.6 Monitoring Streaming Queries

```python
# Get query progress
query = df.writeStream.format("delta").start()

# Check status
print(query.status)    # {"message": "Processing new data", ...}
print(query.lastProgress)  # dict with metrics

# Key metrics in lastProgress
# - numInputRows: rows processed in last batch
# - inputRowsPerSecond: throughput
# - processedRowsPerSecond: processing rate
# - batchId: current batch number
# - durationMs: time for each phase

# List all active streams
for q in spark.streams.active:
    print(q.name, q.status)

# Await termination
query.awaitTermination(timeout=60)  # timeout in seconds
```

### SQL Alerts for Streaming Lags
```sql
-- Create an alert in Databricks SQL
-- Query: SELECT COUNT(*) FROM streaming_table WHERE processed_at < NOW() - INTERVAL 5 MINUTES
-- Alert when count > 0 (backlog detected)
```

---

## Key Exam Points ✔️

- **Watermark** = max event time - delay; state for windows is dropped after watermark passes
- **Append mode** only outputs new rows; requires watermark for windowed aggregations
- **Update mode** outputs only changed rows; good for aggregations without state size concerns
- **Complete mode** outputs all rows on every trigger; state grows without bound (no watermark needed but dangerous)
- `trigger(availableNow=True)` is the modern replacement for `trigger(once=True)`
- Stream-stream joins require watermarks on **both** sides
- `query.lastProgress` contains batch metrics (inputRows, processedRows, durationMs)
- Continuous trigger supports only map/filter operations, no aggregations
- Session windows close after a period of inactivity (not a fixed size)
