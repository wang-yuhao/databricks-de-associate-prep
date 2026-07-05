# Day 11: Advanced Streaming with Structured Streaming & Auto Loader

## Schedule
- Morning: Structured Streaming internals, triggers, watermarks
- Afternoon: Auto Loader advanced patterns, schema evolution
- Evening: Stateful operations and practice

---

## 11.1 Structured Streaming Internals

```python
# Streaming query with different triggers
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count

spark = SparkSession.builder.appName("streaming").getOrCreate()

# Read from Delta (streaming source)
df = spark.readStream \
    .format("delta") \
    .table("bronze.events")

# Trigger options:
# - processingTime: micro-batch interval
# - once: run once and stop (deprecated, use availableNow)
# - availableNow: process all available data then stop
# - continuous: millisecond latency (limited operations)

query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", "/checkpoints/events") \
    .table("silver.events")

# availableNow trigger (preferred over once)
query_batch = df.writeStream \
    .trigger(availableNow=True) \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/events_batch") \
    .table("silver.events")
```

---

## 11.2 Output Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| append | Only new rows added to result | Non-aggregation, windowed with watermark |
| complete | Full result table rewritten | Aggregations without watermark |
| update | Only changed rows output | Aggregations |

```python
# Windowed aggregation with watermark
windowed_counts = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("user_id")
    ) \
    .agg(count("*").alias("event_count"))

query = windowed_counts.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/windowed") \
    .table("silver.windowed_counts")
```

---

## 11.3 Auto Loader Advanced

```python
# Auto Loader with schema evolution
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/schema/events") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.schemaHints", "id BIGINT, amount DECIMAL(10,2)") \
    .load("abfss://raw@storage.dfs.core.windows.net/events/")

# Schema evolution modes:
# addNewColumns (default): new columns added, existing schema preserved
# rescue: unexpected columns go to _rescued_data column
# failOnNewColumns: fail if new columns detected
# none: ignore schema changes

# Auto Loader with file notification (event-based, scalable)
df_notify = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.useNotifications", "true") \
    .option("cloudFiles.schemaLocation", "/schema/orders") \
    .load("abfss://raw@storage.dfs.core.windows.net/orders/")
```

---

## 11.4 Stateful Operations

```python
from pyspark.sql.functions import session_window, expr

# Session windows (gap-based grouping)
session_df = df \
    .withWatermark("event_time", "30 minutes") \
    .groupBy(
        session_window(col("event_time"), "5 minutes"),
        col("user_id")
    ) \
    .agg(count("*").alias("events_in_session"))

# Stream-stream joins
orders = spark.readStream.table("bronze.orders")
returns = spark.readStream.table("bronze.returns")

# Both sides need watermarks for stream-stream join
orders_w = orders.withWatermark("order_time", "1 hour")
returns_w = returns.withWatermark("return_time", "1 hour")

joined = orders_w.join(
    returns_w,
    expr("""
        orders.order_id = returns.order_id AND
        returns.return_time BETWEEN orders.order_time AND
        orders.order_time + INTERVAL 24 HOURS
    """),
    "leftOuter"
)
```

---

## 11.5 foreachBatch for Custom Sinks

```python
def upsert_to_delta(batch_df, batch_id):
    """Custom sink: upsert to Delta table"""
    batch_df.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql("""
        MERGE INTO silver.customers AS target
        USING updates AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

query = df.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "/checkpoints/customers") \
    .trigger(processingTime="30 seconds") \
    .start()
```

---

## 11.6 Monitoring Streaming Queries

```python
# Query progress and status
for q in spark.streams.active:
    print(f"Query: {q.name}")
    print(f"Status: {q.status}")
    print(f"Recent progress: {q.recentProgress[-1]}")

# Stop a query
query.stop()

# Wait for termination
query.awaitTermination(timeout=60)

# Check for exceptions
if query.exception():
    print(f"Error: {query.exception()}")
```

---

## Key Exam Points

- `availableNow=True` replaces deprecated `once` trigger
- Watermark enables stateful ops with `append` output mode
- Auto Loader `cloudFiles.schemaEvolutionMode`: addNewColumns (default), rescue, failOnNewColumns
- `foreachBatch` enables any custom logic per micro-batch (MERGE, multi-table writes)
- Stream-stream joins require watermarks on both sides
- Checkpoint location stores progress; never reuse for different queries
- `processingTime` trigger: micro-batch at fixed interval
- `continuous` trigger: ms latency but limited to map operations only

---

## Practice Tasks
- [ ] Write streaming query with 1-minute processingTime trigger
- [ ] Implement windowed count with 10-minute watermark
- [ ] Configure Auto Loader with addNewColumns schema evolution
- [ ] Use foreachBatch to MERGE streaming data into Delta
- [ ] Monitor active streaming queries and check progress
