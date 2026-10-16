# Day 4 — Hands-On Practice Tasks

## Setup

All tasks run in **Databricks Community Edition** (free).
- URL: https://community.cloud.databricks.com
- Create a cluster: Runtime 14.x+ LTS (Python 3.10)
- Create a new notebook: Language = Python

---

## Task 1: Basic Streaming Read + Write (30 min)

**Goal:** Read a Delta table as a stream, write to another Delta table.

### Step 1 — Create source data
```python
# In a new notebook cell, create a source Delta table
from pyspark.sql.functions import current_timestamp, lit
import random

# Create sample events
data = [(i, f"user_{i % 10}", random.choice(["click", "view", "purchase"]), i * 1.5)
        for i in range(1, 101)]

df = spark.createDataFrame(data, ["event_id", "user_id", "event_type", "value"])
df = df.withColumn("event_time", current_timestamp())

df.write.format("delta").mode("overwrite").save("/tmp/streaming_lab/source")
print("Source table created with", df.count(), "rows")
```

### Step 2 — Start a streaming query
```python
# Read from source as a stream
source_stream = (
    spark.readStream
    .format("delta")
    .load("/tmp/streaming_lab/source")
)

# Write to sink with AvailableNow trigger
query = (
    source_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/streaming_lab/checkpoints/task1")
    .trigger(availableNow=True)
    .start("/tmp/streaming_lab/sink")
)

query.awaitTermination()
print("Stream finished. Status:", query.status)
```

### Step 3 — Verify output
```python
result = spark.read.format("delta").load("/tmp/streaming_lab/sink")
print("Sink row count:", result.count())
result.show(5)
```

**Expected:** Sink has same 100 rows as source.

---

## Task 2: Streaming Aggregation with Watermark (45 min)

**Goal:** Use windowed aggregation with watermarking.

```python
from pyspark.sql.functions import window, col, count, sum as spark_sum
from pyspark.sql.types import *
import datetime

# Create source with timestamps
rows = []
base_time = datetime.datetime(2024, 1, 1, 10, 0, 0)
for i in range(200):
    ts = base_time + datetime.timedelta(seconds=i * 30)
    rows.append((i, f"user_{i%5}", "click", float(i), ts))

schema = StructType([
    StructField("id", IntegerType()),
    StructField("user_id", StringType()),
    StructField("event", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType())
])

df_with_ts = spark.createDataFrame(rows, schema)
df_with_ts.write.format("delta").mode("overwrite").save("/tmp/streaming_lab/source_ts")
```

```python
# Streaming aggregation with 5-min tumbling window
stream = (
    spark.readStream
    .format("delta")
    .load("/tmp/streaming_lab/source_ts")
)

agg_stream = (
    stream
    .withWatermark("event_time", "10 minutes")  # allow 10min late data
    .groupBy(
        window(col("event_time"), "5 minutes"),  # 5-min windows
        col("user_id")
    )
    .agg(
        count("*").alias("event_count"),
        spark_sum("amount").alias("total_amount")
    )
)

query2 = (
    agg_stream
    .writeStream
    .format("delta")
    .outputMode("complete")   # complete because aggregation
    .option("checkpointLocation", "/tmp/streaming_lab/checkpoints/task2")
    .trigger(availableNow=True)
    .start("/tmp/streaming_lab/agg_sink")
)

query2.awaitTermination()
```

```python
# Check aggregated results
agg_result = spark.read.format("delta").load("/tmp/streaming_lab/agg_sink")
agg_result.orderBy("window", "user_id").show(20, truncate=False)
```

**What you should see:** Rows grouped by 5-minute time windows and user_id with counts and totals.

---

## Task 3: foreachBatch — MERGE/Upsert Pattern (45 min)

**Goal:** Use `foreachBatch` to implement SCD Type 1 upsert.

```python
# Create target Delta table (simulating existing silver table)
from delta.tables import DeltaTable

initial_data = [(1, "Alice", "alice@email.com", 100.0),
                (2, "Bob", "bob@email.com", 200.0),
                (3, "Carol", "carol@email.com", 300.0)]

target_df = spark.createDataFrame(initial_data, ["id", "name", "email", "score"])
target_df.write.format("delta").mode("overwrite").saveAsTable("default.customers_silver")
print("Target table created")
spark.sql("SELECT * FROM default.customers_silver").show()
```

```python
# Create streaming source (updates + new records)
updates = [(2, "Bob", "bob.new@email.com", 250.0),  # update existing
           (4, "Dave", "dave@email.com", 150.0),    # new record
           (5, "Eve", "eve@email.com", 180.0)]      # new record

updates_df = spark.createDataFrame(updates, ["id", "name", "email", "score"])
updates_df.write.format("delta").mode("overwrite").save("/tmp/streaming_lab/updates")
```

```python
# foreachBatch function with MERGE logic
def merge_to_silver(batch_df, batch_id):
    print(f"Processing batch {batch_id} with {batch_df.count()} rows")
    
    # Register batch as temp view
    batch_df.createOrReplaceTempView("incoming_updates")
    
    # Execute MERGE
    batch_df.sparkSession.sql("""
        MERGE INTO default.customers_silver AS target
        USING incoming_updates AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                target.name = source.name,
                target.email = source.email,
                target.score = source.score
        WHEN NOT MATCHED THEN
            INSERT (id, name, email, score)
            VALUES (source.id, source.name, source.email, source.score)
    """)
    print(f"Batch {batch_id} merged successfully")

# Stream the updates through foreachBatch
updates_stream = (
    spark.readStream
    .format("delta")
    .load("/tmp/streaming_lab/updates")
)

upsert_query = (
    updates_stream
    .writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", "/tmp/streaming_lab/checkpoints/task3")
    .trigger(availableNow=True)
    .start()
)

upsert_query.awaitTermination()
```

```python
# Verify: Bob's email updated, Dave and Eve added
print("Final state of customers_silver:")
spark.sql("SELECT * FROM default.customers_silver ORDER BY id").show()
# Expected: 5 rows, Bob has new email, Dave and Eve present
```

---

## Task 4: Monitor & Control Streams (15 min)

```python
# Start a continuous stream (processingTime trigger)
continuous_stream = (
    spark.readStream
    .format("delta")
    .load("/tmp/streaming_lab/source")
    .writeStream
    .format("memory")           # memory sink for demo
    .queryName("demo_stream")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

import time
time.sleep(10)  # let it run for 10 seconds

# Check status
print("Is active:", continuous_stream.isActive)
print("Status:", continuous_stream.status)
print("Last progress:", continuous_stream.lastProgress)

# List all active streams
print("Active streams:", [s.name for s in spark.streams.active])

# Query the in-memory sink
spark.sql("SELECT COUNT(*) as rows FROM demo_stream").show()

# Stop the stream
continuous_stream.stop()
print("Stream stopped")
```

---

## ✅ Task Completion Checklist

- [ ] Task 1: Basic stream read/write with AvailableNow trigger runs successfully
- [ ] Task 2: Windowed aggregation with watermark produces correct per-window counts
- [ ] Task 3: foreachBatch MERGE updates existing rows and inserts new ones
- [ ] Task 4: Can start, monitor, query, and stop a continuous stream

---

## 🧠 Self-Test Questions

1. What is the difference between `trigger(once=True)` and `trigger(availableNow=True)`?
2. Why can't you use `outputMode("append")` with an aggregation that has no watermark?
3. What does `foreachBatch` enable that a regular `.writeStream` sink cannot do?
4. You need a streaming pipeline that processes data every 2 minutes. Which trigger do you use?
5. What happens to late data that arrives after the watermark threshold?

**Answers:**
1. `once` processes 1 micro-batch; `availableNow` processes ALL available data in multiple micro-batches, then stops
2. Without watermark, Spark can't know when a window is complete → can't use append (data could change); must use `complete`
3. `foreachBatch` allows you to call `.merge()`, `.save()` with custom logic not possible in a regular streaming write
4. `.trigger(processingTime="2 minutes")`
5. Data older than watermark is silently dropped / ignored
