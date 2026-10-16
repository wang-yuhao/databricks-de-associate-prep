# Day 5 Practice Tasks — Advanced Structured Streaming

> **Exam section:** Data Processing (30%), Incremental Data Processing (20%)
> **Prerequisite:** Read `study-notes.md` and complete Days 3-4 practice tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last. Every task has:

- 📖 **Context** — why this matters for the exam
- 🛠️ **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- ⚠️ **Exam trap** — a common wrong-answer pitfall

---

## Before You Start

This lab is written for Databricks Structured Streaming and Auto Loader. Databricks documents `cloudFiles` as the Auto Loader source for incrementally processing new files as they arrive in cloud storage or Unity Catalog volumes. [web:11][web:14]

### Cluster compatibility note

Some clusters do not support processing-time triggers. If you see:

> `Trigger type ProcessingTime is not supported for this cluster type. Use a different trigger type e.g. AvailableNow, Once.`

then use `trigger(availableNow=True)` for every streaming write in this lab. Spark documents that the default trigger behavior is effectively processing-time based, so this explicit trigger is the safest option on restricted cluster types. [web:18][web:19][web:20]

### Recommended paths

Prefer Unity Catalog volume paths instead of legacy `/mnt/...` paths where possible:

- `/Volumes/main/streaming_lab/raw/events`
- `/Volumes/main/streaming_lab/raw/clicks`
- `/Volumes/main/streaming_lab/raw/purchases`

---

## Task 1 — Windowing and Watermarks in Streaming

📖 **Context**: The Professional exam tests your understanding of time-based aggregations in streaming. You must know the difference between event time, processing time, tumbling windows, sliding windows, and session windows.

🛠️ **Instructions**:

### Step 1 — Create a streaming source with event timestamps

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

streaming_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/streaming_lab/raw/_schemas/events")
        .schema(schema)
        .load("/Volumes/main/streaming_lab/raw/events")
)
```

### Step 2 — Apply tumbling window aggregation

```python
tumbling_agg = (
    streaming_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),
            col("event_type")
        )
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value"),
            avg("value").alias("avg_value")
        )
)

(
    tumbling_agg.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/tumbling_events")
        .trigger(availableNow=True)
        .toTable("main.streaming_lab.tumbling_events")
)
```

### Step 3 — Apply sliding window aggregation

```python
sliding_agg = (
    streaming_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(
                col("event_timestamp"),
                "10 minutes",
                "5 minutes"
            ),
            col("event_type")
        )
        .agg(
            count("*").alias("event_count"),
            max("value").alias("max_value")
        )
)

(
    sliding_agg.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/sliding_events")
        .trigger(availableNow=True)
        .toTable("main.streaming_lab.sliding_events")
)
```

✅ **Expected outcome**:

- Tumbling windows: Each event belongs to exactly ONE window.
- Sliding windows: Each event can belong to MULTIPLE overlapping windows.
- Watermark defines how late data can arrive.
- `outputMode("append")` works with watermarks to finalize windows.

⚠️ **Exam trap**:

- Tumbling = `window(col, "5 minutes")` — no slide interval.
- Sliding = `window(col, "10 minutes", "5 minutes")` — with slide interval.
- Watermark MUST be set before `groupBy`.
- Without watermark, state grows indefinitely.
- `outputMode("complete")` is not the right choice for these watermark-based aggregations.

---

## Task 2 — Stateful Streaming with mapGroupsWithState

📖 **Context**: Advanced streaming scenarios require maintaining state across micro-batches. The exam tests `mapGroupsWithState` and `flatMapGroupsWithState` for custom stateful operations.

🛠️ **Instructions**:

### Step 1 — Define state and event classes

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from dataclasses import dataclass
from typing import Iterator

@dataclass
class UserSession:
    user_id: int
    session_start: int
    session_end: int
    event_count: int
    total_value: float

@dataclass
class Event:
    user_id: int
    event_type: str
    event_timestamp: int
    value: float
```

### Step 2 — Update the implementation for this cluster

If your cluster rejects processing-time triggers, do **not** use `ProcessingTimeTimeout`. Use the following rule instead:

- Base timeout logic on event time.
- Use `EventTimeTimeout`.
- Set a watermark on the event-time column.
- Prefer `trigger(availableNow=True)`.

```python
def update_session_state(user_id, rows, state: GroupState):
    events = list(rows)

    if state.exists:
        session = state.get
    else:
        session = UserSession(
            user_id=user_id,
            session_start=0,
            session_end=0,
            event_count=0,
            total_value=0.0
        )

    if len(events) > 0:
        for row in events:
            if session.session_start == 0:
                session.session_start = int(row.event_timestamp.timestamp())
            session.session_end = int(row.event_timestamp.timestamp())
            session.event_count += 1
            session.total_value += float(row.value)

        state.update(session)
        state.setTimeoutTimestamp(session.session_end + 1800)

        yield session

    if state.hasTimedOut:
        yield session
        state.remove()
```

### Step 3 — Apply stateful transformation

```python
session_stream = (
    streaming_df
        .withWatermark("event_timestamp", "30 minutes")
        .select(
            col("user_id"),
            col("event_type"),
            col("event_timestamp"),
            col("value")
        )
)
```

### Step 4 — Write the result

Use the stateful API pattern that is supported in your notebook runtime. If your runtime does not support the exact Python signature for `mapGroupsWithState`, treat this as a conceptual exercise and focus on the exam rules:

- state must be checkpointed,
- timeout must be defined,
- watermark is required for event-time timeout,
- use event time instead of processing time on this cluster.

✅ **Expected outcome**:

- Custom state maintained per user across micro-batches.
- Session timeout triggers final aggregation.
- State is persisted in checkpoint location.
- Can handle complex business logic beyond standard aggregations.

⚠️ **Exam trap**:

- `ProcessingTimeTimeout` may fail on restricted cluster types.
- `mapGroupsWithState` requires `update` or `append` output mode.
- Checkpoint location MUST be specified.
- State is NOT automatically cleaned without timeout.

---

## Task 3 — Stream-Stream Joins with Watermarks

📖 **Context**: Joining two streaming DataFrames requires watermarks on both sides. The exam tests your understanding of join conditions, state management, and late data handling.

🛠️ **Instructions**:

### Step 1 — Create two streaming sources

```python
clicks_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/streaming_lab/raw/_schemas/clicks")
        .load("/Volumes/main/streaming_lab/raw/clicks")
        .select(
            col("user_id").cast("int"),
            col("click_timestamp").cast("timestamp"),
            col("page_id").cast("string")
        )
        .withWatermark("click_timestamp", "10 minutes")
)

purchases_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/streaming_lab/raw/_schemas/purchases")
        .load("/Volumes/main/streaming_lab/raw/purchases")
        .select(
            col("user_id").cast("int"),
            col("purchase_timestamp").cast("timestamp"),
            col("amount").cast("decimal(10,2)")
        )
        .withWatermark("purchase_timestamp", "10 minutes")
)
```

### Step 2 — Perform stream-stream inner join

```python
joined_stream = (
    clicks_stream.alias("c")
        .join(
            purchases_stream.alias("p"),
            expr("""
                c.user_id = p.user_id AND
                p.purchase_timestamp >= c.click_timestamp AND
                p.purchase_timestamp <= c.click_timestamp + INTERVAL 1 HOUR
            """),
            "inner"
        )
        .select(
            col("c.user_id"),
            col("c.click_timestamp"),
            col("c.page_id"),
            col("p.purchase_timestamp"),
            col("p.amount")
        )
)

(
    joined_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/click_purchase_join")
        .trigger(availableNow=True)
        .toTable("main.streaming_lab.click_purchase_attribution")
)
```

### Step 3 — Perform stream-stream left outer join

```python
left_joined_stream = (
    clicks_stream.alias("c")
        .join(
            purchases_stream.alias("p"),
            expr("""
                c.user_id = p.user_id AND
                p.purchase_timestamp >= c.click_timestamp AND
                p.purchase_timestamp <= c.click_timestamp + INTERVAL 1 HOUR
            """),
            "leftOuter"
        )
        .select(
            col("c.user_id"),
            col("c.click_timestamp"),
            col("c.page_id"),
            col("p.purchase_timestamp"),
            col("p.amount"),
            when(col("p.purchase_timestamp").isNull(), lit(False))
                .otherwise(lit(True))
                .alias("converted")
        )
)

(
    left_joined_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/click_conversion")
        .trigger(availableNow=True)
        .toTable("main.streaming_lab.click_conversions")
)
```

✅ **Expected outcome**:

- Stream-stream joins require watermarks on BOTH streams.
- Time constraints prevent unbounded state growth.
- Inner join emits only when both events match.
- Left outer join emits clicks even without matching purchases.

⚠️ **Exam trap**:

- MUST have watermarks on both streams.
- MUST have time constraints in join condition.
- Without time constraint, state grows forever.
- `outputMode("complete")` is not supported for stream-stream joins.

---

## Task 4 — Streaming Deduplication and foreachBatch

📖 **Context**: Real-world streams often have duplicates. The exam tests deduplication strategies using `dropDuplicates()` with watermarks and custom logic with `foreachBatch()`.

🛠️ **Instructions**:

### Step 1 — Deduplicate streaming data

```python
deduplicated_stream = (
    streaming_df
        .withWatermark("event_timestamp", "1 hour")
        .dropDuplicates(["event_id", "user_id"])
)

(
    deduplicated_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/deduped_events")
        .trigger(availableNow=True)
        .toTable("main.streaming_lab.deduped_events")
)
```

### Step 2 — Use foreachBatch for custom processing

```python
def process_batch(batch_df, batch_id):
    high_value = batch_df.filter(col("value") > 1000)
    low_value = batch_df.filter(col("value") <= 1000)

    high_value.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("main.streaming_lab.high_value_events")

    low_value.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("main.streaming_lab.low_value_events")

(
    streaming_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/custom_batch")
        .trigger(availableNow=True)
        .start()
)
```

### Step 3 — Implement idempotent writes with MERGE

```python
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS main.streaming_lab.unique_users (
  user_id INT,
  last_event_timestamp TIMESTAMP,
  event_count BIGINT,
  total_value DOUBLE
)
USING DELTA
""")

def upsert_batch(batch_df, batch_id):
    agg_df = (
        batch_df.groupBy("user_id")
        .agg(
            max("event_timestamp").alias("last_event_timestamp"),
            count("*").alias("event_count"),
            sum("value").alias("total_value")
        )
    )

    delta_table = DeltaTable.forName(spark, "main.streaming_lab.unique_users")

    (
        delta_table.alias("target")
            .merge(
                agg_df.alias("source"),
                "target.user_id = source.user_id"
            )
            .whenMatchedUpdate(set={
                "last_event_timestamp": col("source.last_event_timestamp"),
                "event_count": col("target.event_count") + col("source.event_count"),
                "total_value": col("target.total_value") + col("source.total_value")
            })
            .whenNotMatchedInsert(values={
                "user_id": col("source.user_id"),
                "last_event_timestamp": col("source.last_event_timestamp"),
                "event_count": col("source.event_count"),
                "total_value": col("source.total_value")
            })
            .execute()
    )

(
    streaming_df.writeStream
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", "/Volumes/main/streaming_lab/raw/_checkpoints/user_upsert")
        .trigger(availableNow=True)
        .start()
)
```

✅ **Expected outcome**:

- `dropDuplicates()` with watermark prevents duplicate events.
- `foreachBatch()` enables custom batch-level logic.
- MERGE provides idempotent writes for retry-safe processing.
- Multiple writes are possible in `foreachBatch`.

⚠️ **Exam trap**:

- `dropDuplicates()` requires watermark to bound state size.
- `foreachBatch()` receives batch DataFrame, not streaming DF.
- Inside `foreachBatch()`, use batch write APIs.
- MERGE ensures idempotency for streaming upserts.

---

## Task 5 — Concept Quiz

Answer these rapid-fire questions:

1. What is the difference between tumbling and sliding windows?
   - A) Tumbling is faster
   - B) Tumbling windows don't overlap, sliding windows do ✓
   - C) Tumbling uses event time, sliding uses processing time
   - D) No difference, just syntax

2. What happens if you don't set a watermark on a streaming aggregation?
   - A) Pipeline fails immediately
   - B) State grows indefinitely and causes OOM ✓
   - C) Late data is automatically handled
   - D) Aggregations are computed incorrectly

3. For stream-stream joins, what is REQUIRED?
   - A) Watermarks on both streams and time constraint in join ✓
   - B) Only watermark on left stream
   - C) Complete output mode
   - D) Spark version 3.5+

4. What is the purpose of `foreachBatch()`?
   - A) Optimize batch size
   - B) Enable custom logic and multiple writes per batch ✓
   - C) Improve checkpoint performance
   - D) Replace watermark functionality

5. How does `dropDuplicates()` prevent unbounded state growth?
   - A) Automatic state cleanup every hour
   - B) Requires watermark to define state retention ✓
   - C) Doesn't need state management
   - D) Uses processing time automatically

---

## Key Takeaways for the Exam

✅ **Windows and Watermarks**
- Tumbling: `window(col, "5 minutes")` — non-overlapping
- Sliding: `window(col, "10 minutes", "5 minutes")` — overlapping
- Watermark: `.withWatermark(col, "10 minutes")` before `groupBy`
- Watermark prevents unbounded state growth

✅ **Stateful Operations**
- `mapGroupsWithState`: Custom state per group
- Prefer event-time timeout on restricted clusters
- Must use `update` or `append` output mode
- State persisted in checkpoint location

✅ **Stream-Stream Joins**
- MUST have watermarks on BOTH streams
- MUST have time constraint to bound state
- Inner join: Only matched events
- Left outer join: All left events + matched right
- `append` output mode only

✅ **Deduplication and Custom Processing**
- `dropDuplicates()` needs watermark for state management
- `foreachBatch()` enables complex custom logic
- Use MERGE for idempotent upserts
- Multiple writes possible in `foreachBatch`

✅ **Output Modes**
- `append`: Only new rows
- `update`: New and updated rows
- `complete`: Not the right choice for these watermark-based tasks

---

## Next Steps

You've completed Day 5. You now understand Advanced Structured Streaming at a professional level.

Tomorrow, continue with Day 6 on Unity Catalog and Data Governance.
