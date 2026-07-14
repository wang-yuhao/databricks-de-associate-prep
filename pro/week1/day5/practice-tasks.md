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

## Task 1 — Windowing and Watermarks in Streaming

📖 **Context**: The Professional exam tests your understanding of time-based aggregations in streaming. You must know the difference between event time, processing time, tumbling windows, sliding windows, and session windows.

🛠️ **Instructions**:

### Step 1 — Create a streaming source with event timestamps:

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema for streaming data
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True)
])

# Read streaming data
streaming_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/streaming/event_schema")
        .schema(schema)
        .load("/mnt/streaming/events/")
)
```

### Step 2 — Apply tumbling window aggregation:

```python
# Tumbling window: Non-overlapping fixed-size windows
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

# Write to Delta table with Unity Catalog
(
    tumbling_agg.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/mnt/checkpoints/tumbling_events")
        .toTable("main_catalog.streaming_schema.tumbling_events")
)
```

### Step 3 — Apply sliding window aggregation:

```python
# Sliding window: Overlapping windows
sliding_agg = (
    streaming_df
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(
                col("event_timestamp"), 
                "10 minutes",  # window duration
                "5 minutes"    # slide interval
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
        .option("checkpointLocation", "/mnt/checkpoints/sliding_events")
        .toTable("main_catalog.streaming_schema.sliding_events")
)
```

✅ **Expected outcome**: 
- Tumbling windows: Each event belongs to exactly ONE window
- Sliding windows: Each event can belong to MULTIPLE overlapping windows
- Watermark defines how late data can arrive (10 minutes in this case)
- `outputMode("append")` works with watermarks to finalize windows

⚠️ **Exam trap**: 
- Tumbling = `window(col, "5 minutes")` — no slide interval
- Sliding = `window(col, "10 minutes", "5 minutes")` — with slide interval
- Watermark MUST be set before groupBy for late data handling
- Without watermark, state grows indefinitely!
- `outputMode("complete")` NOT supported with watermarks

---

## Task 2 — Stateful Streaming with mapGroupsWithState

📖 **Context**: Advanced streaming scenarios require maintaining state across micro-batches. The exam tests `mapGroupsWithState` and `flatMapGroupsWithState` for custom stateful operations.

🛠️ **Instructions**:

### Step 1 — Define state and event classes:

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from dataclasses import dataclass
from typing import Iterator, Tuple

@dataclass
class UserSession:
    user_id: int
    session_start: int  # timestamp in seconds
    session_end: int
    event_count: int
    total_value: float

@dataclass
class Event:
    user_id: int
    event_type: str
    event_timestamp: int  # timestamp in seconds
    value: float
```

### Step 2 — Implement stateful session aggregation:

```python
def update_session_state(
    user_id: int,
    events: Iterator[Event],
    state: GroupState
) -> Iterator[UserSession]:
    """
    Maintains session state per user with 30-minute timeout.
    """
    
    # Get existing state or initialize
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
    
    # Process new events
    event_list = list(events)
    if len(event_list) > 0:
        for event in event_list:
            # Update session
            if session.session_start == 0:
                session.session_start = event.event_timestamp
            session.session_end = event.event_timestamp
            session.event_count += 1
            session.total_value += event.value
        
        # Update state
        state.update(session)
        state.setTimeoutDuration("30 minutes")
    
    # Check for timeout
    if state.hasTimedOut:
        # Emit final session and remove state
        result = session
        state.remove()
        yield result
    elif state.exists:
        # Emit intermediate session (optional)
        yield session

# Apply stateful transformation
session_stream = (
    streaming_df
        .selectExpr(
            "user_id",
            "event_type",
            "CAST(UNIX_TIMESTAMP(event_timestamp) AS INT) as event_timestamp",
            "value"
        )
        .as[(int, str, int, float)]
        .groupByKey(lambda x: x[0])  # Group by user_id
        .mapGroupsWithState(
            update_session_state,
            GroupStateTimeout.ProcessingTimeTimeout
        )
)
```

### Step 3 — Write stateful stream to Delta:

```python
(
    session_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", "/mnt/checkpoints/user_sessions")
        .toTable("main_catalog.streaming_schema.user_sessions")
)
```

✅ **Expected outcome**: 
- Custom state maintained per user across micro-batches
- Session timeout triggers final aggregation
- State is persisted in checkpoint location
- Can handle complex business logic beyond standard aggregations

⚠️ **Exam trap**: 
- `mapGroupsWithState` requires `update` or `append` output mode
- Must set timeout: `ProcessingTimeTimeout` or `EventTimeTimeout`
- State size grows with number of groups — monitor memory!
- Checkpoint location MUST be specified for state recovery
- State is NOT automatically cleaned without timeout

---

## Task 3 — Stream-Stream Joins with Watermarks

📖 **Context**: Joining two streaming DataFrames requires watermarks on both sides. The exam tests your understanding of join conditions, state management, and late data handling.

🛠️ **Instructions**:

### Step 1 — Create two streaming sources:

```python
# Stream 1: User clicks
clicks_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/streaming/clicks_schema")
        .load("/mnt/streaming/clicks/")
        .select(
            col("user_id").cast("int"),
            col("click_timestamp").cast("timestamp"),
            col("page_id").cast("string")
        )
        .withWatermark("click_timestamp", "10 minutes")
)

# Stream 2: User purchases
purchases_stream = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/streaming/purchases_schema")
        .load("/mnt/streaming/purchases/")
        .select(
            col("user_id").cast("int"),
            col("purchase_timestamp").cast("timestamp"),
            col("amount").cast("decimal(10,2)")
        )
        .withWatermark("purchase_timestamp", "10 minutes")
)
```

### Step 2 — Perform stream-stream inner join:

```python
# Inner join with time constraint
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
        .option("checkpointLocation", "/mnt/checkpoints/click_purchase_join")
        .toTable("main_catalog.streaming_schema.click_purchase_attribution")
)
```

### Step 3 — Perform stream-stream left outer join:

```python
# Left outer join to capture clicks without purchases
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
        .option("checkpointLocation", "/mnt/checkpoints/click_conversion")
        .toTable("main_catalog.streaming_schema.click_conversions")
)
```

✅ **Expected outcome**: 
- Stream-stream joins require watermarks on BOTH sides
- Time constraints prevent unbounded state growth
- Inner join emits only when both events match
- Left outer join emits clicks even without matching purchases

⚠️ **Exam trap**: 
- MUST have watermarks on both streams for stream-stream joins
- MUST have time constraints in join condition to bound state
- Without time constraint, state grows forever
- `outputMode("complete")` NOT supported for stream-stream joins
- Late data outside watermark is dropped from join

---

## Task 4 — Streaming Deduplication and Foreachbatch

📖 **Context**: Real-world streams often have duplicates. The exam tests deduplication strategies using `dropDuplicates()` with watermarks and custom logic with `foreachBatch()`.

🛠️ **Instructions**:

### Step 1 — Deduplicate streaming data:

```python
# Deduplicate based on event_id within watermark window
deduplicated_stream = (
    streaming_df
        .withWatermark("event_timestamp", "1 hour")
        .dropDuplicates(["event_id", "user_id"])
)

(
    deduplicated_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/mnt/checkpoints/deduped_events")
        .toTable("main_catalog.streaming_schema.deduped_events")
)
```

### Step 2 — Use foreachBatch for custom processing:

```python
def process_batch(batch_df, batch_id):
    """
    Custom batch processing with complex logic.
    Can perform multiple writes, call external APIs, etc.
    """
    # Example: Write to multiple tables based on event type
    
    # Filter high-value events
    high_value = batch_df.filter(col("value") > 1000)
    high_value.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("main_catalog.streaming_schema.high_value_events")
    
    # Filter low-value events  
    low_value = batch_df.filter(col("value") <= 1000)
    low_value.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("main_catalog.streaming_schema.low_value_events")
    
    # Optional: Call external API for high-value events
    if high_value.count() > 0:
        # Send notification, update external system, etc.
        pass

# Apply foreachBatch
(
    streaming_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/mnt/checkpoints/custom_batch")
        .start()
)
```

### Step 3 — Implement idempotent writes with MERGE:

```python
from delta.tables import DeltaTable

def upsert_batch(batch_df, batch_id):
    """
    Idempotent upsert using Delta MERGE.
    Prevents duplicate processing if batch is retried.
    """
    # Create or get Delta table
    delta_table = DeltaTable.forName(spark, "main_catalog.streaming_schema.unique_users")
    
    # Perform MERGE for upsert
    (
        delta_table.alias("target")
            .merge(
                batch_df.alias("source"),
                "target.user_id = source.user_id"
            )
            .whenMatchedUpdate(set={
                "last_event_timestamp": col("source.event_timestamp"),
                "event_count": col("target.event_count") + 1,
                "total_value": col("target.total_value") + col("source.value")
            })
            .whenNotMatchedInsert(values={
                "user_id": col("source.user_id"),
                "last_event_timestamp": col("source.event_timestamp"),
                "event_count": lit(1),
                "total_value": col("source.value")
            })
            .execute()
    )

(
    streaming_df.writeStream
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", "/mnt/checkpoints/user_upsert")
        .start()
)
```

✅ **Expected outcome**: 
- `dropDuplicates()` with watermark prevents duplicate events
- `foreachBatch()` enables custom batch-level logic
- MERGE provides idempotent writes for exactly-once semantics
- Can write to multiple sinks in single batch

⚠️ **Exam trap**: 
- `dropDuplicates()` requires watermark to bound state size
- `foreachBatch()` receives batch DataFrame, NOT streaming DF
- Inside `foreachBatch()`, use batch write APIs (not writeStream)
- MERGE ensures idempotency for streaming upserts
- Batch processing breaks exactly-once if not idempotent

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

✅ **Windows and Watermarks:**
- Tumbling: `window(col, "5 minutes")` — non-overlapping
- Sliding: `window(col, "10 minutes", "5 minutes")` — overlapping
- Watermark: `.withWatermark(col, "10 minutes")` before groupBy
- Watermark prevents unbounded state growth

✅ **Stateful Operations:**
- `mapGroupsWithState`: Custom state per group
- Requires timeout: `ProcessingTimeTimeout` or `EventTimeTimeout`
- Must use `update` or `append` output mode
- State persisted in checkpoint location

✅ **Stream-Stream Joins:**
- MUST have watermarks on BOTH streams
- MUST have time constraint to bound state
- Inner join: Only matched events
- Left outer join: All left events + matched right
- `append` output mode only

✅ **Deduplication and Custom Processing:**
- `dropDuplicates()` needs watermark for state management
- `foreachBatch()` enables complex custom logic
- Use MERGE for idempotent upserts
- Multiple writes possible in foreachBatch

✅ **Output Modes:**
- `append`: Only new rows (default, works with watermarks)
- `update`: New and updated rows (stateful operations)
- `complete`: All rows (NOT supported with watermarks/joins)

---

## Next Steps

You've completed Day 5! You now understand Advanced Structured Streaming at a professional level. Tomorrow (Day 6), you'll cover Unity Catalog and Data Governance including namespaces, permissions, external locations, row/column security, and data lineage.
