# Day 11: Advanced Streaming with Structured Streaming & Auto Loader

**Exam Sections**: 
- Section 2: Data Pipelines (40%)
- Section 4: Infrastructure & CI/CD (20%)

**Prerequisites**: Spark Structured Streaming basics, Delta Lake fundamentals

**Time Estimate**: 3-4 hours

**Difficulty**: Professional Level ⭐⭐⭐⭐

---

## Task 1: Implement Different Trigger Types

📖 **Context**: 
The Professional exam tests your understanding of streaming triggers and their impact on performance and latency. Questions often compare `processingTime`, `availableNow`, and `continuous` triggers.

🛠️ **Instructions**:

1. Create a streaming source with Delta:

```python
# Create sample streaming data
from pyspark.sql.functions import current_timestamp, lit, expr
import time

# Write initial batch
spark.range(0, 100) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("value", expr("id * 2")) \
    .write.format("delta").mode("overwrite").save("/tmp/streaming_source")

# Simulate continuous data arrival
for i in range(1, 6):
    time.sleep(5)
    spark.range(i*100, (i+1)*100) \
        .withColumn("timestamp", current_timestamp()) \
        .withColumn("value", expr("id * 2")) \
        .write.format("delta").mode("append").save("/tmp/streaming_source")
```

2. Implement processing with different triggers:

```python
from pyspark.sql.functions import window, avg

# Read streaming source
stream_df = spark.readStream \
    .format("delta") \
    .load("/tmp/streaming_source")

# Trigger 1: ProcessingTime - micro-batch every 30 seconds
query1 = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_processing_time") \
    .trigger(processingTime="30 seconds") \
    .start("/tmp/output_processing_time")

# Trigger 2: AvailableNow - process all available data then stop
query2 = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_available_now") \
    .trigger(availableNow=True) \
    .start("/tmp/output_available_now")

# Trigger 3: Continuous - experimental low-latency mode
query3 = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_continuous") \
    .trigger(continuous="1 second") \
    .start("/tmp/output_continuous")

# Monitor queries
display(spark.streams.active)

# Wait for availableNow to complete
query2.awaitTermination()
```

3. Compare checkpoint directories:

```python
# Examine checkpoint structure
dbutils.fs.ls("/tmp/checkpoint_processing_time")
# Shows: commits/, offsets/, metadata, sources/

# Check latest processed offset
spark.read.text("/tmp/checkpoint_processing_time/offsets/0").show(truncate=False)
```

✅ **Expected Outcome**:
- Three streaming queries with different trigger behaviors
- `availableNow` processes all data and stops automatically
- `processingTime` runs continuously with 30-second batches
- `continuous` mode achieves millisecond latency (if supported by source/sink)

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Use `trigger(once=True)` for batch processing"
  - **Why it's wrong**: `once=True` is deprecated; use `availableNow=True` instead
  - **Correct**: "availableNow provides better handling of watermarks and is the recommended approach"

---

## Task 2: Implement Event-Time Watermarking

📖 **Context**: 
Watermarking handles late-arriving data in streaming applications. The exam tests how watermarks interact with aggregations and state management.

🛠️ **Instructions**:

1. Create event data with late arrivals:

```python
from pyspark.sql.functions import to_timestamp, expr
from datetime import datetime, timedelta

# Generate timestamped events (some late-arriving)
events = []
base_time = datetime(2024, 1, 1, 10, 0, 0)

for i in range(100):
    # 80% on-time, 20% late by 5-15 minutes
    if i % 5 == 0:
        delay = timedelta(minutes=random.randint(5, 15))
    else:
        delay = timedelta(minutes=0)
    
    event_time = base_time + timedelta(minutes=i) - delay
    events.append((i, event_time.strftime("%Y-%m-%d %H:%M:%S"), f"sensor_{i%10}"))

df = spark.createDataFrame(events, ["event_id", "event_time", "sensor_id"])
df.write.format("delta").mode("overwrite").save("/tmp/events_source")
```

2. Apply watermarking with window aggregations:

```python
from pyspark.sql.functions import window, count, col

# Read stream with event time
stream = spark.readStream \
    .format("delta") \
    .load("/tmp/events_source") \
    .withColumn("event_time", to_timestamp("event_time"))

# Apply watermark - 10 minute tolerance for late data
watermarked_stream = stream \
    .withWatermark("event_time", "10 minutes")

# Window aggregation with watermark
windowed_counts = watermarked_stream \
    .groupBy(
        window("event_time", "5 minutes", "5 minutes"),
        "sensor_id"
    ) \
    .agg(count("*").alias("event_count")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "sensor_id",
        "event_count"
    )

# Write with complete output mode (requires watermark for state cleanup)
query = windowed_counts.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint_watermark") \
    .trigger(processingTime="30 seconds") \
    .start("/tmp/output_watermarked")

query.awaitTermination(60)
```

3. Verify watermark behavior:

```python
# Check state store size (should remain bounded due to watermark)
spark.sql("""
    SELECT 
        window_start,
        window_end,
        sensor_id,
        event_count
    FROM delta.`/tmp/output_watermarked`
    ORDER BY window_start DESC
    LIMIT 20
""").display()

# Examine query progress
query.lastProgress
# Look for: "watermark" field showing current watermark value
```

✅ **Expected Outcome**:
- Events arriving within 10-minute watermark are included in aggregations
- Late events beyond watermark are dropped
- State store size remains bounded (doesn't grow indefinitely)
- Output shows windowed counts per sensor

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Watermarks work with all output modes"
  - **Why it's wrong**: Watermarks only work with `append` and `update` modes, not `complete` mode (unless using aggregations)
  - **Correct**: "For stateful aggregations with watermarks, use `append` or `update` mode to enable state cleanup"

---

## Task 3: Auto Loader with Schema Evolution

📖 **Context**: 
Auto Loader's schema inference and evolution features are heavily tested. Understanding `cloudFiles.schemaEvolutionMode` options is critical.

🛠️ **Instructions**:

1. Set up source data with evolving schema:

```python
# Initial schema
initial_data = [
    (1, "user1", 25, "2024-01-01"),
    (2, "user2", 30, "2024-01-01")
]
spark.createDataFrame(initial_data, ["id", "name", "age", "date"]) \
    .write.format("json").mode("overwrite").save("/tmp/autoloader_source/batch1")

# Evolved schema - added new column "email"
evolved_data = [
    (3, "user3", 28, "2024-01-02", "user3@example.com"),
    (4, "user4", 35, "2024-01-02", "user4@example.com")
]
spark.createDataFrame(evolved_data, ["id", "name", "age", "date", "email"]) \
    .write.format("json").mode("overwrite").save("/tmp/autoloader_source/batch2")
```

2. Configure Auto Loader with schema evolution:

```python
# Option 1: addNewColumns - automatically add new columns
auto_loader_stream = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/autoloader_schema") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .load("/tmp/autoloader_source")

query = auto_loader_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_autoloader") \
    .option("mergeSchema", "true") \
    .trigger(availableNow=True) \
    .start("/tmp/output_autoloader")

query.awaitTermination()

# Verify schema evolution
spark.read.format("delta").load("/tmp/output_autoloader").printSchema()
# Should show: id, name, age, date, email (with email nullable)
```

3. Compare schema evolution modes:

```python
# Mode 2: rescue - store unexpected columns in _rescued_data
auto_loader_rescue = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/autoloader_schema_rescue") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .load("/tmp/autoloader_source")

# Mode 3: failOnNewColumns - fail if schema changes
auto_loader_fail = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/autoloader_schema_fail") \
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns") \
    .load("/tmp/autoloader_source")
# This will fail when processing batch2 with new "email" column
```

4. Implement schema hints:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Provide schema hints for better type inference
schema_hints = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date", StringType(), True)
])

auto_loader_hints = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/autoloader_schema_hints") \
    .option("cloudFiles.schemaHints", "email STRING") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .schema(schema_hints) \
    .load("/tmp/autoloader_source")
```

✅ **Expected Outcome**:
- `addNewColumns` mode automatically adds "email" column
- Schema location stores inferred schema metadata
- Different modes handle schema changes differently
- Schema hints provide type guidance for new columns

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Auto Loader works only with S3"
  - **Why it's wrong**: Auto Loader supports multiple cloud storage systems
  - **Correct**: "Auto Loader works with S3, Azure Blob/ADLS, Google Cloud Storage, and even DBFS for testing"

---

## Task 4: Stateful Stream Processing with mapGroupsWithState

📖 **Context**: 
The Professional exam tests arbitrary stateful operations beyond simple aggregations. `mapGroupsWithState` and `flatMapGroupsWithState` are advanced topics.

🛠️ **Instructions**:

1. Create session tracking data:

```python
from pyspark.sql.functions import expr, current_timestamp
from datetime import datetime, timedelta

# User activity events
activities = []
base_time = datetime(2024, 1, 1, 10, 0, 0)

for user in range(1, 6):
    for event in range(10):
        timestamp = base_time + timedelta(minutes=user*10 + event*2)
        activities.append((
            f"user_{user}",
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            f"action_{event % 3}"
        ))

spark.createDataFrame(activities, ["user_id", "timestamp", "action"]) \
    .write.format("delta").mode("overwrite").save("/tmp/user_activities")
```

2. Implement stateful session tracking:

```python
from pyspark.sql import Row
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from typing import Tuple, Iterator
from datetime import datetime

# Define state structure
class SessionState:
    def __init__(self, start_time: datetime, last_event: datetime, event_count: int):
        self.start_time = start_time
        self.last_event = last_event
        self.event_count = event_count

# Define stateful function
def update_session_state(
    key: str,
    events: Iterator[Row],
    state: GroupState
) -> Iterator[Tuple[str, datetime, datetime, int, str]]:
    
    # Get or create state
    if state.exists:
        session = state.get
    else:
        session = SessionState(None, None, 0)
    
    # Process events
    for event in events:
        event_time = datetime.strptime(event.timestamp, "%Y-%m-%d %H:%M:%S")
        
        if session.start_time is None:
            session.start_time = event_time
        
        session.last_event = event_time
        session.event_count += 1
    
    # Session timeout: 5 minutes of inactivity
    if session.last_event:
        timeout_threshold = session.last_event + timedelta(minutes=5)
        
        if datetime.now() > timeout_threshold:
            # Session ended
            result = (
                key,
                session.start_time,
                session.last_event,
                session.event_count,
                "ENDED"
            )
            state.remove()
        else:
            # Session ongoing
            result = (
                key,
                session.start_time,
                session.last_event,
                session.event_count,
                "ACTIVE"
            )
            state.update(session)
            state.setTimeoutDuration(300000)  # 5 minutes in ms
    
    yield result

# Apply stateful transformation
stream = spark.readStream \
    .format("delta") \
    .load("/tmp/user_activities") \
    .withColumn("timestamp", col("timestamp"))

from pyspark.sql.streaming import GroupStateTimeout

sessions = stream \
    .groupBy("user_id") \
    .applyInPandasWithState(
        update_session_state,
        outputStructType=StructType([
            StructField("user_id", StringType()),
            StructField("session_start", TimestampType()),
            StructField("last_event", TimestampType()),
            StructField("event_count", IntegerType()),
            StructField("status", StringType())
        ]),
        stateStructType=StructType([
            StructField("start_time", TimestampType()),
            StructField("last_event", TimestampType()),
            StructField("event_count", IntegerType())
        ]),
        outputMode="update",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )
```

3. Simpler approach with structured aggregations:

```python
# Alternative: Use window + watermark for sessionization
from pyspark.sql.functions import session_window

stream_with_timestamp = spark.readStream \
    .format("delta") \
    .load("/tmp/user_activities") \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withWatermark("timestamp", "10 minutes")

# Session window: group events with <= 5 minute gaps
sessions_simple = stream_with_timestamp \
    .groupBy(
        "user_id",
        session_window("timestamp", "5 minutes")
    ) \
    .agg(
        count("*").alias("event_count"),
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end")
    )

query = sessions_simple.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_sessions") \
    .trigger(processingTime="30 seconds") \
    .start("/tmp/output_sessions")
```

✅ **Expected Outcome**:
- User sessions tracked with state management
- Sessions timeout after 5 minutes of inactivity
- State persists across micro-batches
- Session window provides simpler alternative for many use cases

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Use `groupBy().agg()` for all stateful operations"
  - **Why it's wrong**: Some use cases require custom state logic beyond simple aggregations
  - **Correct**: "Use `mapGroupsWithState` for custom state management, session windows for session-based aggregations, and standard aggregations for simple stateful operations"

---

## Task 5: Streaming Deduplication

📖 **Context**: 
Exam questions test deduplication in streaming with various approaches: watermarks, `dropDuplicates`, and merge operations.

🛠️ **Instructions**:

1. Create data with duplicates:

```python
# Generate events with intentional duplicates
events_with_dupes = []
base_time = datetime(2024, 1, 1, 10, 0, 0)

for i in range(50):
    timestamp = base_time + timedelta(minutes=i)
    event_id = i // 2  # Creates duplicates
    events_with_dupes.append((
        event_id,
        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        f"data_{i}"
    ))

spark.createDataFrame(events_with_dupes, ["event_id", "timestamp", "data"]) \
    .write.format("delta").mode("overwrite").save("/tmp/events_with_dupes")
```

2. Streaming deduplication with watermark:

```python
from pyspark.sql.functions import to_timestamp, col

stream = spark.readStream \
    .format("delta") \
    .load("/tmp/events_with_dupes") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Deduplicate with watermark (bounded state)
deduped_stream = stream \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id"]) \
    .select("event_id", "timestamp", "data")

query = deduped_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_dedup") \
    .trigger(availableNow=True) \
    .start("/tmp/output_deduped")

query.awaitTermination()

# Verify deduplication
result = spark.read.format("delta").load("/tmp/output_deduped")
print(f"Original count: {spark.read.format('delta').load('/tmp/events_with_dupes').count()}")
print(f"Deduped count: {result.count()}")
```

3. Compare with stateless deduplication:

```python
# WARNING: This keeps all keys in state forever (unbounded)
stream_no_watermark = spark.readStream \
    .format("delta") \
    .load("/tmp/events_with_dupes") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Dangerous: no watermark = unbounded state growth
deduped_no_watermark = stream_no_watermark \
    .dropDuplicates(["event_id"])

# This will cause state to grow indefinitely
```

4. Deduplication with merge (for complex scenarios):

```python
from delta.tables import DeltaTable

# Read stream
incoming_stream = spark.readStream \
    .format("delta") \
    .load("/tmp/events_with_dupes") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Use foreachBatch for merge-based deduplication
def merge_dedup(batch_df, batch_id):
    if DeltaTable.isDeltaTable(spark, "/tmp/output_merge_dedup"):
        target_table = DeltaTable.forPath(spark, "/tmp/output_merge_dedup")
        
        # Merge: insert new, ignore duplicates
        target_table.alias("target").merge(
            batch_df.alias("source"),
            "target.event_id = source.event_id"
        ).whenNotMatchedInsertAll().execute()
    else:
        batch_df.write.format("delta").mode("overwrite").save("/tmp/output_merge_dedup")

query_merge = incoming_stream.writeStream \
    .foreachBatch(merge_dedup) \
    .option("checkpointLocation", "/tmp/checkpoint_merge_dedup") \
    .trigger(availableNow=True) \
    .start()

query_merge.awaitTermination()
```

✅ **Expected Outcome**:
- Watermark-based deduplication maintains bounded state
- State size doesn't grow indefinitely with watermarking
- Merge approach provides idempotent writes
- Different strategies for different use cases

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Always use dropDuplicates() for streaming deduplication"
  - **Why it's wrong**: Without watermark, state grows unbounded; not suitable for long-running streams
  - **Correct**: "Use dropDuplicates() with watermark for bounded state, or merge operations for complex deduplication logic"

---

## Task 6: Streaming Joins

📖 **Context**: 
Streaming joins have specific constraints and performance implications. The exam tests stream-stream joins, stream-static joins, and watermark requirements.

🛠️ **Instructions**:

1. Create two streaming sources:

```python
# Stream 1: User clicks
clicks = []
base_time = datetime(2024, 1, 1, 10, 0, 0)
for i in range(100):
    timestamp = base_time + timedelta(minutes=i)
    clicks.append((
        f"user_{i % 10}",
        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        f"page_{i % 5}"
    ))

spark.createDataFrame(clicks, ["user_id", "click_time", "page"]) \
    .write.format("delta").mode("overwrite").save("/tmp/clicks_stream")

# Stream 2: User purchases (20% of users)
purchases = []
for i in range(20):
    timestamp = base_time + timedelta(minutes=i*5 + 2)
    purchases.append((
        f"user_{i % 10}",
        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        f"product_{i % 3}",
        round(random.uniform(10, 100), 2)
    ))

spark.createDataFrame(purchases, ["user_id", "purchase_time", "product", "amount"]) \
    .write.format("delta").mode("overwrite").save("/tmp/purchases_stream")
```

2. Stream-stream join with watermarks:

```python
from pyspark.sql.functions import to_timestamp, expr

# Read both streams
clicks_stream = spark.readStream \
    .format("delta") \
    .load("/tmp/clicks_stream") \
    .withColumn("click_time", to_timestamp("click_time")) \
    .withWatermark("click_time", "10 minutes")

purchases_stream = spark.readStream \
    .format("delta") \
    .load("/tmp/purchases_stream") \
    .withColumn("purchase_time", to_timestamp("purchase_time")) \
    .withWatermark("purchase_time", "10 minutes")

# Join with time constraints (clicks within 5 minutes before purchase)
joined_stream = clicks_stream.join(
    purchases_stream,
    expr("""
        user_id = user_id AND
        click_time <= purchase_time AND
        click_time >= purchase_time - INTERVAL 5 MINUTES
    """),
    "inner"
)

query = joined_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_stream_join") \
    .trigger(processingTime="30 seconds") \
    .start("/tmp/output_stream_join")
```

3. Stream-static join:

```python
# Create static dimension table
users_dim = spark.createDataFrame([
    (f"user_{i}", f"User {i}", f"Segment {i % 3}")
    for i in range(10)
], ["user_id", "user_name", "segment"])

users_dim.write.format("delta").mode("overwrite").save("/tmp/users_dim")

# Join stream with static table (no watermark needed)
enriched_clicks = clicks_stream.join(
    spark.read.format("delta").load("/tmp/users_dim"),
    "user_id",
    "left"
)

query_static = enriched_clicks.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_static_join") \
    .trigger(availableNow=True) \
    .start("/tmp/output_static_join")
```

4. Monitor join performance:

```python
# Check state store size
query.lastProgress['stateOperators']
# Look for: numRowsTotal, memoryUsedBytes

# Analyze join patterns
spark.read.format("delta").load("/tmp/output_stream_join") \
    .groupBy("page", "product") \
    .count() \
    .orderBy("count", ascending=False) \
    .display()
```

✅ **Expected Outcome**:
- Stream-stream join produces matched click-to-purchase events
- Time bounds prevent unbounded state growth
- Stream-static join enriches with dimension data
- Join state size remains manageable with watermarks

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Stream-stream joins without watermarks are fine"
  - **Why it's wrong**: State grows unbounded, leading to memory issues
  - **Correct**: "Both sides of a stream-stream join require watermarks to enable state cleanup and prevent unbounded state growth"

---

## Task 7: Monitoring Streaming Queries

📖 **Context**: 
The exam tests your ability to monitor, debug, and optimize streaming queries using query progress, metrics, and Spark UI.

🛠️ **Instructions**:

1. Create a monitorable streaming query:

```python
# Set up monitoring
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

stream = spark.readStream \
    .format("delta") \
    .load("/tmp/streaming_source") \
    .withColumn("processing_time", current_timestamp())

query = stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_monitoring") \
    .trigger(processingTime="10 seconds") \
    .start("/tmp/output_monitoring")
```

2. Extract query metrics:

```python
# Get latest batch progress
progress = query.lastProgress

if progress:
    print(f"Batch ID: {progress['batchId']}")
    print(f"Input Rows: {progress['numInputRows']}")
    print(f"Processing Time: {progress['durationMs']['triggerExecution']}ms")
    print(f"Input Rate: {progress['inputRowsPerSecond']}")
    print(f"Process Rate: {progress['processedRowsPerSecond']}")
    
    # State store metrics
    if 'stateOperators' in progress:
        for state_op in progress['stateOperators']:
            print(f"State Rows: {state_op['numRowsTotal']}")
            print(f"State Memory: {state_op['memoryUsedBytes']} bytes")

# Continuous monitoring
import time
for i in range(10):
    time.sleep(15)
    progress = query.lastProgress
    if progress:
        print(f"Batch {progress['batchId']}: " +
              f"{progress['numInputRows']} rows in " +
              f"{progress['durationMs']['triggerExecution']}ms")
```

3. Check query status and health:

```python
# Query status
print(f"Query ID: {query.id}")
print(f"Run ID: {query.runId}")
print(f"Status: {query.status}")
print(f"Is Active: {query.isActive}")

# Get recent progress
recent_progress = query.recentProgress
print(f"Recent batches: {len(recent_progress)}")

# Exception handling
try:
    query.awaitTermination(60)
except Exception as e:
    print(f"Query failed: {e}")
    query.stop()
```

4. Analyze checkpoint data:

```python
# List checkpoint contents
checkpoint_files = dbutils.fs.ls("/tmp/checkpoint_monitoring")
for file in checkpoint_files:
    print(f"{file.name}: {file.size} bytes")

# Read commit log
commits = spark.read.text("/tmp/checkpoint_monitoring/commits")
commits.show(truncate=False)

# Read offset log
offsets = spark.read.text("/tmp/checkpoint_monitoring/offsets")
offsets.show(truncate=False)
```

5. Set up custom metrics:

```python
from pyspark.sql.functions import col, count, when

# Add observability columns
monitored_stream = stream \
    .withColumn("is_valid", when(col("value").isNotNull(), 1).otherwise(0)) \
    .withColumn("batch_time", current_timestamp())

# Aggregate metrics per batch
def process_with_metrics(batch_df, batch_id):
    metrics = batch_df.agg(
        count("*").alias("total_records"),
        sum("is_valid").alias("valid_records"),
        current_timestamp().alias("batch_timestamp")
    ).collect()[0]
    
    print(f"Batch {batch_id}: {metrics['total_records']} total, " +
          f"{metrics['valid_records']} valid")
    
    # Write to target
    batch_df.write.format("delta").mode("append").save("/tmp/output_with_metrics")

query_metrics = stream.writeStream \
    .foreachBatch(process_with_metrics) \
    .option("checkpointLocation", "/tmp/checkpoint_with_metrics") \
    .trigger(processingTime="10 seconds") \
    .start()
```

✅ **Expected Outcome**:
- Real-time visibility into streaming query performance
- Identification of bottlenecks and slow batches
- State growth monitoring
- Checkpoint structure understanding

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Query metrics are only available after query completion"
  - **Why it's wrong**: Metrics are available during query execution via `lastProgress` and `recentProgress`
  - **Correct**: "Use `query.lastProgress` for the most recent batch metrics and `query.recentProgress` for historical metrics during active streaming"

---

## Task 8: Advanced Auto Loader Patterns

📖 **Context**: 
The exam tests advanced Auto Loader features like schema hints, notification modes, and file metadata handling.

🛠️ **Instructions**:

1. Auto Loader with file metadata:

```python
# Include file metadata in output
auto_loader = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/schema_with_metadata") \
    .option("cloudFiles.includeExistingFiles", "true") \
    .option("cloudFiles.useNotifications", "false") \
    .load("/tmp/autoloader_source") \
    .withColumn("source_file", input_file_name()) \
    .withColumn("processing_time", current_timestamp())

query = auto_loader.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_metadata") \
    .trigger(availableNow=True) \
    .start("/tmp/output_with_metadata")
```

2. Configure notification mode (for cloud storage):

```python
# File notification mode (AWS S3 + SNS/SQS)
auto_loader_notifications = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/schema_notifications") \
    .option("cloudFiles.useNotifications", "true") \
    .option("cloudFiles.queueUrl", "https://sqs.us-west-2.amazonaws.com/123456/my-queue") \
    .option("cloudFiles.region", "us-west-2") \
    .load("s3://my-bucket/data/")

# Directory listing mode (default, for small datasets)
auto_loader_listing = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/schema_listing") \
    .option("cloudFiles.useNotifications", "false") \
    .option("cloudFiles.maxFilesPerTrigger", "1000") \
    .load("/tmp/autoloader_source")
```

3. Partition management with Auto Loader:

```python
# Read partitioned data efficiently
auto_loader_partitioned = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/tmp/schema_partitioned") \
    .option("pathGlobFilter", "*.parquet") \
    .option("recursiveFileLookup", "true") \
    .option("cloudFiles.partitionColumns", "year,month,day") \
    .load("/tmp/partitioned_source")

# Apply partition filtering
filtered = auto_loader_partitioned \
    .filter("year = 2024 AND month = 1")
```

4. Handle corrupt records:

```python
# Configure corrupt record handling
auto_loader_rescue = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/tmp/schema_rescue") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("mode", "PERMISSIVE") \
    .load("/tmp/autoloader_source")

# Rescue column contains unparseable data
rescued_data = auto_loader_rescue \
    .filter(col("_rescued_data").isNotNull())

query_rescued = rescued_data.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint_rescued") \
    .start("/tmp/output_rescued_data")
```

✅ **Expected Outcome**:
- File metadata tracking for audit trails
- Efficient file discovery with notification mode
- Partition-aware processing
- Corrupt record handling without failing entire batches

⚠️ **Exam Trap**: 
- **Wrong Answer**: "Auto Loader notification mode works with all file systems"
  - **Why it's wrong**: Notification mode requires cloud storage with event notification support (S3+SNS/SQS, Azure+Event Grid, GCS+Pub/Sub)
  - **Correct**: "Use notification mode for cloud storage at scale; use directory listing for DBFS or small datasets"

---

## Concept Quiz

1. What happens if you use `dropDuplicates()` in streaming without a watermark?
2. Which trigger type processes all available data once and then stops?
3. Can you use `complete` output mode with watermarks in streaming aggregations?
4. What's the difference between `mapGroupsWithState` and `flatMapGroupsWithState`?
5. Does Auto Loader's `addNewColumns` mode require `mergeSchema=true` in the writer?
6. What is the purpose of time constraints in stream-stream joins?
7. How do you check the current watermark value of a running streaming query?
8. What's stored in the checkpoint's `commits/` directory?
9. Can session windows handle overlapping sessions for the same user?
10. What's the benefit of `cloudFiles.useNotifications=true` over directory listing?

**Answers:**

1. State grows unbounded as all distinct keys are kept forever, leading to potential OOM errors
2. `availableNow=True` (formerly `once=True`, which is deprecated)
3. No, `complete` mode is incompatible with watermarks; use `append` or `update` mode instead
4. `mapGroupsWithState` returns one row per group; `flatMapGroupsWithState` can return multiple rows per group
5. Yes, to allow schema evolution in the Delta target table
6. Bounds prevent unbounded state growth and enable state cleanup via watermarks
7. `query.lastProgress['eventTime']['watermark']` shows the current watermark timestamp
8. Metadata about successfully committed batches, used for exactly-once processing
9. No, session windows create non-overlapping sessions based on inactivity gaps
10. File notifications scale better for large datasets, avoiding repeated directory listings

---

## Key Takeaways

✅ **Triggers & Performance**:
- `availableNow` for batch-style streaming (replaces deprecated `once`)
- `processingTime` for micro-batch with controlled intervals
- `continuous` for millisecond latency (limited support)

✅ **Watermarks**:
- Essential for bounded state in streaming aggregations
- Required on both sides of stream-stream joins
- Format: `.withWatermark("event_time_column", "threshold")`

✅ **Auto Loader**:
- `cloudFiles` format for incremental file ingestion
- Schema evolution modes: `addNewColumns`, `rescue`, `failOnNewColumns`
- Notification mode for scalable cloud storage monitoring

✅ **Stateful Operations**:
- Standard aggregations for simple counts/sums
- Session windows for sessionization
- `mapGroupsWithState` for custom stateful logic
- Always use watermarks to prevent unbounded state growth

✅ **Streaming Joins**:
- Stream-stream joins require watermarks on both sides
- Stream-static joins don't need watermarks
- Use time constraints in join conditions

✅ **Deduplication**:
- `dropDuplicates` with watermark for bounded state
- Merge operations for idempotent writes
- Consider state size implications

✅ **Monitoring**:
- `query.lastProgress` for batch-level metrics
- Checkpoint structure: commits, offsets, metadata, sources
- State store metrics indicate memory usage

✅ **Best Practices**:
- Always checkpoint in reliable storage
- Set appropriate watermarks based on SLA
- Monitor state size and processing lag
- Use `foreachBatch` for complex sinks
