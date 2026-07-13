# Day 4 Practice Tasks — Delta Live Tables (DLT)

> **Exam section:** Data Pipelines (40%), Data Transformation, Cleansing, and Quality (10%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
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

## Task 1 — Bronze Table with Auto Loader

📖 **Context**: DLT Bronze tables use Auto Loader for incremental ingestion. The Professional exam tests `@dlt.table` vs `dlt.create_streaming_table()`.

🛠️ **Instructions**:

### Step 1 — Create a DLT Python notebook:

Create new notebook: `pro/week1/notebooks/day4_dlt_pipeline.py`

### Step 2 — Define Bronze streaming table:

```python
import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

@dlt.table(
    name="orders_bronze",
    comment="Raw orders data ingested from cloud storage via Auto Loader"
)
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/tmp/schemas/orders")
            .load("/databricks-datasets/retail-org/customers/")
            .withColumn("_ingest_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
    )
```

### Step 3 — Verify:

- The table is **streaming** (Auto Loader creates a stream)
- Metadata columns `_ingest_timestamp` and `_source_file` are added
- Schema is inferred and stored in `cloudFiles.schemaLocation`

✅ **Expected outcome**: 
- Bronze table continuously ingests new files from `/databricks-datasets/retail-org/customers/`
- Each row has ingest timestamp and source file path
- Schema evolution is handled automatically by Auto Loader

⚠️ **Exam trap**: Forgetting `cloudFiles.schemaLocation` causes schema inference to run on EVERY file. This is expensive! Always set a schema location for production pipelines.

---

## Task 2 — Silver Table with Expectations

📖 **Context**: DLT Expectations are THE most tested DLT feature. You must know `expect`, `expect_or_drop`, and `expect_or_fail`.

🛠️ **Instructions**:

### Step 1 — Define Silver table with expectations:

```python
@dlt.table(
    name="orders_silver",
    comment="Cleansed orders with data quality rules applied"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("positive_amount", "amount > 0")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                "order_id",
                "customer_id",
                col("order_date").cast("date"),
                col("amount").cast("decimal(10,2)"),
                "status"
            )
    )
```

### Step 2 — Understand expectations:

| Expectation | Behavior | Use Case |
|-------------|----------|----------|
| `@dlt.expect("name", "condition")` | WARN if violated, row KEPT | Tracking data quality issues |
| `@dlt.expect_or_drop("name", "condition")` | DROP if violated | Removing invalid rows |
| `@dlt.expect_or_fail("name", "condition")` | FAIL pipeline if violated | Critical data integrity |

✅ **Expected outcome**: 
- Rows with NULL `order_id` are DROPPED
- Rows with negative `amount` are KEPT but flagged in metrics
- Expectation metrics appear in the DLT Event Log

⚠️ **Exam trap**: Thinking `expect` drops rows. Wrong! Only `expect_or_drop` and `expect_or_fail` prevent rows from passing through. `expect` only logs violations.

---

## Task 3 — Gold Aggregation Table

📖 **Context**: Gold tables are materialized views for analytics. The exam tests the difference between streaming and materialized tables.

🛠️ **Instructions**:

### Step 1 — Define Gold aggregation table:

```python
@dlt.table(
    name="orders_gold_daily",
    comment="Daily order aggregations by customer segment"
)
def orders_gold_daily():
    return (
        dlt.read("orders_silver")
            .groupBy("order_date", "customer_segment")
            .agg(
                count("*").alias("total_orders"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_order_value")
            )
    )
```

### Step 2 — Understand table types:

| Table Type | Definition | Refresh Behavior |
|------------|------------|------------------|
| **Streaming Live Table** | `@dlt.table()` + `readStream` | Continuous processing |
| **Materialized Live Table** | `@dlt.table()` + `read()` | Full refresh or incremental |
| **View** | `@dlt.view()` | Not stored, always computed |

✅ **Expected outcome**: 
- Gold table is MATERIALIZED (not streaming)
- Full refresh recomputes all aggregations from Silver
- No `_rescued_data` column (only in streaming tables)

⚠️ **Exam trap**: Thinking Gold tables must be streaming. Wrong! Gold tables are typically materialized views that aggregate cleaned data. Use materialized tables when you need complete aggregations.

---

## Task 4 — Pipeline Modes and Development Settings

📖 **Context**: The exam tests when to use Triggered vs Continuous mode, and Development vs Production mode.

🛠️ **Instructions**:

### Step 1 — Create DLT pipeline via UI:

1. Go to **Workflows** > **Delta Live Tables**
2. Click **Create Pipeline**
3. Set these configs:
   - **Name**: `orders_dlt_pipeline`
   - **Notebook**: Select your Day 4 notebook
   - **Mode**: Development
   - **Trigger**: Triggered
   - **Storage Location**: `/tmp/dlt/orders_pipeline`
   - **Catalog**: `training_uc`
   - **Target Schema**: `bronze`

### Step 2 — Understand pipeline modes:

| Mode | Use Case | Key Differences |
|------|----------|----------------|
| **Development** | Testing, debugging | Tables prefixed with username, enhanced logging, auto-retry disabled |
| **Production** | Live pipelines | No username prefix, strict error handling, full lineage tracking |

| Trigger Type | Use Case | Key Differences |
|--------------|----------|----------------|
| **Triggered** | Batch processing | Runs once, processes all available data, stops |
| **Continuous** | Real-time streaming | Runs continuously, processes new data as it arrives |

### Step 3 — Run the pipeline:

1. Click **Start** to run the pipeline
2. Observe the lineage graph (Bronze → Silver → Gold)
3. Check the **Event Log** for expectation metrics

✅ **Expected outcome**: 
- Pipeline creates 3 tables: `orders_bronze`, `orders_silver`, `orders_gold_daily`
- In Development mode, tables are prefixed with your username
- Event Log shows expectation violations and processing metrics

⚠️ **Exam trap**: Thinking Development mode tables are permanent. Wrong! Dev tables are temporary and may be dropped when switching to Production. Always use Production mode for live pipelines.

---

## Task 5 — DLT Expectations Analysis

📖 **Context**: The exam gives you a table with multiple expectations and asks you to predict which rows are kept/dropped/fail.

🛠️ **Instructions**:

Given a DLT table with these constraints:

```python
@dlt.table()
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dlt.expect("positive_amount", "amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('placed','shipped','delivered','cancelled')")
def orders_silver():
    return dlt.read_stream("orders_bronze")
```

### Analyze these rows:

| order_id | amount | status | Result |
|----------|--------|--------|--------|
| ORD001 | 50.0 | placed | ✅ KEPT |
| NULL | 50.0 | placed | ❌ DROPPED (fails `valid_order`) |
| ORD003 | -10.0 | shipped | ✅ KEPT (negative amount only WARNS) |
| ORD004 | 20.0 | refunded | 💥 PIPELINE FAILS (fails `valid_status`) |

✅ **Expected outcome**: 
- Row 1: All expectations pass → KEPT
- Row 2: `expect_or_drop` fails → DROPPED
- Row 3: Only `expect` fails → KEPT (with warning)
- Row 4: `expect_or_fail` fails → PIPELINE FAILS

⚠️ **Exam trap**: Thinking `expect` drops rows. Wrong! Only `expect_or_drop` and `expect_or_fail` affect data flow. `expect` only logs violations.

---

## Task 6 — Streaming vs Materialized Tables

📖 **Context**: The exam tests whether you understand when to use streaming vs materialized tables.

🛠️ **Instructions**:

### Decision Matrix:

| Scenario | Table Type | Reason |
|----------|------------|--------|
| Ingest raw data from cloud storage | Streaming | Auto Loader requires streaming |
| Apply row-level transformations | Streaming | Process records as they arrive |
| Aggregate metrics for BI dashboard | Materialized | Need complete view of all data |
| Join streaming data with dimension table | Streaming | Use `stream-static` join |
| Create a view for SQL queries | Materialized or View | Views are not stored |

### Key differences:

```python
# Streaming table
@dlt.table()
def my_stream():
    return spark.readStream.table("source")  # Note: readStream

# Materialized table
@dlt.table()
def my_table():
    return spark.read.table("source")  # Note: read (not readStream)

# View (not stored)
@dlt.view()
def my_view():
    return spark.read.table("source")
```

✅ **Expected outcome**: 
- Streaming tables for ingestion and row-level transformations
- Materialized tables for aggregations and analytics
- Views for intermediate transformations that don't need storage

⚠️ **Exam trap**: Using `dlt.read_stream()` on a materialized table. Wrong! Materialized tables use `dlt.read()` (non-streaming). Only Bronze and Silver tables are typically streaming.

---

## Task 7 — DLT Event Log Analysis

📖 **Context**: The exam tests your ability to query the DLT Event Log for metrics and debugging.

🛠️ **Instructions**:

### Step 1 — Query event log:

```sql
SELECT 
  timestamp,
  details:flow_definition.output_dataset as dataset,
  details:flow_definition.schema as schema,
  details:flow_progress.metrics.num_output_rows as rows_processed,
  details:flow_progress.data_quality.expectations as quality_metrics
FROM event_log("training_uc.bronze.orders_dlt_pipeline_events")
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 10;
```

### Step 2 — Find expectation violations:

```sql
SELECT 
  timestamp,
  details:flow_definition.output_dataset as dataset,
  details:flow_progress.data_quality.dropped_records as dropped_records,
  details:flow_progress.data_quality.expectations
FROM event_log("training_uc.bronze.orders_dlt_pipeline_events")
WHERE details:flow_progress.data_quality.dropped_records > 0
ORDER BY timestamp DESC;
```

✅ **Expected outcome**: 
- Event log shows all pipeline runs with detailed metrics
- Expectation violations are tracked per expectation name
- Dropped records are counted and logged

⚠️ **Exam trap**: Thinking event log is in `information_schema`. Wrong! Event log has its own path: `event_log("<pipeline-storage-path>/system/events")`.

---

## Task 8 — Concept Quiz

Answer these rapid-fire questions:

1. What is the difference between `@dlt.table()` and `@dlt.view()`?
2. What does `expect_or_drop` do?
3. What does `expect_or_fail` do?
4. When should you use **Triggered** mode vs **Continuous** mode?
5. What is the difference between Development and Production mode?
6. Can a DLT pipeline contain both streaming and materialized tables?
7. Where is the DLT Event Log stored?
8. What is `cloudFiles.schemaLocation` used for?
9. What happens if a row fails an `expect` constraint?
10. Can you use `dlt.read_stream()` on a materialized table?

---

## Key Takeaways for the Exam

✅ **DLT Table Types:**
- **Streaming Live Table**: `@dlt.table()` + `readStream` → continuous processing
- **Materialized Live Table**: `@dlt.table()` + `read()` → batch processing
- **View**: `@dlt.view()` → not stored, always computed

✅ **Expectations:**
- `@dlt.expect("name", "condition")`: WARN if violated, row KEPT
- `@dlt.expect_or_drop("name", "condition")`: DROP if violated
- `@dlt.expect_or_fail("name", "condition")`: FAIL pipeline if violated
- Expectations are tracked in the Event Log

✅ **Pipeline Modes:**
- **Development**: Testing, username-prefixed tables, enhanced logging
- **Production**: Live pipelines, strict error handling, full lineage
- **Triggered**: Runs once, processes all data, stops
- **Continuous**: Runs indefinitely, processes new data as it arrives

✅ **Auto Loader:**
- Use `cloudFiles` format for incremental ingestion
- Always set `cloudFiles.schemaLocation` for schema evolution
- Supports JSON, CSV, Parquet, Avro, and more

✅ **Event Log:**
- Query using `event_log("<pipeline-path>/system/events")`
- Shows metrics, expectation violations, and debugging info
- Use for monitoring and troubleshooting pipelines

---

## Next Steps

You've completed Day 4! You now understand Delta Live Tables at a professional level. Tomorrow (Day 5), you'll learn Workflows and Job Orchestration.
