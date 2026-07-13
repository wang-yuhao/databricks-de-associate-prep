# Day 3 Practice Tasks — Lakeflow Declarative Pipelines (Advanced DLT)

> **Exam section:** Data Transformation, Cleansing, and Quality (10%), Data Ingestion (25%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last. Every task has:

- 📘 **Context** — why this matters for the exam
- 🔧 **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- ⚠️ **Exam trap** — a common wrong-answer pitfall

---

## Task 1 — DLT Pipeline Definition with Expectations

📘 **Context**: The Professional exam tests your ability to create DLT pipelines with data quality rules using expectations. You must understand the difference between `@dlt.table`, `@dlt.view`, and `@expect` decorators.

🔧 **Instructions**:

### Step 1 — Create a DLT notebook with streaming table:

```python
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from cloud storage",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "order_date"
    }
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/mnt/schema/orders")
            .load("/mnt/raw/orders/")
    )
```

### Step 2 — Add silver table with expectations:

```python
@dlt.table(
    name="silver_orders",
    comment="Cleaned orders with quality checks"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_fail("valid_amount", "amount > 0")
@dlt.expect("valid_status", "status IN ('pending', 'shipped', 'delivered')")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
            .select(
                col("order_id"),
                col("customer_id"),
                col("amount").cast("decimal(10,2)"),
                col("status"),
                to_date(col("order_date")).alias("order_date")
            )
    )
```

### Step 3 — Create gold aggregation:

```python
@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregated from silver orders"
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")
            .groupBy("order_date")
            .agg(
                sum("amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("amount").alias("avg_order_value")
            )
    )
```

✅ **Expected outcome**: 
- Pipeline creates three tables: bronze_orders (streaming), silver_orders (with quality checks), gold_daily_revenue (aggregated)
- Rows failing `expect_or_fail` will halt the pipeline
- Rows failing `expect_or_drop` will be dropped
- Rows failing `expect` will be recorded but still processed

⚠️ **Exam trap**: 
- `@dlt.expect()` logs violations but doesn't drop rows
- `@dlt.expect_or_drop()` drops invalid rows
- `@dlt.expect_or_fail()` stops the pipeline on violations
- Many candidates confuse these three behaviors!

---

## Task 2 — Change Data Capture (CDC) with APPLY CHANGES INTO

📘 **Context**: DLT provides `dlt.apply_changes()` for handling CDC from sources like Debezium or Delta CDF. The exam tests your understanding of SCD Type 1 vs Type 2.

🔧 **Instructions**:

### Step 1 — Create bronze CDC stream:

```python
@dlt.table(
    name="bronze_customer_cdc",
    comment="Raw CDC events from source system"
)
def bronze_customer_cdc():
    return (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table("source_catalog.source_schema.customers_cdc")
    )
```

### Step 2 — Apply changes with SCD Type 1:

```python
dlt.create_streaming_table("silver_customers_scd1")

dlt.apply_changes(
    target="silver_customers_scd1",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_timestamp"),
    except_column_list=["_rescued_data"],
    stored_as_scd_type="1"
)
```

### Step 3 — Apply changes with SCD Type 2:

```python
dlt.create_streaming_table("silver_customers_scd2")

dlt.apply_changes(
    target="silver_customers_scd2",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=["email", "phone", "address"],
    track_history_except_column_list=["last_login"]
)
```

✅ **Expected outcome**: 
- SCD Type 1: Only current records, updates overwrite
- SCD Type 2: Historical records preserved with __START_AT, __END_AT, __CURRENT columns
- `sequence_by` ensures correct ordering of CDC events

⚠️ **Exam trap**: 
- SCD Type 1 = current state only (no history)
- SCD Type 2 = full history with temporal columns
- `sequence_by` is REQUIRED to handle out-of-order events
- Without `sequence_by`, late-arriving updates may be ignored!

---

## Task 3 — DLT Pipeline Configuration and Deployment

📘 **Context**: DLT pipelines require proper configuration for production use. The exam tests your knowledge of pipeline settings, refresh modes, and Unity Catalog integration.

🔧 **Instructions**:

### Step 1 — Create DLT pipeline via Databricks UI or CLI:

```python
# Pipeline configuration (as JSON for CLI deployment)
{
    "name": "orders_dlt_pipeline",
    "storage": "/mnt/dlt/orders_pipeline",
    "target": "prod_catalog.sales_schema",
    "notebooks": [
        {
            "path": "/Workspace/Pipelines/orders_dlt_notebook"
        }
    ],
    "configuration": {
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "pipelines.trigger.interval": "5 minutes"
    },
    "clusters": [
        {
            "label": "default",
            "num_workers": 2,
            "node_type_id": "i3.xlarge",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5
            }
        }
    ],
    "continuous": false,
    "development": false,
    "photon": true,
    "channel": "CURRENT"
}
```

### Step 2 — Understand refresh modes:

```python
# Full Refresh: Drop and recreate all tables
# Triggered: Run once on demand or on schedule
# Continuous: Keep pipeline running, process new data as it arrives

# For exam:
# - Full refresh = expensive, recreates everything
# - Triggered = batch processing, cost-effective
# - Continuous = real-time streaming, higher cost
```

### Step 3 — Deploy with Unity Catalog:

```python
# Specify Unity Catalog target in pipeline config
"target": "main_catalog.analytics_schema"

# All DLT tables will be created as:
# main_catalog.analytics_schema.bronze_orders
# main_catalog.analytics_schema.silver_orders
# main_catalog.analytics_schema.gold_daily_revenue

# Unity Catalog provides:
# - Fine-grained access control
# - Data lineage tracking
# - Audit logging
# - Cross-workspace data sharing
```

✅ **Expected outcome**: 
- Pipeline deployed with Unity Catalog target
- Tables accessible via 3-level namespace
- Lineage visible in Unity Catalog UI

⚠️ **Exam trap**: 
- `target` must specify Unity Catalog: `catalog.schema`
- Without Unity Catalog, tables go to Hive metastore
- `storage` location is for DLT metadata, NOT your tables
- Continuous mode costs more than triggered mode!

---

## Task 4 — DLT Event Log Analysis

📘 **Context**: DLT creates an event log for monitoring and debugging. The exam may ask you to query the event log to troubleshoot pipeline issues.

🔧 **Instructions**:

### Step 1 — Query the DLT event log:

```sql
-- Event log is stored at: <storage_location>/system/events
-- Query using SQL:

SELECT 
    timestamp,
    details:flow_definition.output_dataset as dataset,
    details:flow_definition.input_datasets as inputs,
    details:flow_progress.metrics.num_output_rows as output_rows,
    details:flow_progress.data_quality.dropped_records as dropped_records,
    details:flow_progress.data_quality.expectations as expectations
FROM 
    delta.`/mnt/dlt/orders_pipeline/system/events`
WHERE 
    event_type = 'flow_progress'
    AND timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY 
    timestamp DESC;
```

### Step 2 — Monitor data quality metrics:

```sql
-- Check expectation violations:
SELECT 
    timestamp,
    details:flow_definition.output_dataset as table_name,
    explode(details:flow_progress.data_quality.expectations) as expectation,
    expectation.name as rule_name,
    expectation.passed_records,
    expectation.failed_records
FROM 
    delta.`/mnt/dlt/orders_pipeline/system/events`
WHERE 
    event_type = 'flow_progress'
    AND expectation.failed_records > 0
ORDER BY 
    timestamp DESC;
```

✅ **Expected outcome**: 
- Event log shows all pipeline execution details
- Data quality metrics track expectation violations
- Flow progress shows input/output row counts

⚠️ **Exam trap**: 
- Event log path is `<storage>/system/events`, NOT `<target>`
- Use `delta.\`path\`` syntax to query by path
- Event log structure uses nested JSON in `details` column
- Must use `explode()` to access array elements!

---

## Concept Quiz

1. What happens to rows that fail an `@dlt.expect_or_fail()` constraint?
   - A) Rows are dropped silently
   - B) Rows are logged but processed
   - C) Pipeline stops immediately ✓
   - D) Rows are quarantined to a separate table

2. In DLT CDC with `apply_changes()`, what does `sequence_by` do?
   - A) Sorts the output table
   - B) Determines the order of CDC events for conflict resolution ✓
   - C) Partitions the target table
   - D) Creates a sequence number column

3. What is the difference between SCD Type 1 and Type 2?
   - A) Type 1 is faster than Type 2
   - B) Type 1 keeps current state only, Type 2 keeps full history ✓
   - C) Type 1 uses MERGE, Type 2 uses INSERT
   - D) Type 1 is for streaming, Type 2 is for batch

4. Where is the DLT event log stored?
   - A) In the target database
   - B) In `<storage_location>/system/events` ✓
   - C) In Databricks SQL warehouse
   - D) In the Unity Catalog system schema

5. What does `continuous: true` mean in a DLT pipeline?
   - A) Pipeline runs every minute
   - B) Pipeline keeps running and processes new data immediately ✓
   - C) Pipeline uses continuous optimization
   - D) Pipeline never stops even on errors

---

## Key Takeaways

✅ **For the exam, remember:**

1. **DLT Expectations**: Three types with different behaviors
   - `@dlt.expect()` = log only
   - `@dlt.expect_or_drop()` = drop invalid rows
   - `@dlt.expect_or_fail()` = stop pipeline

2. **CDC with apply_changes()**:
   - `sequence_by` is critical for handling out-of-order events
   - SCD Type 1 = current state only
   - SCD Type 2 = full history with temporal columns

3. **Pipeline Configuration**:
   - `target` = Unity Catalog destination (catalog.schema)
   - `storage` = DLT metadata location
   - Continuous vs Triggered vs Full Refresh modes

4. **Event Log**:
   - Located at `<storage>/system/events`
   - Query with `delta.\`path\`` syntax
   - Contains flow_progress, data_quality, and metrics

5. **Unity Catalog Integration**:
   - Use 3-level namespace: `catalog.schema.table`
   - Provides lineage, access control, and audit
   - Required for production DLT pipelines

---

**Next Steps**: Review `study-notes.md` again, focusing on DLT architecture and pipeline lifecycle. Practice creating DLT pipelines with different expectation types and CDC scenarios.
