# Day 12 Practice Tasks — Lakeflow Declarative Pipelines (DLT) - Advanced

> **Exam section:** Data Transformation, Cleansing, and Quality (10%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last.
Every task has:
- 📖 **Context** — why this matters for the exam
- 🛠️ **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- ⚠️ **Exam trap** — a common wrong-answer pitfall

---

## Task 1 — DLT Expectations: expect vs expect_or_drop vs expect_or_fail

📖 **Context:**
DLT expectations are THE most tested topic for DLT in the Professional exam. You must understand the three behaviors and when to use each.

🛠️ **Instructions:**

**Step 1 — Create a DLT notebook with all three expectation types:**

Create a new Python notebook: `pro/week2/notebooks/day12_dlt_expectations.py`

```python
import dlt
from pyspark.sql.functions import col

# Bronze layer - raw data with quality issues
@dlt.table(
    name="bronze_orders",
    comment="Raw orders with quality issues for testing expectations"
)
def bronze_orders():
    return spark.createDataFrame([
        (1, 100, "pending", "2026-01-15"),
        (2, -50, "completed", "2026-01-16"),  # negative amount - invalid
        (3, None, "shipped", "2026-01-17"),   # null amount - invalid
        (4, 200, "invalid_status", "2026-01-18"),  # bad status
        (5, 150, "completed", "2026-01-19"),
        (6, 0, "pending", "2026-01-20"),  # zero amount - edge case
    ], ["order_id", "amount", "status", "order_date"])

# Silver layer - WARN on violations (keep all rows, log violations)
@dlt.table(
    name="silver_orders_warn",
    comment="Keeps invalid rows but WARNS - use for audit"
)
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect("valid_status", "status IN ('pending', 'completed', 'shipped', 'delivered')")
def silver_orders_warn():
    return dlt.read("bronze_orders")

# Silver layer - DROP rows that violate (most common in production)
@dlt.table(
    name="silver_orders_clean",
    comment="Drops invalid rows - use for clean silver layer"
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_status", "status IN ('pending', 'completed', 'shipped', 'delivered')")
def silver_orders_clean():
    return dlt.read("bronze_orders")

# Silver layer - FAIL pipeline on violations (critical data)
@dlt.table(
    name="silver_orders_critical",
    comment="Fails pipeline if ANY row is invalid - use for critical data"
)
@dlt.expect_or_fail("valid_amount", "amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('pending', 'completed', 'shipped', 'delivered')")
def silver_orders_critical():
    return dlt.read("bronze_orders")
```

**Step 2 — Create DLT pipeline:**

Go to Databricks UI → **Delta Live Tables** → **Create Pipeline**

Configure:
- **Pipeline name**: `day12_expectations_test`
- **Product edition**: Advanced
- **Notebook**: Select the notebook you just created
- **Target**: `training.prep` (or your catalog.schema)
- **Storage location**: `/Volumes/training/prep/dlt_storage/day12`
- **Cluster mode**: Fixed size (1 worker) or Serverless

**Step 3 — Run the pipeline and observe:**

Click **Start** → Watch the pipeline execute

**Step 4 — Query the results:**

```sql
-- Count rows in each table
SELECT 'bronze_orders' as table_name, COUNT(*) as row_count 
FROM training.prep.bronze_orders
UNION ALL
SELECT 'silver_orders_warn', COUNT(*) 
FROM training.prep.silver_orders_warn
UNION ALL
SELECT 'silver_orders_clean', COUNT(*) 
FROM training.prep.silver_orders_clean;
```

✅ **Expected outcome:**
- `bronze_orders`: 6 rows (all data)
- `silver_orders_warn`: 6 rows (all data kept, violations logged)
- `silver_orders_clean`: 2-3 rows (invalid rows dropped)
- `silver_orders_critical`: Pipeline FAILS because of invalid data

⚠️ **Exam trap:**
`expect` = WARN (rows kept), `expect_or_drop` = DROP (rows removed), `expect_or_fail` = FAIL pipeline. Exam may ask "which prevents bad data from entering silver?" Answer: `expect_or_drop` (not `expect`).

---

## Task 2 — Multiple Expectations with expect_all

📖 **Context:**
Professional exam tests combining multiple expectations efficiently.

🛠️ **Instructions:**

```python
# Add to your DLT notebook

@dlt.table(name="silver_validated_all")
@dlt.expect_all({
    "positive_amount": "amount > 0",
    "valid_status": "status IN ('pending', 'completed', 'shipped', 'delivered')",
    "recent_order": "order_date >= '2026-01-01'",
    "non_null_id": "order_id IS NOT NULL"
})
def silver_validated_all():
    return dlt.read("bronze_orders")

# All expectations must pass - warns on ANY violation
# For dropping, use expect_all_or_drop
@dlt.table(name="silver_validated_drop")
@dlt.expect_all_or_drop({
    "positive_amount": "amount > 0",
    "valid_status": "status IN ('pending', 'completed', 'shipped', 'delivered')"
})
def silver_validated_drop():
    return dlt.read("bronze_orders")
```

✅ **Expected outcome:**
- `expect_all`: evaluates ALL expectations, warns on ANY failure
- `expect_all_or_drop`: drops row if ANY expectation fails
- More efficient than multiple decorators

⚠️ **Exam trap:**
`expect_all_or_drop` drops row if ANY condition fails (not ALL). One bad field = entire row dropped.

---

## Task 3 — CDC with APPLY CHANGES INTO

📖 **Context:**
APPLY CHANGES INTO is the DLT way to implement CDC (Change Data Capture). This is heavily tested.

🛠️ **Instructions:**

**Step 1 — Create CDC source data:**

```python
# Simulate CDC stream
@dlt.view
def customers_cdc_source():
    return spark.createDataFrame([
        (1, "Alice", "alice@email.com", "INSERT", "2026-01-15 10:00:00"),
        (2, "Bob", "bob@email.com", "INSERT", "2026-01-15 11:00:00"),
        (1, "Alice Smith", "alice.smith@email.com", "UPDATE", "2026-01-16 09:00:00"),
        (3, "Charlie", "charlie@email.com", "INSERT", "2026-01-16 10:00:00"),
        (2, None, None, "DELETE", "2026-01-17 14:00:00"),
    ], ["customer_id", "name", "email", "operation", "update_timestamp"])

# Apply CDC to target table
dlt.create_streaming_table("customers_silver")

dlt.apply_changes(
    target="customers_silver",
    source="customers_cdc_source",
    keys=["customer_id"],
    sequence_by=col("update_timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    apply_as_truncates=expr("operation = 'TRUNCATE'"),
    column_list=["customer_id", "name", "email"],
    except_column_list=["operation", "update_timestamp"]
)
```

**Step 2 — Query the CDC result:**

```sql
SELECT * FROM training.prep.customers_silver ORDER BY customer_id;
```

✅ **Expected outcome:**
- customer_id=1: "Alice Smith" with updated email (UPDATE applied)
- customer_id=2: NOT present (DELETE applied)
- customer_id=3: "Charlie" (INSERT applied)
- Changes processed in order by `update_timestamp`

⚠️ **Exam trap:**
APPLY CHANGES requires `dlt.create_streaming_table()` target, NOT `@dlt.table`. Using `@dlt.table` causes error. Also, `sequence_by` determines order - later timestamps override earlier ones.

---

## Task 4 — SCD Type 2 with APPLY CHANGES

📖 **Context:**
SCD Type 2 (history tracking) is a Professional exam topic.

🛠️ **Instructions:**

```python
# SCD Type 2 - track history
dlt.create_streaming_table("customers_history")

dlt.apply_changes(
    target="customers_history",
    source="customers_cdc_source",
    keys=["customer_id"],
    sequence_by=col("update_timestamp"),
    stored_as_scd_type="2",  # KEY LINE for SCD Type 2
    apply_as_deletes=expr("operation = 'DELETE'")
)
```

**Query to see history:**

```sql
SELECT 
    customer_id, 
    name, 
    email,
    __START_AT,
    __END_AT,
    __CURRENT
FROM training.prep.customers_history
ORDER BY customer_id, __START_AT;
```

✅ **Expected outcome:**
- customer_id=1 has TWO rows: one for "Alice" (historical), one for "Alice Smith" (current)
- `__START_AT` and `__END_AT` show validity period
- `__CURRENT = true` for latest version

⚠️ **Exam trap:**
SCD Type 2 requires `stored_as_scd_type="2"`. Default is SCD Type 1 (upsert, no history). Exam may ask "how to track history in DLT?" Answer: SCD Type 2.

---

## Task 5 — DLT Event Log Analysis

📖 **Context:**
Querying the DLT event log is essential for monitoring and debugging.

🛠️ **Instructions:**

**Step 1 — Find your pipeline's event log path:**

Go to your DLT pipeline → **Configuration** → note the **Storage location**

Event log is at: `<storage_location>/system/events`

**Step 2 — Query expectation violations:**

```sql
-- Replace with your actual event log path
CREATE OR REPLACE TEMP VIEW dlt_events AS
SELECT * FROM delta.`/Volumes/training/prep/dlt_storage/day12/system/events`;

-- Find all expectation violations
SELECT 
    timestamp,
    origin.flow_name as table_name,
    details:flow_progress:data_quality:dropped_records as dropped_records,
    details:flow_progress:data_quality:expectations as expectations
FROM dlt_events
WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality IS NOT NULL
ORDER BY timestamp DESC;
```

**Step 3 — Parse specific expectation results:**

```sql
SELECT 
    timestamp,
    origin.flow_name as table_name,
    expectation.name,
    expectation.passed_records,
    expectation.failed_records
FROM dlt_events
LATERAL VIEW explode(
    from_json(
        details:flow_progress:data_quality:expectations, 
        'array<struct<name:string,passed_records:long,failed_records:long>>'
    )
) t AS expectation
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```

✅ **Expected outcome:**
- See all tables and their expectation results
- `failed_records` shows how many rows violated each expectation
- `dropped_records` shows total rows dropped

⚠️ **Exam trap:**
Event log path is `<storage>/system/events`. Exam may give a storage path and ask where to find event logs. Don't confuse with table data location.

---

## Task 6 — DLT Table Types: @dlt.table vs @dlt.view vs dlt.create_streaming_table

📖 **Context:**
Understanding when to use each DLT table type is critical.

🛠️ **Instructions:**

```python
# 1. @dlt.table - Materialized table (stored)
@dlt.table(name="gold_daily_revenue")
def gold_daily_revenue():
    return dlt.read("silver_orders_clean") \
        .groupBy("order_date") \
        .agg({"amount": "sum"})

# 2. @dlt.view - Temporary view (not stored, query-time computation)
@dlt.view
def intermediate_calculations():
    return dlt.read("silver_orders_clean") \
        .filter(col("amount") > 100)

# 3. Streaming table (for incremental processing)
@dlt.table(
    name="silver_orders_streaming",
    comment="Streaming table for incremental ingestion"
)
def silver_orders_streaming():
    return dlt.read_stream("bronze_orders")  # Use read_stream for streaming

# 4. create_streaming_table (for APPLY CHANGES)
# Used in Task 3 above
```

| Type | Stored? | Use Case | Supports Streaming? |
|------|---------|----------|---------------------|
| `@dlt.table` | Yes | Materialized results, aggregations | Yes |
| `@dlt.view` | No | Intermediate transformations | Yes |
| `dlt.create_streaming_table` | Yes | CDC target for APPLY CHANGES | Yes |

✅ **Expected outcome:**
You understand:
- `@dlt.table`: stored Delta table
- `@dlt.view`: temporary, not stored
- `dlt.read_stream()`: streaming source
- `dlt.create_streaming_table()`: required for APPLY CHANGES

⚠️ **Exam trap:**
APPLY CHANGES requires `create_streaming_table`, not `@dlt.table`. Views are NOT stored, so can't be queried outside the pipeline.

---

## Task 7 — Pipeline Execution Modes

📖 **Context:**
Understanding Triggered vs Continuous modes.

🛠️ **Instructions:**

**Step 1 — Run pipeline in Triggered mode:**

In DLT Pipeline UI:
- Mode: **Triggered**
- Click **Start**

Observe:
- Processes all available data
- Pipeline stops when done
- Good for: batch ETL, scheduled jobs

**Step 2 — Run pipeline in Continuous mode:**

In DLT Pipeline UI:
- Mode: **Continuous**
- Click **Start**

Observe:
- Pipeline keeps running
- Processes new data as it arrives
- Low latency
- Good for: real-time streaming

**Step 3 — Development vs Production mode:**

- **Development**: reuses cluster, no auto-retry, faster iteration
- **Production**: auto-scaling, retries on failure, more resilient

✅ **Expected outcome:**
- Triggered: runs once, stops
- Continuous: runs indefinitely
- Development: for testing
- Production: for production workloads

⚠️ **Exam trap:**
Continuous mode has LOWER latency but HIGHER cost (always running). Triggered mode is cheaper for batch.

---

## Task 8 — Full Refresh

📖 **Context:**
Full refresh reprocesses ALL data from scratch.

🛠️ **Instructions:**

**In DLT Pipeline UI:**

- Click **Start** dropdown
- Select **Start with full refresh**

Or via CLI:
```bash
databricks pipelines start --pipeline-id <id> --full-refresh
```

**Selective refresh (specific table):**
```bash
databricks pipelines start --pipeline-id <id> --full-refresh-selection silver_orders_clean
```

✅ **Expected outcome:**
- All tables recomputed from source
- Previous data deleted
- Use when: schema changes, logic changes, corrupted state

⚠️ **Exam trap:**
Full refresh DELETES existing data and recomputes. It's expensive. Don't use for routine runs.

---

## Concept Quiz (Self-Check)

Answer these without looking at notes:

1. What is the difference between `expect`, `expect_or_drop`, and `expect_or_fail`?
2. Does `expect_all_or_drop` drop if ANY expectation fails or ALL expectations fail?
3. What function is required for CDC in DLT?
4. What is the default SCD type for `apply_changes()`?
5. How do you enable SCD Type 2 in DLT?
6. Where is the DLT event log stored?
7. Can you query a `@dlt.view` outside the pipeline?
8. What table type is required as target for `apply_changes()`?
9. What is the difference between Triggered and Continuous pipeline mode?
10. What does `full-refresh` do?

**Answers:**
1. `expect` = warn (keep row); `expect_or_drop` = drop row; `expect_or_fail` = fail pipeline
2. ANY - one failed expectation drops the entire row
3. `dlt.apply_changes()`
4. SCD Type 1 (upsert, no history)
5. `stored_as_scd_type="2"` parameter
6. `<pipeline_storage>/system/events`
7. No - views are temporary, not stored
8. `dlt.create_streaming_table()` - NOT `@dlt.table`
9. Triggered = runs once, stops; Continuous = always running, low latency
10. Deletes all data and recomputes from scratch

---

## Key Takeaways for Exam

✅ **`expect` = WARN, `expect_or_drop` = DROP, `expect_or_fail` = FAIL pipeline**
✅ **APPLY CHANGES requires `dlt.create_streaming_table()` target**
✅ **SCD Type 2 needs `stored_as_scd_type="2"`, default is Type 1**
✅ **Event log at `<storage>/system/events`, query with `delta.\`path\``**
✅ **`@dlt.view` is NOT stored, only for intermediate logic**
✅ **`sequence_by` in apply_changes determines CDC order**
✅ **Continuous mode = low latency, higher cost; Triggered = batch, cheaper**
✅ **Full refresh = recompute ALL data, deletes existing data**
