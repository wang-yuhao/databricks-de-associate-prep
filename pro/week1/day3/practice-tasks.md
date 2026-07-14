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

## ⚠️ IMPORTANT: DLT Pipeline Setup Required (July 2026 UI)

**ERROR FIX: `ModuleNotFoundError: No module named 'dlt'`**

You CANNOT run DLT code on regular Databricks clusters! DLT code must run within a **Lakeflow Spark Declarative Pipeline** (formerly Delta Live Tables).

> 💡 `import dlt` still works in July 2026 — the Python module name has not changed even though the product is now called "Lakeflow Spark Declarative Pipelines".

### How to Run These Tasks (July 2026 UI):

**Step 1: Create a New ETL Pipeline**

1. In the Azure Databricks left sidebar, click the **➕ New** button at the top
2. Select **ETL pipeline** from the dropdown
3. A new pipeline opens in the **Lakeflow Pipelines Editor** — a full IDE with asset browser, code editor, and graph panel

> 🔁 **Alternative path**: Click **Jobs & Pipelines** in the left sidebar → click **New** → select **ETL Pipeline**

> ⚠️ **July 2026 note**: There is no separate "Workflows" item in the sidebar. Pipelines are now managed under **Jobs & Pipelines** or created directly via **New → ETL Pipeline**.

**Step 2: Name and Configure Your Pipeline**

1. At the top of the editor, rename the pipeline to: `day3_practice_pipeline`
2. Click the **catalog/schema** shown next to the name to set your Unity Catalog target (e.g. `your_catalog.your_schema`)
3. Click **Settings** (gear icon) to verify:
   - **Compute**: Serverless (default) or Fixed Size 1 worker
   - **Channel**: Current
   - **Pipeline mode**: Development

**Step 3: Create Source Code Files**

1. In the **Pipeline asset browser** (left panel), click **➕** next to the `transformations/` folder
2. Click **Transformation**
3. Name it `day3_dlt_pipeline`, choose **Python**
4. Click **Create**

> 💡 Use the `explorations/` folder for setup notebooks and test queries — code there runs on regular clusters, not inside the pipeline.

**Step 4: Run the Pipeline**
- Click **Run pipeline** in the toolbar to execute
- Monitor execution in the **Pipeline graph** (bottom panel)
- Check the **Event log** tab in the bottom panel for data quality metrics
- Use **Run file** to refresh only a single source file during development

**Alternative: Study for Exam Without Running**
- Focus on understanding `@dlt.table`, `@dlt.expect*` decorators
- Learn SCD Type 1 vs Type 2 differences
- Memorize DLT-specific syntax

| Regular Notebook | Lakeflow Pipeline |
|-----------------|------------------|
| Runs on cluster | Runs in Lakeflow Pipeline |
| Manual CREATE TABLE | `@dlt.table` decorator |
| Manual ordering | Auto dependency resolution |
| Manual validation | Built-in expectations |


## Task 1 — DLT Pipeline Definition with Expectations

📘 **Context**: The Professional exam tests your ability to create DLT pipelines with data quality rules using expectations. You must understand the difference between `@dlt.table`, `@dlt.view`, and `@expect` decorators.

🔧 **Instructions**:

Paste the following code **in order** into your `day3_dlt_pipeline.py` file in the `transformations/` folder:

### Step 1 — Create a bronze streaming table:

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

### Step 4 — Run the Pipeline

1. Click **Run pipeline** in the toolbar
2. The **Pipeline graph** (bottom panel) shows your 3-node DAG: `bronze_orders → silver_orders → gold_daily_revenue`
3. Watch the **Tables** tab for execution insights

✅ **Expected outcome**: 
- Pipeline creates three tables: bronze_orders (streaming), silver_orders (with quality checks), gold_daily_revenue (aggregated)
- Rows failing `expect_or_fail` will halt the pipeline
- Rows failing `expect_or_drop` will be dropped
- Rows failing `expect` will be recorded but still processed

⚠️ **Exam trap**: 

| Decorator | Behavior |
|:--|:--|
| `@dlt.expect()` | Logs violations, **keeps** rows |
| `@dlt.expect_or_drop()` | **Drops** invalid rows |
| `@dlt.expect_or_fail()` | **Stops** the whole pipeline |

Many candidates confuse these three behaviors!

---

## Task 2 — Change Data Capture (CDC) with `dlt.apply_changes()`

📘 **Context**: The exam tests your understanding of CDC, SCD Type 1 vs Type 2, and how Lakeflow pipelines process ordered changes. `dlt.apply_changes()` is still valid in July 2026 and is the primary CDC API tested in the exam.

🔧 **Instructions**:

### Part 1 — Create Mock CDC Source Data (in `explorations/`)

Since no live CDC source is available, create mock data using a setup notebook. In the **Lakeflow Pipelines Editor**, click **➕** next to `explorations/` → **Notebook** → name it `setup_cdc_data` → **Python**.

**Cell 1 — Create schema and source table:**

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS main.cdc_tutorial")

spark.sql("""
CREATE TABLE IF NOT EXISTS main.cdc_tutorial.customers_cdc (
    customer_id   INT,
    email         STRING,
    phone         STRING,
    address       STRING,
    last_login    TIMESTAMP,
    operation     STRING,
    updated_timestamp BIGINT
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
```

**Cell 2 — Insert INSERTs, UPDATEs, and a DELETE:**

```python
spark.sql("""
INSERT INTO main.cdc_tutorial.customers_cdc VALUES
(1, 'alice@example.com',   '+49-89-1234', 'Munich, DE',    current_timestamp(), 'INSERT', 1),
(2, 'bob@example.com',     '+49-89-5678', 'Berlin, DE',    current_timestamp(), 'INSERT', 2),
(3, 'charlie@example.com', '+49-89-9999', 'Hamburg, DE',   current_timestamp(), 'INSERT', 3),
(4, 'diana@example.com',   '+49-89-1111', 'Frankfurt, DE', current_timestamp(), 'INSERT', 4),
(1, 'alice_new@example.com', '+49-89-1234', 'Munich, DE',  current_timestamp(), 'UPDATE', 10),
(2, 'bob@example.com',       '+49-89-5678', 'Cologne, DE', current_timestamp(), 'UPDATE', 11),
(3, NULL, NULL, NULL, NULL, 'DELETE', 12)
""")
```

**Cell 3 — Verify 7 rows exist:**

```python
display(spark.sql("SELECT * FROM main.cdc_tutorial.customers_cdc ORDER BY customer_id, updated_timestamp"))
```

### Part 2 — Write CDC Pipeline in `transformations/`

Create a transformation file named `day3_cdc_pipeline.py`:

### Step 1 — Create bronze CDC stream:

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
    name="bronze_customer_cdc",
    comment="Raw CDC events from mock customers source"
)
def bronze_customer_cdc():
    return (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table("main.cdc_tutorial.customers_cdc")
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
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "updated_timestamp"],
    stored_as_scd_type="1"
)
```

> ⚠️ Do NOT include `_rescued_data` in `except_column_list` here — that column only exists for Auto Loader (`cloudFiles`) sources, not Delta CDF sources.

### Step 3 — Apply changes with SCD Type 2:

```python
dlt.create_streaming_table("silver_customers_scd2")

dlt.apply_changes(
    target="silver_customers_scd2",
    source="bronze_customer_cdc",
    keys=["customer_id"],
    sequence_by=col("updated_timestamp"),
    apply_as_deletes="operation = 'DELETE'",
    stored_as_scd_type="2",
    track_history_except_column_list=["last_login", "operation", "updated_timestamp"]
)
```

> ⚠️ **Critical**: Use EITHER `track_history_column_list` OR `track_history_except_column_list` — **never both**. Defining both at the same time causes `_LEGACY_ERROR_TEMP_66_INVALID_TRACK_HISTORY_COLS`.

### Part 3 — Run and Verify

Run the pipeline, then verify in a new `explorations/` notebook named `verify_cdc`:

**Check SCD Type 1** (should show 3 rows — charlie deleted, alice/bob updated):

```python
display(spark.sql("SELECT * FROM main.cdc_tutorial.silver_customers_scd1 ORDER BY customer_id"))
```

**Check SCD Type 2** (full history with `__START_AT`, `__END_AT`):

```python
display(spark.sql("""
SELECT
    customer_id,
    email,
    address,
    __START_AT,
    __END_AT,
    CASE WHEN __END_AT IS NULL THEN true ELSE false END AS is_current
FROM main.cdc_tutorial.silver_customers_scd2
ORDER BY customer_id, __START_AT
"""))
```

**Simulate a new CDC wave (optional)**:

```python
spark.sql("""
INSERT INTO main.cdc_tutorial.customers_cdc VALUES
(4, 'diana_new@example.com', '+49-89-2222', 'Stuttgart, DE', current_timestamp(), 'UPDATE', 20),
(5, 'eve@example.com', '+49-89-3333', 'Dresden, DE', current_timestamp(), 'INSERT', 21)
""")
```

Re-run the pipeline and check that diana now has two rows in SCD Type 2.

✅ **Expected outcome**: 

| | SCD Type 1 | SCD Type 2 |
|:--|:--|:--|
| alice after update | only `alice_new@example.com` | both old + new email rows |
| charlie after delete | **row gone** | row with `__END_AT` populated |
| Extra columns | none | `__START_AT`, `__END_AT` |
| Current row identification | only row present | `__END_AT IS NULL` |

⚠️ **Exam trap**: 
- Do **not** define both `track_history_column_list` and `track_history_except_column_list` at the same time
- Do **not** reference `_rescued_data` in `except_column_list` unless your source uses Auto Loader (`cloudFiles`)
- SCD Type 2 does **not** generate a `__CURRENT` column — use `__END_AT IS NULL` to identify current rows
- `sequence_by` is **required** to handle out-of-order CDC events; without it, late-arriving updates may corrupt your target table

---

## Task 3 — DLT Pipeline Configuration and Deployment

📘 **Context**: DLT pipelines require proper configuration for production use. The exam tests your knowledge of pipeline settings, refresh modes, and Unity Catalog integration.

🔧 **Instructions**:

### Step 1 — Open Pipeline Settings

In the Lakeflow Pipelines Editor, click the **Settings** (gear icon) in the asset browser panel.

You do **not** need to write JSON config manually — the editor provides a UI for all settings.

### Step 2 — Understand refresh modes:

```python
# Full Refresh: Drop and recreate all tables (expensive)
# Triggered: Run once on demand or on schedule (batch, cost-effective)
# Continuous: Keep pipeline running, process new data as it arrives (real-time, higher cost)
```

For this exercise, keep it as **Triggered** (Development mode).

### Step 3 — Verify Unity Catalog target:

1. In **Settings**, confirm **Target catalog** and **Target schema** are set (e.g. `main_catalog.analytics_schema`)
2. After a successful run, go to **Catalog Explorer** → browse to your catalog/schema
3. Tables will be registered as:
   - `main_catalog.analytics_schema.bronze_orders`
   - `main_catalog.analytics_schema.silver_orders`
   - `main_catalog.analytics_schema.gold_daily_revenue`

### Step 4 — Publish Event Log to Unity Catalog:

1. In **Settings → Advanced settings** → click **Edit advanced settings**
2. Under **Event logs**, click **Publish to catalog**
3. Provide a catalog, schema, and table name
4. Click **Save**

Reference pipeline configuration (for CLI/API deployment):

```json
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

### Step 1 — View the Event Log in the UI (July 2026)

In July 2026, the event log is accessible **directly in the Lakeflow Pipelines Editor**:

1. After running the pipeline, click the **Event log** tab in the **bottom panel** of the editor
2. All flow progress events, errors, and data quality metrics are visible inline

### Step 2 — Query the event log via SQL:

Open the **SQL Editor** (switch context in the top of the left sidebar), then run:

```sql
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

### Step 3 — Monitor data quality metrics:

```sql
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

## July 2026 UI Navigation Quick Reference

| Old path (pre-2026) | New path (July 2026) |
|:--|:--|
| Workflows → Pipelines | **Jobs & Pipelines** in left sidebar |
| Create Pipeline (legacy form) | **➕ New → ETL Pipeline** opens Lakeflow Pipelines Editor |
| DLT Notebook | Source file in `transformations/` folder |
| Setup/seed scripts | Notebook in `explorations/` folder |
| Pipeline monitoring tab | **Event log** tab in Lakeflow Pipelines Editor bottom panel |
| Separate pipeline config JSON | **Settings** panel inside the Editor |

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

6. You define both `track_history_column_list` and `track_history_except_column_list` in `apply_changes()`. What happens?
   - A) Only `track_history_column_list` takes effect
   - B) Only `track_history_except_column_list` takes effect
   - C) Databricks throws `_LEGACY_ERROR_TEMP_66_INVALID_TRACK_HISTORY_COLS` ✓
   - D) Both are merged into a single allow-list

7. For SCD Type 2, how do you identify the current (latest) version of a record?
   - A) Filter on `__CURRENT = true`
   - B) Filter on `__END_AT IS NULL` ✓
   - C) Filter on `__START_AT IS NOT NULL`
   - D) Take the row with the highest `__START_AT`

---

## Key Takeaways

✅ **For the exam, remember:**

1. **DLT Expectations**: Three types with different behaviors
   - `@dlt.expect()` = log only
   - `@dlt.expect_or_drop()` = drop invalid rows
   - `@dlt.expect_or_fail()` = stop pipeline

2. **CDC with `apply_changes()`**:
   - `sequence_by` is critical for handling out-of-order events
   - SCD Type 1 = current state only (overwrites)
   - SCD Type 2 = full history with `__START_AT` and `__END_AT` (no `__CURRENT` column)
   - Current rows in SCD Type 2 = `__END_AT IS NULL`
   - `track_history_column_list` and `track_history_except_column_list` are mutually exclusive
   - `_rescued_data` only exists in Auto Loader sources, not Delta CDF sources

3. **Pipeline Configuration**:
   - `target` = Unity Catalog destination (catalog.schema)
   - `storage` = DLT metadata location
   - Continuous vs Triggered vs Full Refresh modes

4. **Event Log**:
   - Located at `<storage>/system/events`
   - Also viewable in the Lakeflow Pipelines Editor **Event log** tab (July 2026 UI)
   - Query with `delta.\`path\`` syntax
   - Contains flow_progress, data_quality, and metrics

5. **Unity Catalog Integration**:
   - Use 3-level namespace: `catalog.schema.table`
   - Provides lineage, access control, and audit
   - Required for production DLT pipelines

6. **July 2026 UI**:
   - No separate "Workflows" in sidebar — use **Jobs & Pipelines** or **New → ETL Pipeline**
   - Lakeflow Pipelines Editor replaces old notebook + pipeline form workflow
   - `transformations/` = pipeline source code; `explorations/` = setup notebooks, test queries
   - `import dlt` still works despite product rename to "Lakeflow Spark Declarative Pipelines"

---

**Next Steps**: Review `study-notes.md` again, focusing on DLT architecture and pipeline lifecycle. Practice creating DLT pipelines with different expectation types and CDC scenarios.
