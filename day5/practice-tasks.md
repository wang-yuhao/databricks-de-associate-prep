# Day 5 — Practice Tasks: DLT, Workflows & CI/CD

> **Environment:** Databricks Community Edition (free) at [community.cloud.databricks.com](https://community.cloud.databricks.com)
> 
> ⚠️ **Note:** DLT is not available in Community Edition. Use the notebook simulation tasks (Tasks 1-3) to practice DLT syntax. For real DLT testing, use a 14-day free trial workspace.

---

## 🛠️ Setup (5 minutes)

1. Open your Community Edition workspace
2. Create a new cluster (Runtime 13.3 LTS or higher)
3. Create a new notebook: `day5_dlt_workflows`
4. Set language to Python

---

## Task 1 — DLT Syntax Validation (30 min)

**Goal:** Practice writing DLT-style code and understand the patterns.

**Step 1:** Create a notebook that simulates what a DLT pipeline would do, using regular Spark (since DLT isn't available in Community Edition):

```python
# === SIMULATE DLT PIPELINE IN REGULAR SPARK ===
# In a real DLT pipeline, replace spark.read/write with @dlt.table decorators

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import date

# ─── Sample data mimicking raw ingestion ───
raw_data = [
    ("ORD001", "2024-01-15", "C001", 150.00, "completed"),
    ("ORD002", "2024-01-16", "C002", -50.00, "completed"),   # bad: negative amount
    (None,    "2024-01-17", "C003",  75.00, "pending"),     # bad: null order_id
    ("ORD004", "2024-01-18", "C004",  200.00, "completed"),
    ("ORD005", "2019-12-31", "C005",  90.00, "completed"),   # bad: date too old
    ("ORD006", "2024-01-20", "C001",  310.00, "cancelled"),
]

schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_date", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

df_raw = spark.createDataFrame(raw_data, schema)
print("=== Raw Bronze data ===")
df_raw.show()
```

**Step 2:** Apply DLT-style expectations (simulate with filter logic):

```python
# ─── Simulate DLT Expectations ───
# In DLT: @dlt.expect_or_drop("valid_amount", "amount > 0")
# In DLT: @dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
# In DLT: @dlt.expect("valid_date", "order_date >= '2020-01-01'")  -- WARN only

# Count violations before dropping
violations_amount = df_raw.filter(~(F.col("amount") > 0)).count()
violations_null   = df_raw.filter(F.col("order_id").isNull()).count()
violations_date   = df_raw.filter(F.col("order_date") < "2020-01-01").count()

print(f"Expectation 'valid_amount' violations (DROP): {violations_amount}")
print(f"Expectation 'valid_order_id' violations (DROP): {violations_null}")
print(f"Expectation 'valid_date' violations (WARN): {violations_date}")

# Silver layer — apply DROP rules, keep WARN rows but log
df_silver = (
    df_raw
    .filter(F.col("order_id").isNotNull())      # DROP rule
    .filter(F.col("amount") > 0)               # DROP rule
    .withColumn("order_date", F.to_date(F.col("order_date")))
)

print("\n=== Silver layer after applying expectations ===")
df_silver.show()
print(f"Rows before: {df_raw.count()}, Rows after: {df_silver.count()}")
```

**Step 3:** Write DLT-style SQL for the same pipeline:

```sql
-- In a DLT notebook (SQL), this would be:

-- Bronze
-- CREATE OR REFRESH STREAMING TABLE raw_orders
-- COMMENT 'Raw orders from landing zone'
-- AS SELECT * FROM cloud_files("/mnt/landing/orders", "json",
--   map("cloudFiles.inferColumnTypes", "true"))

-- Silver
-- CREATE OR REFRESH MATERIALIZED VIEW silver_orders
--   CONSTRAINT valid_amount   EXPECT (amount > 0)              ON VIOLATION DROP ROW,
--   CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)    ON VIOLATION DROP ROW,
--   CONSTRAINT recent_date    EXPECT (order_date >= '2020-01-01')
-- AS SELECT
--   order_id,
--   CAST(order_date AS DATE) AS order_date,
--   customer_id,
--   amount,
--   status
-- FROM LIVE.raw_orders

-- Gold
-- CREATE LIVE VIEW customer_summary
-- AS SELECT
--   customer_id,
--   COUNT(*) AS total_orders,
--   SUM(amount) AS total_spend,
--   MAX(order_date) AS last_order_date
-- FROM LIVE.silver_orders
-- WHERE status = 'completed'
-- GROUP BY customer_id
```

**✅ Check:** Can you explain the difference between STREAMING TABLE vs MATERIALIZED VIEW vs LIVE VIEW?

---

## Task 2 — DLT CDC Pattern (20 min)

**Goal:** Understand the `APPLY CHANGES INTO` CDC pattern.

```python
# Simulate CDC (Change Data Feed) processing
# In DLT, APPLY CHANGES INTO handles this automatically

# Source: CDC events (inserts, updates, deletes)
cdc_events = [
    (1, "Alice",  "alice@a.com",  "INSERT",  "2024-01-01 10:00:00"),
    (2, "Bob",    "bob@b.com",    "INSERT",  "2024-01-01 10:01:00"),
    (1, "Alice",  "alice2@a.com", "UPDATE",  "2024-01-01 11:00:00"),  # email changed
    (3, "Carol",  "carol@c.com",  "INSERT",  "2024-01-01 11:30:00"),
    (2, None,     None,           "DELETE",  "2024-01-01 12:00:00"),  # Bob deleted
]

from pyspark.sql.types import LongType, TimestampType

cdc_schema = "id LONG, name STRING, email STRING, operation STRING, event_time TIMESTAMP"
df_cdc = spark.createDataFrame(cdc_events).toDF("id", "name", "email", "operation", "event_time")

print("=== CDC Events Stream ===")
df_cdc.show(truncate=False)

# Simulate APPLY CHANGES INTO logic:
# 1. Rank rows by event_time per id
# 2. Keep only the latest record per id
# 3. Remove DELETEs from output
from pyspark.sql.window import Window

window = Window.partitionBy("id").orderBy(F.desc("event_time"))
df_latest = (
    df_cdc
    .withColumn("rank", F.rank().over(window))
    .filter(F.col("rank") == 1)
    .filter(F.col("operation") != "DELETE")
    .select("id", "name", "email", "event_time")
)

print("=== Resulting Target Table (after CDC merge) ===")
df_latest.show()
```

**DLT CDC SQL (for reference):**
```sql
-- In a real DLT pipeline:
-- CREATE OR REFRESH STREAMING TABLE customers;

-- APPLY CHANGES INTO LIVE.customers
-- FROM STREAM(LIVE.cdc_events)
-- KEYS (id)
-- APPLY AS DELETE WHEN operation = 'DELETE'
-- SEQUENCE BY event_time
-- COLUMNS * EXCEPT (operation, event_time)
```

---

## Task 3 — Workflows Simulation (20 min)

**Goal:** Understand how to structure multi-task jobs.

**Exercise — Design a Job:**

You have these tasks in a data pipeline:
1. `ingest_raw` — loads CSV files from landing zone
2. `validate_schema` — checks schema correctness
3. `transform_silver` — cleans and enriches data
4. `aggregate_gold` — builds aggregate tables
5. `send_report` — sends email with row counts
6. `notify_failure` — sends alert if any step fails

**Draw the DAG (write it out):**
```
# Your answer:
# ingest_raw → validate_schema → transform_silver → aggregate_gold → send_report
#                    ↓                                        ↓
#             notify_failure                          notify_failure
# (run_if: AT_LEAST_ONE_FAILED for notify_failure from any upstream)
```

**Step 2 — Understand task values:**

```python
# Task 1: ingest_raw — counts records and passes to next task
def ingest_raw():
    # Simulate ingestion
    record_count = 5000
    dbutils.jobs.taskValues.set(key="record_count", value=record_count)
    print(f"Ingested {record_count} records")
    return record_count

# Run it
count = ingest_raw()

# Task 2: validate_schema — picks up from task 1
# In a real workflow, this would be:
# count = dbutils.jobs.taskValues.get(taskKey="ingest_raw", key="record_count", default=0)
# For testing purposes:
count = 5000

if count == 0:
    raise ValueError("No records ingested — aborting pipeline")

print(f"Validation passed: {count} records to process")
```

**Step 3 — Repair run concept quiz:**
```
Q: A 5-task job has tasks: [A → B → C → D → E]
   Tasks A, B, C succeeded. Task D failed. Task E was skipped.
   
   If you do a Repair Run:
   - Which tasks re-run? D and E
   - Which tasks are skipped? A, B, C (already succeeded)
   - What data is preserved? All outputs from A, B, C
```

---

## Task 4 — DABs YAML Comprehension (15 min)

**Goal:** Read and understand a DABs configuration.

**Exercise — Read this bundle YAML and answer questions:**

```yaml
bundle:
  name: retail_analytics

variables:
  env:
    description: Deployment environment
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev.azuredatabricks.net
    variables:
      env: dev

  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net
    variables:
      env: prod
    run_as:
      service_principal_name: sp-retail-prod@company.com

resources:
  jobs:
    daily_pipeline:
      name: retail_daily_${var.env}
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "Europe/Berlin"
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/ingest.py
          job_cluster_key: main_cluster
        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./src/transform.py
          job_cluster_key: main_cluster
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            num_workers: 2
```

**Questions (answer in your notebook as comments):**
1. What time does this job run, and in which timezone?
2. In `dev` mode, does it create a new cluster or reuse an existing one?
3. What user/principal runs the job in production?
4. What is the dependency between `ingest` and `transform`?
5. How would you deploy this to production? (CLI command)

**Answers:**
```python
# 1. Every day at 06:00 CET (Europe/Berlin timezone)
# 2. In development mode, Databricks reuses existing clusters for faster iteration
#    In production mode, a NEW cluster is created per run for isolation
# 3. Service principal: sp-retail-prod@company.com
# 4. transform depends on ingest — ingest must succeed before transform starts
# 5. databricks bundle deploy --target prod
print("Answers confirmed!")
```

---

## Task 5 — Knowledge Check Quiz (15 min)

Answer these exam-style questions. Check answers at the bottom.

**Q1.** In a DLT pipeline, you define:
```sql
CREATE OR REFRESH MATERIALIZED VIEW orders_clean
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
AS SELECT * FROM LIVE.orders_raw
```
What happens when a row with `amount = -5` arrives?
- A) The pipeline fails
- B) The row is logged as a warning and kept
- C) The row is removed from the output
- D) The pipeline pauses

**Q2.** You have a 6-task Workflows job. Tasks 1-3 succeeded, task 4 failed, tasks 5-6 were skipped. You click "Repair Run". Which statement is correct?
- A) All 6 tasks run from the beginning
- B) Only task 4 re-runs; tasks 5-6 remain skipped
- C) Tasks 4, 5, and 6 re-run; tasks 1-3 are not re-run
- D) Only failed tasks re-run; skipped tasks require a full new run

**Q3.** In a DLT notebook, you want to read from a streaming source within a pipeline. Which function do you use?
- A) `spark.readStream("table_name")`
- B) `dlt.read("table_name")`
- C) `dlt.read_stream("table_name")`
- D) `spark.table("LIVE.table_name")`

**Q4.** Which DLT table type is NOT physically stored as a Delta table?
- A) Streaming Table
- B) Materialized View
- C) Live View
- D) Both A and B

**Q5.** In Databricks Asset Bundles, what does `mode: production` affect?
- A) Uses a new isolated cluster per run and enables auto-retry on failure
- B) Deploys to the production workspace URL automatically
- C) Enables row-level security on all tables
- D) Activates Unity Catalog governance

```python
# ─── Answers ───
print("Q1: C — ON VIOLATION DROP ROW removes the bad row")
print("Q2: C — Repair Run re-runs failed + downstream skipped tasks")
print("Q3: C — dlt.read_stream() for streaming sources within DLT")
print("Q4: C — Live Views are computed on-the-fly, not stored")
print("Q5: A — production mode creates new clusters and enables retry")
```

---

## ✅ Day 5 Completion Checklist

- [ ] Can explain difference between DLT Streaming Table, Materialized View, and Live View
- [ ] Know all 3 DLT expectation modes (WARN / DROP / FAIL)
- [ ] Understand `APPLY CHANGES INTO` for CDC
- [ ] Know all Workflows task types
- [ ] Understand `dbutils.jobs.taskValues` for passing data between tasks
- [ ] Know what Repair Run does
- [ ] Can read and interpret a DABs YAML file
- [ ] Know `mode: development` vs `mode: production` differences
- [ ] Completed all 5 tasks above

## 🔗 Additional Practice
- [DLT Quickstart (Official)](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-quickstart.html)
- [CertSafari — DLT Questions](https://www.certsafari.com/databricks/data-engineer-associate) (filter by topic)
- [ExamTopics DLT Thread](https://www.examtopics.com/discussions/databricks/)


---

## Task 6 — Photon Engine & Serverless Compute (20 min — Conceptual)

### Photon Engine

Photon is a Databricks-native vectorized query engine written in C++ that accelerates SQL and DataFrame workloads.

**Key facts for the exam:**
- Photon is automatically enabled on clusters with Photon-capable runtimes (DBR with Photon suffix)
- It accelerates SQL queries, aggregations, joins, and scans
- No code changes needed — it transparently replaces Spark's execution engine
- Most beneficial for: large-scale aggregations, joins, and scans on columnar data
- NOT available in Community Edition — requires paid workspace

```python
# Cell: Check if Photon is enabled (in a paid workspace)
# In Community Edition, this will show photon = false
spark.conf.get("spark.databricks.photon.enabled")
```

**When to use Photon:**
- BI/SQL workloads with large scans
- ETL jobs with heavy aggregations
- NOT needed for streaming-only or ML training workloads

---

### Serverless Compute

Serverless Compute removes the need to manage cluster infrastructure. Databricks automatically provisions and scales compute.

**Key facts for the exam:**
- Serverless SQL Warehouses: for SQL analytics / BI dashboards (auto-scaling, instant start)
- Serverless Jobs: for running notebooks and workflows without cluster management
- Billing: per-second billing based on DBUs consumed (no idle cost)
- Cold start: nearly instant (< 5 seconds vs 5+ minutes for classic clusters)

**Classic Cluster vs Serverless:**

| Feature | Classic Cluster | Serverless |
|---|---|---|
| Startup time | 5-10 minutes | < 5 seconds |
| Management | Manual config | Fully managed |
| Scaling | Manual/auto-scale | Automatic |
| Idle cost | Billed when idle | No idle cost |
| Availability | Community Edition | Paid workspaces |

```python
# Conceptual Exercise: Answer these questions

questions = [
    "Q1: Which compute type has nearly instant startup?",
    "Q2: Which runtime accelerates SQL without code changes?",
    "Q3: Is Photon available in Community Edition?",
    "Q4: What is the billing model for Serverless?",
    "Q5: When is Photon NOT beneficial?"
]

answers = [
    "A1: Serverless Compute (< 5 second cold start)",
    "A2: Photon Engine (transparent acceleration)",
    "A3: No — Photon requires a paid Databricks workspace",
    "A4: Per-second billing on DBUs consumed, no idle cost",
    "A5: Streaming-only workloads and ML training (less columnar scan benefit)"
]

for q, a in zip(questions, answers):
    print(q)
    print(a)
    print()
```

---

## Task 7 — Unity Catalog Basics (20 min)

Unity Catalog is Databricks' unified governance solution for data and AI.

**Three-level namespace:** `catalog.schema.table`

```sql
-- The three-level namespace
-- catalog = top level (e.g., "main", "dev", "prod")
-- schema  = database (e.g., "sales", "marketing")
-- table   = the actual table

-- Example:
SELECT * FROM main.sales.transactions;

-- In Community Edition, use hive_metastore as catalog:
SELECT * FROM hive_metastore.default.my_table;
```

```sql
-- Create a catalog (requires Unity Catalog enabled workspace)
-- CREATE CATALOG IF NOT EXISTS my_catalog;

-- Create a schema within a catalog
-- CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

-- Grant permissions (Unity Catalog)
-- GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `user@company.com`;
-- GRANT USAGE ON SCHEMA my_catalog.my_schema TO `analyst_group`;
```

**Key Unity Catalog concepts for the exam:**
- `GRANT` / `REVOKE` for access control
- Row-level and column-level security via dynamic views
- Data lineage: automatically tracked across notebooks, jobs, and SQL queries
- Metastore: one per region, shared across workspaces

```python
# Conceptual Quiz
uc_facts = {
    "Namespace levels": "catalog.schema.table (3 levels)",
    "Default catalog in CE": "hive_metastore",
    "Grant syntax": "GRANT privilege ON object TO principal",
    "Lineage tracking": "Automatic — no extra config needed",
    "Metastore scope": "Per region, shared across workspaces"
}

for concept, fact in uc_facts.items():
    print(f"{concept}: {fact}")
```

---

## ✅ Day 5 Extended Checklist

- [ ] Understand what Photon Engine does and when to use it
- [ ] Know the difference between Classic Cluster and Serverless
- [ ] Can explain the Unity Catalog three-level namespace
- [ ] Know how to GRANT permissions in Unity Catalog
- [ ] Understand that Photon is NOT available in Community Edition
