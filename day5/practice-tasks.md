# Day 5 — Practice Tasks: DLT, Workflows & CI/CD

> **Environment:** Full production Databricks workspace (Azure / AWS / GCP)
> **Requirements:** Unity Catalog enabled workspace, ADLS Gen2 or S3 storage, DLT-capable tier (Pro or Advanced for CDC tasks)
>
> All tasks below run directly in your production workspace. There are no simulations.

---

## 🛠️ Setup (10 minutes)

### Step 1: Create a test storage path

If you are on Azure, create a container called `dlt-lab` in your ADLS Gen2 account and note:
- Storage account name: `<your-storage-account>`
- Container: `dlt-lab`
- Full path pattern: `abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/`

If you are on AWS, use an S3 path: `s3://your-bucket/dlt-lab/`

### Step 2: Create a Unity Catalog schema for this lab

Run the following in a SQL notebook or the SQL Editor:

```sql
-- Create a lab catalog (skip if using an existing catalog)
CREATE CATALOG IF NOT EXISTS lab_catalog
  COMMENT 'Sandbox catalog for DE associate exam practice';

-- Create a schema for Day 5 work
CREATE SCHEMA IF NOT EXISTS lab_catalog.day5_dlt
  COMMENT 'Day 5 DLT and Workflows practice';

-- Confirm
SHOW SCHEMAS IN lab_catalog;
```

### Step 3: Upload sample landing data

In a Python notebook on any cluster, generate test data in your landing zone:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

# Generate sample orders JSON
orders_data = [
    ("ORD001", "2024-01-15", "C001",  150.00, "completed"),
    ("ORD002", "2024-01-16", "C002",  -50.00, "completed"),  # bad: negative amount
    (None,     "2024-01-17", "C003",   75.00, "pending"),    # bad: null order_id
    ("ORD004", "2024-01-18", "C004",  200.00, "completed"),
    ("ORD005", "2019-12-31", "C005",   90.00, "completed"),  # bad: date too old
    ("ORD006", "2024-01-20", "C001",  310.00, "cancelled"),
    ("ORD007", "2024-02-01", "C002",  450.00, "completed"),
    ("ORD008", "2024-02-03", "C003",  125.00, "completed"),
]

schema = StructType([
    StructField("order_id",    StringType()),
    StructField("order_date",  StringType()),
    StructField("customer_id", StringType()),
    StructField("amount",      DoubleType()),
    StructField("status",      StringType()),
])

df = spark.createDataFrame(orders_data, schema)

# Write to your landing zone as JSON (Auto Loader will pick this up)
LANDING_PATH = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/orders/"
SCHEMA_PATH  = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/checkpoints/orders_schema/"

df.write.mode("overwrite").json(LANDING_PATH)
print(f"Wrote {df.count()} rows to {LANDING_PATH}")
```

---

## Task 1 — Build a Real DLT Pipeline: Bronze → Silver → Gold (45 min)

**Goal:** Create and run a production DLT pipeline with all three medallion layers, data quality expectations, and Unity Catalog as the target.

### Step 1: Create a DLT notebook

Create a new notebook in your Databricks workspace (e.g., `/Repos/<you>/day5-lab/dlt_orders_pipeline`).
Set the default language to **Python**.

Paste and review each cell below — do **not** run this notebook directly. It is attached to a DLT pipeline.

```python
# Cell 1 — Imports
import dlt
from pyspark.sql.functions import col, to_date, current_timestamp

LANDING_PATH = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/orders/"
SCHEMA_PATH  = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/checkpoints/orders_schema/"
```

```python
# Cell 2 — BRONZE: Auto Loader streaming ingest
@dlt.table(
    name    = "raw_orders",
    comment = "Bronze: raw orders ingested from ADLS Gen2 landing zone via Auto Loader",
    table_properties = {
        "quality": "bronze",
        "pipelines.reset.allowed": "false"  # prevent accidental full refresh in prod
    }
)
def raw_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",           "json")
        .option("cloudFiles.inferColumnTypes",  "true")
        .option("cloudFiles.schemaLocation",    SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unknown cols go to _rescued_data
        .load(LANDING_PATH)
    )
```

```python
# Cell 3 — SILVER: Expectations enforce data quality
@dlt.table(
    name    = "clean_orders",
    comment = "Silver: validated and type-cast orders",
    table_properties = {"quality": "silver"}
)
@dlt.expect_or_fail("no_null_order_id",  "order_id IS NOT NULL")      # FAIL  — stops pipeline
@dlt.expect_or_drop("positive_amount",   "amount > 0")                # DROP  — removes bad rows
@dlt.expect("recent_date",               "order_date >= '2020-01-01'") # WARN  — logs, keeps rows
def clean_orders():
    return (
        dlt.read_stream("raw_orders")
        .withColumn("order_date",  to_date(col("order_date")))
        .withColumn("amount",      col("amount").cast("decimal(10,2)"))
        .withColumn("ingested_at", current_timestamp())
        .select("order_id", "order_date", "customer_id", "amount", "status", "ingested_at")
    )
```

```python
# Cell 4 — GOLD: Materialized View for analytics
@dlt.table(
    name    = "customer_order_summary",
    comment = "Gold: per-customer order summary — refreshed on each pipeline run",
    table_properties = {"quality": "gold"}
)
def customer_order_summary():
    # Use dlt.read() (batch read) for the gold layer — no streaming needed
    return (
        dlt.read("clean_orders")
        .where(col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("amount").alias("total_spend"),
            F.max("order_date").alias("last_order_date")
        )
    )
```

### Step 2: Create and configure the DLT pipeline

1. Navigate to **Workflows → Delta Live Tables → Create Pipeline**
2. Fill in:
   - **Pipeline name:** `orders_pipeline_lab`
   - **Product edition:** Advanced
   - **Pipeline mode:** Triggered
   - **Source code:** path to your DLT notebook above
   - **Target schema:** `lab_catalog.day5_dlt`
   - **Storage location:** `abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/dlt_storage/`
   - **Cluster:** 1 driver + 1 worker (minimum for lab)
   - **Photon:** Enable (checkbox)
   - **Channel:** Current
3. Click **Start** to run the pipeline

### Step 3: Verify results

After the pipeline completes, run these queries in the SQL Editor:

```sql
-- Check all three layers exist
SHOW TABLES IN lab_catalog.day5_dlt;

-- Bronze: all 8 rows
SELECT COUNT(*) FROM lab_catalog.day5_dlt.raw_orders;

-- Silver: 6 rows (1 null order_id = FAIL stops pipeline BEFORE this... 
-- so in practice remove the FAIL expectation from clean_orders for this test,
-- or fix the data. Switch to WARN to see DROP-only behavior.)
SELECT * FROM lab_catalog.day5_dlt.clean_orders ORDER BY order_id;

-- Gold: only 'completed' orders grouped by customer
SELECT * FROM lab_catalog.day5_dlt.customer_order_summary ORDER BY total_spend DESC;
```

### Step 4: Inspect the Data Quality tab

In the DLT pipeline UI → **Data Quality** tab:
- You should see pass/fail counts for each expectation
- `positive_amount`: 1 violation (ORD002, amount = -50)
- `recent_date`: 1 warning (ORD005, date in 2019)

**✅ Check:** Can you explain why `dlt.read_stream()` is used in the Silver layer but `dlt.read()` is used in the Gold layer?

---

## Task 2 — CDC with APPLY CHANGES INTO (30 min)

**Goal:** Build a DLT CDC pipeline that maintains current customer state using SCD Type 1 and SCD Type 2.

### Step 1: Generate CDC source data

In a regular Python notebook, write CDC events to ADLS:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# CDC events: id, name, email, operation, updated_at
cdc_data = [
    (1, "Alice",  "alice@a.com",   "INSERT",  "2024-01-01 10:00:00"),
    (2, "Bob",    "bob@b.com",     "INSERT",  "2024-01-01 10:01:00"),
    (1, "Alice",  "alice2@a.com",  "UPDATE",  "2024-01-01 11:00:00"),  # email changed
    (3, "Carol",  "carol@c.com",   "INSERT",  "2024-01-01 11:30:00"),
    (2, None,     None,            "DELETE",  "2024-01-01 12:00:00"),  # Bob deleted
]

cdc_schema = StructType([
    StructField("id",         LongType()),
    StructField("name",       StringType()),
    StructField("email",      StringType()),
    StructField("operation",  StringType()),
    StructField("updated_at", TimestampType()),
])

CDC_PATH = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/cdc_customers/"
df_cdc = spark.createDataFrame(cdc_data, cdc_schema)
df_cdc = df_cdc.withColumn("updated_at", F.to_timestamp("updated_at"))
df_cdc.write.mode("overwrite").json(CDC_PATH)
print(f"Written {df_cdc.count()} CDC events to {CDC_PATH}")
```

### Step 2: Create the CDC DLT notebook

Create a new DLT notebook (`dlt_customers_cdc`):

```python
# Cell 1 — Bronze: ingest CDC events
import dlt
from pyspark.sql.functions import col

CDC_PATH    = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/cdc_customers/"
CHECK_PATH  = "abfss://dlt-lab@<your-storage-account>.dfs.core.windows.net/checkpoints/cdc_schema/"

@dlt.table(comment="Bronze CDC events from source system")
def bronze_customers_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format",        "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", CHECK_PATH)
        .load(CDC_PATH)
    )
```

```python
# Cell 2 — Silver SCD Type 1: current state only (overwrites on UPDATE)
dlt.create_streaming_table(
    name    = "silver_customers_scd1",
    comment = "Current customer state — SCD Type 1 (no history)"
)

dlt.apply_changes(
    target             = "silver_customers_scd1",
    source             = "bronze_customers_cdc",
    keys               = ["id"],
    sequence_by        = col("updated_at"),
    apply_as_deletes   = col("operation") == "DELETE",
    except_column_list = ["operation", "updated_at"],
    stored_as_scd_type = 1
)
```

```python
# Cell 3 — Silver SCD Type 2: full history (inserts new row on UPDATE)
dlt.create_streaming_table(
    name    = "silver_customers_scd2",
    comment = "Customer history — SCD Type 2 (full audit trail via __START_AT / __END_AT)"
)

dlt.apply_changes(
    target             = "silver_customers_scd2",
    source             = "bronze_customers_cdc",
    keys               = ["id"],
    sequence_by        = col("updated_at"),
    apply_as_deletes   = col("operation") == "DELETE",
    except_column_list = ["operation"],
    stored_as_scd_type = 2  # adds __START_AT and __END_AT columns
)
```

### Step 3: Create a new DLT pipeline for CDC

Repeat the pipeline creation from Task 1, but point to the `dlt_customers_cdc` notebook.
After it runs, verify:

```sql
-- SCD1: Alice should have updated email; Bob should be gone
SELECT * FROM lab_catalog.day5_dlt.silver_customers_scd1 ORDER BY id;

-- SCD2: Alice should have TWO rows (original + updated), Bob should be gone
SELECT id, name, email, __START_AT, __END_AT
FROM lab_catalog.day5_dlt.silver_customers_scd2
ORDER BY id, __START_AT;
```

**Expected SCD2 output for Alice:**
```
id | name  | email         | __START_AT          | __END_AT
 1 | Alice | alice@a.com   | 2024-01-01 10:00:00 | 2024-01-01 11:00:00
 1 | Alice | alice2@a.com  | 2024-01-01 11:00:00 | null   <-- current record
```

**✅ Check:** Why does `APPLY CHANGES INTO` require DLT Pro or Advanced edition, and what would you use instead if you needed CDC outside of DLT?

---

## Task 3 — Build a Multi-Task Workflow (30 min)

**Goal:** Create a real Lakeflow Job with task dependencies, task values, and repair run practice.

### Step 1: Create the task notebooks

**Notebook 1: `task_ingest`**
```python
# Simulates ingestion and passes record count to downstream tasks
record_count = spark.table("lab_catalog.day5_dlt.raw_orders").count()

dbutils.jobs.taskValues.set(key="record_count", value=int(record_count))
dbutils.jobs.taskValues.set(key="run_date",     value=str(spark.sql("SELECT current_date()").collect()[0][0]))

print(f"[ingest] Records available: {record_count:,}")
```

**Notebook 2: `task_validate`**
```python
# Reads task value from ingest task and validates
count = dbutils.jobs.taskValues.get(
    taskKey    = "task_ingest",
    key        = "record_count",
    default    = 0,
    debugValue = 999  # used only in interactive runs
)

if count == 0:
    raise ValueError("Validation failed: no records found from ingest task")

print(f"[validate] {count:,} records passed validation")
```

**Notebook 3: `task_report`**
```python
# Reads both task values and prints a run summary
count    = dbutils.jobs.taskValues.get(taskKey="task_ingest", key="record_count", default=0)
run_date = dbutils.jobs.taskValues.get(taskKey="task_ingest", key="run_date",     default="unknown")

print(f"[report] Run date: {run_date} | Records processed: {count:,}")
```

**Notebook 4: `task_alert`**
```python
# This task runs on ANY_FAILED condition — simulates alerting
print("[alert] A pipeline task failed. Sending notification...")
# In production: call a webhook or Databricks SQL Alert here
raise Exception("Alert task triggered — this is expected on failure path")
```

### Step 2: Create the Job

1. Navigate to **Workflows → Create Job**
2. Set the job name: `day5_lab_pipeline`
3. Add tasks in this order with these dependencies:

```
task_ingest → task_validate → task_report
                    ↓
              task_alert  (run condition: AT_LEAST_ONE_FAILED)
```

For each task:
- **Type:** Notebook
- **Cluster:** Job cluster (new cluster per run) with DBR 15.4 LTS
- **Parameters:** none needed (task values are used instead)

4. Add a **schedule**: every weekday at 06:00 Europe/Berlin
   - Quartz cron: `0 0 6 ? * MON-FRI *`
   - Timezone: `Europe/Berlin`

### Step 3: Test Repair Run

1. Temporarily introduce a failure in `task_validate` (e.g., change `count == 0` to `count > 0`)
2. Run the job manually — task_validate should fail, task_report should be skipped
3. Fix the notebook
4. In the job run history, click **Repair Run**
5. Observe that only task_validate and task_report re-run — task_ingest is skipped

**✅ Check:** What is the difference between `AT_LEAST_ONE_FAILED` and `ALL_DONE` task run conditions? Give an example of when you would use each.

---

## Task 4 — Databricks Asset Bundles (DABs) Hands-On (30 min)

**Goal:** Create, deploy, and run a real DABs bundle using the Databricks CLI v2.

### Step 1: Install the Databricks CLI v2

```bash
# macOS (Homebrew)
brew tap databricks/tap
brew install databricks

# Linux / WSL
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify
databricks --version
# Expected: Databricks CLI v0.200+
```

### Step 2: Authenticate

```bash
# OAuth (recommended for human users)
databricks auth login --host https://adb-<workspace-id>.azuredatabricks.net

# Or PAT (for CI/CD service principals)
export DATABRICKS_HOST=https://adb-<workspace-id>.azuredatabricks.net
export DATABRICKS_TOKEN=<your-pat>
```

### Step 3: Initialize a bundle

```bash
mkdir day5-bundle && cd day5-bundle
databricks bundle init
# Choose: default-python template
# Enter project name: day5_orders_lab
```

### Step 4: Edit `databricks.yml`

Replace the generated content with:

```yaml
bundle:
  name: day5_orders_lab

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net

  prod:
    mode: production
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
    run_as:
      # Replace with your service principal application ID
      service_principal_name: sp-day5-prod@company.com

resources:
  jobs:
    day5_orders_job:
      name: day5_orders_lab_${bundle.target}
      schedule:
        quartz_cron_expression: "0 0 6 ? * MON-FRI *"
        timezone_id: "Europe/Berlin"
        pause_status: PAUSED  # safe default — don't auto-run on deploy
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/notebooks/task_ingest.py
          job_cluster_key: main_cluster
        - task_key: validate
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./src/notebooks/task_validate.py
          job_cluster_key: main_cluster
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id:  "Standard_DS3_v2"   # Azure; change for AWS/GCP
            num_workers:   2
            spark_conf:
              spark.databricks.delta.preview.enabled: "true"
```

### Step 5: Deploy and run

```bash
# Validate the bundle config before deploying
databricks bundle validate --target dev

# Deploy to dev (creates/updates the job in the workspace)
databricks bundle deploy --target dev

# Confirm the job appears in the UI, then run it
databricks bundle run --target dev day5_orders_job

# Check run status
databricks bundle run --target dev day5_orders_job --no-wait
```

### Step 6: Compare dev vs prod deployment

```bash
# Deploy to prod (uses mode: production — note the run_as service principal)
databricks bundle deploy --target prod

# Observe in the UI: prod job has [dev <you>] prefix REMOVED
# In dev: "[dev yuhao] day5_orders_lab_dev"
# In prod: "day5_orders_lab_prod"
```

**✅ Check questions:**
1. What does `mode: development` add to the job name, and why?
2. Why is `pause_status: PAUSED` a safe default for scheduled jobs on deploy?
3. What is the difference between `databricks bundle deploy` and `databricks bundle run`?

```python
# Answers
# 1. mode: development prepends "[dev <username>]" to resource names to namespace
#    dev deployments from prod — multiple developers can deploy without overwriting each other
# 2. PAUSED prevents the schedule from firing automatically on first deploy,
#    giving you a chance to validate before enabling
# 3. deploy = create/update the job definition in the workspace
#    run    = trigger an immediate run of the deployed job
print("Answers confirmed")
```

---

## Task 5 — DLT Event Log Monitoring (20 min)

**Goal:** Query the DLT event log to monitor pipeline health and expectation metrics.

### Step 1: Get your pipeline ID

In the DLT UI for your `orders_pipeline_lab` pipeline:
- Settings → copy the **Pipeline ID** (format: `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`)

### Step 2: Query the event log

In a SQL notebook:

```sql
-- Replace <pipeline-id> with your actual pipeline ID

-- 1. View all recent events
SELECT timestamp, event_type, level, message
FROM event_log("<pipeline-id>")
ORDER BY timestamp DESC
LIMIT 50;

-- 2. Show only errors and warnings
SELECT timestamp, level, message
FROM event_log("<pipeline-id>")
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC;

-- 3. Expectation pass/fail counts per run
SELECT
  timestamp,
  details:flow_progress.metrics.num_output_rows                      AS output_rows,
  details:flow_progress.data_quality.expectations[0].name           AS expectation_name,
  details:flow_progress.data_quality.expectations[0].passed_records AS passed,
  details:flow_progress.data_quality.expectations[0].failed_records AS failed
FROM event_log("<pipeline-id>")
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC;
```

### Step 3: Set up a DBSQL Alert

1. Save the error query above as a named query in Databricks SQL: `dlt_errors_last_24h`
2. Navigate to **SQL → Alerts → New Alert**
3. Attach query: `dlt_errors_last_24h`
4. Condition: `Value > 0`
5. Set a notification destination (email or Slack)
6. Schedule: every 1 hour

**✅ Check:** What SQL function do you use to query a DLT pipeline's event log, and what information can you extract from it?

---

## Task 6 — Photon & Serverless Compute (15 min — Hands-On Verification)

**Goal:** Verify Photon is active on your cluster and observe the Spark UI difference.

### Step 1: Check if Photon is enabled

```python
# In a notebook on a Photon-enabled cluster (DBR with Photon runtime)
print(spark.conf.get("spark.databricks.photon.enabled"))
# Expected: true

# Run a SQL aggregation and check SparkUI for Photon nodes
df = spark.range(10_000_000).toDF("id")
result = df.groupBy((df.id % 100).alias("bucket")).count()
result.show(5)

# In Spark UI → SQL/DataFrame tab → click the query plan
# Look for 'WholeStageCodegenTransformer' nodes — these are Photon-executed
```

### Step 2: Test Python UDF limitation

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python UDF — bypasses Photon entirely
@udf(returnType=StringType())
def classify_amount(amount):
    if amount > 300: return "high"
    elif amount > 100: return "medium"
    else: return "low"

df_orders = spark.table("lab_catalog.day5_dlt.clean_orders")

# With Python UDF (no Photon benefit on the UDF step)
df_orders.withColumn("tier", classify_amount("amount")).show()

# Native SQL equivalent (Photon-accelerated)
from pyspark.sql.functions import when
df_orders.withColumn(
    "tier",
    when(col("amount") > 300, "high")
    .when(col("amount") > 100, "medium")
    .otherwise("low")
).show()

# Open Spark UI and compare the two query plans
# The native version will show Photon nodes; the UDF version will not
```

**✅ Check:** How do you identify in the Spark UI whether a query was executed by Photon?

---

## Task 7 — Unity Catalog Governance (20 min)

**Goal:** Practice GRANT/REVOKE, column masking, and verify lineage tracking.

```sql
-- Step 1: Grant access to a group
GRANT USE CATALOG ON CATALOG lab_catalog     TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  lab_catalog.day5_dlt TO `analysts`;
GRANT SELECT      ON TABLE   lab_catalog.day5_dlt.customer_order_summary TO `analysts`;

-- Step 2: Check what permissions exist on a table
SHOW GRANTS ON TABLE lab_catalog.day5_dlt.customer_order_summary;

-- Step 3: Create a column mask for sensitive data (email in customers CDC)
CREATE OR REPLACE FUNCTION lab_catalog.day5_dlt.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-access') THEN email
  ELSE regexp_replace(email, '(^[^@]+)', '***')
END;

-- Apply the mask to silver_customers_scd1
ALTER TABLE lab_catalog.day5_dlt.silver_customers_scd1
  ALTER COLUMN email SET MASK lab_catalog.day5_dlt.mask_email;

-- Verify: users not in 'pii-access' group will see ***@a.com
SELECT id, name, email FROM lab_catalog.day5_dlt.silver_customers_scd1;

-- Step 4: Check lineage in the Unity Catalog UI
-- Navigate to: Catalog Explorer → lab_catalog → day5_dlt → clean_orders → Lineage tab
-- You should see: raw_orders (source) → clean_orders → customer_order_summary (downstream)

-- Step 5: Revoke access
REVOKE SELECT ON TABLE lab_catalog.day5_dlt.customer_order_summary FROM `analysts`;
```

**✅ Check:** What is the difference between a Unity Catalog **column mask** and a **row filter**? Give a use case for each.

---

## Task 8 — Exam-Style Quiz (15 min)

**Q1.** In a DLT pipeline, you define:
```sql
CREATE OR REFRESH MATERIALIZED VIEW orders_clean
  CONSTRAINT positive_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
AS SELECT * FROM LIVE.orders_raw
```
What happens when a row with `amount = -5` arrives?
- A) The pipeline fails
- B) The row is logged as a warning and kept
- **C) The row is silently removed from the output** ✅
- D) The pipeline pauses

**Q2.** A 6-task Workflows job: tasks 1–3 succeeded, task 4 failed, tasks 5–6 were skipped. You click **Repair Run**. Which is correct?
- A) All 6 tasks run from the beginning
- B) Only task 4 re-runs; tasks 5–6 remain skipped
- **C) Tasks 4, 5, and 6 re-run; tasks 1–3 are not re-run** ✅
- D) Only failed tasks re-run; skipped tasks require a full new run

**Q3.** In a DLT notebook, you want to read from another streaming table within the same pipeline. Which function do you use?
- A) `spark.readStream("table_name")`
- B) `dlt.read("table_name")`
- **C) `dlt.read_stream("table_name")` ✅**
- D) `spark.table("LIVE.table_name")`

**Q4.** Which DLT table type is **not** physically stored as a Delta table?
- A) Streaming Table
- B) Materialized View
- **C) Live View** ✅
- D) Both A and B

**Q5.** In Databricks Asset Bundles, what does `mode: production` do?
- **A) Provisions a new isolated cluster per run and enables auto-retry on failure** ✅
- B) Deploys to the production workspace URL automatically
- C) Enables row-level security on all tables
- D) Activates Unity Catalog governance

**Q6.** `APPLY CHANGES INTO` requires which DLT edition minimum?
- A) Core
- **B) Pro** ✅
- C) Advanced
- D) Standard

**Q7.** Which statement about Photon is correct?
- A) Photon accelerates Python UDFs by compiling them to C++
- B) Photon requires code changes to activate
- **C) Photon is enabled per cluster and accelerates SQL/DataFrame workloads without code changes** ✅
- D) Photon is available in open-source Apache Spark

---

## ✅ Day 5 Completion Checklist

- [ ] Successfully ran a 3-layer DLT pipeline (Bronze/Silver/Gold) in your production workspace
- [ ] Observed expectation violations in the DLT Data Quality tab
- [ ] Ran a CDC pipeline with `APPLY CHANGES INTO` and verified SCD1 vs SCD2 output
- [ ] Built a multi-task Workflow with task values and practiced Repair Run
- [ ] Deployed a DABs bundle with the new CLI (`databricks bundle deploy`)
- [ ] Queried the DLT event log for expectation metrics
- [ ] Verified Photon is enabled and compared Python UDF vs native SQL in Spark UI
- [ ] Practiced GRANT/REVOKE and column masking in Unity Catalog
- [ ] Scored 7/7 on the exam-style quiz

## 🔗 Additional Resources

- [DLT Quickstart (Official)](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-quickstart.html)
- [APPLY CHANGES INTO (CDC)](https://docs.databricks.com/workflows/delta-live-tables/cdc.html)
- [DABs Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI v2 Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [Unity Catalog Privileges Reference](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
