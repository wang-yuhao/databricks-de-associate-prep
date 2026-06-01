# Day 5 — Practice Tasks: Lakeflow Spark Declarative Pipelines, Lakeflow Jobs & CI/CD

> **Exam version:** Aligned with the **Databricks Certified Data Engineer Associate** exam guide effective **May 4, 2026** and refreshed against Databricks documentation current through May 2026.[1][2][3][4][5]
>
> Key terminology changes in this version:
> - **Delta Live Tables (DLT)** → **Lakeflow Spark Declarative Pipelines (LDP / SDP)**.[1][2]
> - **Workflows / Jobs** → **Lakeflow Jobs**.[6][7]
> - **Databricks Asset Bundles (DABs)** → **Declarative Automation Bundles (DABs)**.[5]
> - **AUTO CDC / AUTO CDC INTO** is the current Lakeflow wording for CDC flows; older `APPLY CHANGES INTO` phrasing may still appear in notebooks and community content.[8][9]
>
> **Environment:** Full production Databricks workspace (Azure / AWS / GCP).  
> **Requirements:** Unity Catalog enabled workspace, external cloud storage, and a workspace tier that supports the required Lakeflow features.[10][11][2]
>
> **Important compatibility note:** You may still see `import dlt` in Python examples and `APPLY CHANGES INTO` in older study material. For exam and interview prep, map these to the current Lakeflow product names rather than assuming the concepts changed.[1][12][9]

All tasks below run directly in your production workspace. There are no simulations.

***

## 🛠️ Setup (10 minutes)

### Step 1: Create a test storage path

If you are on **Azure**, create a container called `ldp-lab` in your ADLS Gen2 account and note:

- Storage account name: `<your-storage-account>`
- Container: `ldp-lab`
- Full path pattern: `abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/`

If you are on **AWS**, use an S3 path: `s3://your-bucket/ldp-lab/`

### Step 2: Create a Unity Catalog schema for this lab

Run the following in a SQL notebook or the SQL Editor:

```sql
-- Create a lab catalog (skip if using an existing catalog)
CREATE CATALOG IF NOT EXISTS lab_catalog
  COMMENT 'Sandbox catalog for DE associate exam practice';

-- Create a schema for Day 5 work
CREATE SCHEMA IF NOT EXISTS lab_catalog.day5_ldp
  COMMENT 'Day 5 Lakeflow Spark Declarative Pipelines, Lakeflow Jobs, and CI/CD practice';

-- Confirm
SHOW SCHEMAS IN lab_catalog;
```

### Step 3: Upload sample landing data

In a Python notebook on any cluster, generate test data in your landing zone:

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

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

LANDING_PATH = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/orders/"
SCHEMA_PATH  = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/checkpoints/orders_schema/"

df.write.mode("overwrite").json(LANDING_PATH)
print(f"Wrote {df.count()} rows to {LANDING_PATH}")
```

***

## Task 1 — Build a Lakeflow Spark Declarative Pipeline: Bronze → Silver → Gold (45 min)

**Goal:** Create and run a production LDP pipeline with all three Medallion Architecture layers, data quality expectations, and Unity Catalog as the target.

> **Exam note:** Databricks now calls this feature **Lakeflow Spark Declarative Pipelines (LDP)**. You may still see `import dlt` in code — the Python library name has not changed, but the product name has. The exam tests knowledge of LDP concepts under the new terminology.

### Step 1: Create an LDP notebook

Create a new notebook in your Databricks workspace (e.g., `/Repos/<you>/day5-lab/ldp_orders_pipeline`).
Set the default language to **Python**.

Paste and review each cell below — do **not** run this notebook directly. It is attached to an LDP pipeline.

```python
# Cell 1 — Imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, current_timestamp

LANDING_PATH = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/orders/"
SCHEMA_PATH  = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/checkpoints/orders_schema/"
```

```python
# Cell 2 — BRONZE: Auto Loader streaming ingest (Streaming Table)
@dlt.table(
    name    = "raw_orders",
    comment = "Bronze: raw orders ingested from ADLS Gen2 landing zone via Auto Loader",
    table_properties = {
        "quality": "bronze",
        "pipelines.reset.allowed": "false"   # prevent accidental full refresh in prod
    }
)
def raw_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",             "json")
        .option("cloudFiles.inferColumnTypes",    "true")
        .option("cloudFiles.schemaLocation",      SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unknown cols → _rescued_data
        .load(LANDING_PATH)
    )
```

```python
# Cell 3 — SILVER: Expectations enforce data quality (Streaming Table)
@dlt.table(
    name    = "clean_orders",
    comment = "Silver: validated and type-cast orders",
    table_properties = {"quality": "silver"}
)
@dlt.expect_or_fail("no_null_order_id",  "order_id IS NOT NULL")       # FAIL  — stops pipeline
@dlt.expect_or_drop("positive_amount",   "amount > 0")                 # DROP  — removes bad rows
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
    # dlt.read() = batch read; appropriate for Gold aggregations (no streaming needed)
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

### Step 2: Create and configure the LDP pipeline

> **UI note (2026):** In current workspaces, the entry point is typically **Jobs & Pipelines** in the left sidebar, then **Create** → **Pipeline**. Older screenshots may say “Delta Live Tables” or “Declarative Pipelines,” but the current product name is Lakeflow Spark Declarative Pipelines.[1][2][3]

1. In the left sidebar, open **Jobs & Pipelines**.[3][7]
2. Click **Create** → **Pipeline**.[3]
3. In the pipeline editor or create form, fill in:
   - **Pipeline name:** `orders_pipeline_lab`
   - **Pipeline mode:** **Triggered** for this lab
   - **Source code / Notebook:** your `ldp_orders_pipeline` notebook
   - **Target schema:** `lab_catalog.day5_ldp`
   - **Storage location:** a managed or external path dedicated to the pipeline, for example `abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/ldp_storage/`
   - **Compute:** **Serverless** if available; otherwise use an appropriate classic configuration
   - **Channel:** **Current** unless you are explicitly testing preview functionality[2][13]
4. Save the pipeline, then click **Start** or **Run pipeline** to execute it.[2]

> **Current best practice:** Use a dedicated storage location for pipeline state and avoid reusing landing paths for checkpoints or pipeline metadata.[14][15]

### Step 3: Verify results

After the pipeline completes, run these queries in the SQL Editor:

```sql
-- Check all three layers exist
SHOW TABLES IN lab_catalog.day5_ldp;

-- Bronze: all 8 rows
SELECT COUNT(*) FROM lab_catalog.day5_ldp.raw_orders;

-- Silver: rows that pass expectations
-- Note: the FAIL expectation on null order_id stops the pipeline if triggered.
-- For testing DROP behaviour: change expect_or_fail to expect_or_drop.
SELECT * FROM lab_catalog.day5_ldp.clean_orders ORDER BY order_id;

-- Gold: only 'completed' orders grouped by customer
SELECT * FROM lab_catalog.day5_ldp.customer_order_summary ORDER BY total_spend DESC;
```

### Step 4: Inspect the Data Quality tab

In the LDP pipeline UI → **Data Quality** tab:

- Pass/fail counts are shown per expectation
- `positive_amount`: 1 violation (ORD002, amount = -50) → dropped
- `recent_date`: 1 warning (ORD005, date in 2019) → kept with a warning logged

**✅ Check:** Explain the three LDP expectation modes (`expect`, `expect_or_drop`, `expect_or_fail`) and which Gold layer object type (Materialized View vs. Streaming Table vs. View) is most appropriate for the aggregation above.

***

## Task 2 — CDC with APPLY CHANGES INTO (30 min)

**Goal:** Build an LDP CDC pipeline that maintains current customer state using SCD Type 1 and SCD Type 2.

### Step 1: Generate CDC source data

In a regular Python notebook, write CDC events to cloud storage:

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
    StructField("updated_at", StringType()),
])

CDC_PATH   = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/cdc_customers/"
df_cdc = spark.createDataFrame(cdc_data, cdc_schema)
df_cdc = df_cdc.withColumn("updated_at", F.to_timestamp("updated_at"))
df_cdc.write.mode("overwrite").json(CDC_PATH)
print(f"Written {df_cdc.count()} CDC events to {CDC_PATH}")
```

### Step 2: Create the CDC LDP notebook

Create a new LDP notebook (`ldp_customers_cdc`):

```python
# Cell 1 — Bronze: ingest CDC events (Streaming Table)
import dlt
from pyspark.sql.functions import col

CDC_PATH   = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/cdc_customers/"
CHECK_PATH = "abfss://ldp-lab@<your-storage-account>.dfs.core.windows.net/checkpoints/cdc_schema/"

@dlt.table(comment="Bronze CDC events from source system")
def bronze_customers_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format",          "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation",   CHECK_PATH)
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
# Cell 3 — Silver SCD Type 2: full history (__START_AT / __END_AT added automatically)
dlt.create_streaming_table(
    name    = "silver_customers_scd2",
    comment = "Customer history — SCD Type 2 (full audit trail)"
)

dlt.apply_changes(
    target             = "silver_customers_scd2",
    source             = "bronze_customers_cdc",
    keys               = ["id"],
    sequence_by        = col("updated_at"),
    apply_as_deletes   = col("operation") == "DELETE",
    except_column_list = ["operation"],
    stored_as_scd_type = 2   # adds __START_AT and __END_AT columns
)
```

### Step 3: Create a new LDP pipeline for CDC

Repeat the pipeline creation from Task 1, but point to the `ldp_customers_cdc` notebook.[2]
After it runs, verify:

```sql
-- SCD1: Alice should have updated email; Bob should be removed from the current-state table
SELECT * FROM lab_catalog.day5_ldp.silver_customers_scd1 ORDER BY id;

-- SCD2: Alice should have TWO rows (original + updated).
-- Bob should remain in history, but his record should be closed with a non-null __END_AT.
SELECT id, name, email, __START_AT, __END_AT
FROM lab_catalog.day5_ldp.silver_customers_scd2
ORDER BY id, __START_AT;
```

**Expected SCD2 interpretation:**

```text
id | name  | email         | __START_AT          | __END_AT
 1 | Alice | alice@a.com   | 2024-01-01 10:00:00 | 2024-01-01 11:00:00
 1 | Alice | alice2@a.com  | 2024-01-01 11:00:00 | null         ← current record
 2 | Bob   | bob@b.com     | 2024-01-01 10:01:00 | 2024-01-01 12:00:00  ← historical, no longer current
```

In SCD Type 2, deletes close the active version rather than physically removing the historical row from the table.[8][16][17]

**✅ Check:** What is the current Lakeflow SQL name for this CDC feature (`AUTO CDC INTO`), and what would you use for CDC outside of LDP (hint: Delta Lake CDF)?[9][18]

***

## Task 3 — Build a Multi-Task Lakeflow Job (30 min)

**Goal:** Create a Lakeflow Job with task dependencies, task values, trigger types, and repair run practice.

> **Exam note:** The exam no longer tests DLT pipeline tasks inside Workflows as a separate topic. Instead, Lakeflow Jobs uses a **pipeline task** type to trigger an LDP pipeline run. The focus is on control flow, trigger types, and DAG-based orchestration.

### Step 1: Create the task notebooks

**Notebook 1: `task_ingest`**

```python
# Simulates ingestion — passes record count downstream via task values
record_count = spark.table("lab_catalog.day5_ldp.raw_orders").count()

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
    debugValue = 999    # used only in interactive runs
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
# Runs on ANY_FAILED condition — simulates failure alerting
print("[alert] A pipeline task failed. Sending notification...")
# In production: call a webhook, Databricks notification destination, or REST API here
```

### Step 2: Create the Lakeflow Job

1. Navigate to **Lakeflow → Jobs → Create Job**
2. Set the job name: `day5_lab_pipeline`
3. Add tasks in this order with these dependencies:

```
task_ingest → task_validate → task_report
                   ↓
             task_alert  (run condition: AT_LEAST_ONE_FAILED)
```

For each task:

- **Type:** Notebook
- **Cluster:** Job cluster (new cluster per run) with DBR 15.4 LTS or Serverless
- **Parameters:** none needed (task values are used instead)

4. Add a **pipeline task** (optional — for exam awareness):
   - Add a fifth task `task_run_ldp` of type **Pipeline**
   - Point it to your `orders_pipeline_lab` LDP pipeline
   - This is how Lakeflow Jobs orchestrates LDP pipelines — there is no separate "DLT on Workflows" concept

5. Add a **schedule and trigger**:
   - **Time-based trigger:** every weekday at 06:00 Europe/Berlin
     - Quartz cron: `0 0 6 ? * MON-FRI *`
     - Timezone: `Europe/Berlin`
   - **File-arrival trigger (exam topic):** alternatively, configure a file-arrival trigger pointing to your landing zone — the job fires when new files appear
   - **Table-update trigger (exam topic):** trigger downstream jobs when an upstream table is updated

### Step 3: Understand trigger types (exam topic)

| Trigger type | When to use |
|---|---|
| **Scheduled (time-based)** | Predictable batch cadence; data arrives reliably by a certain time |
| **File arrival** | React immediately when files land in cloud storage; removes polling |
| **Table update** | Chain jobs so a downstream job fires when an upstream Delta table changes |

### Step 4: Test Repair Run

1. Temporarily introduce a failure in `task_validate` (e.g., `raise ValueError("forced fail")`)
2. Run the job manually — `task_validate` fails, `task_report` is skipped, `task_alert` runs
3. Fix the notebook
4. In the job run history, click **Repair Run**
5. Observe that only `task_validate` and `task_report` re-run — `task_ingest` is skipped

**✅ Check:** What is the difference between `AT_LEAST_ONE_FAILED` and `ALL_DONE` task run conditions? When would you use a file-arrival trigger instead of a scheduled trigger?

***

## Task 4 — Declarative Automation Bundles (DABs) Hands-On (30 min)

**Goal:** Create, deploy, and run a DABs bundle using the Databricks CLI. Understand environment-specific configuration via variables and overrides.

> **Exam note:** The official name is now **Declarative Automation Bundles (DABs)** (formerly Databricks Asset Bundles). The `databricks.yml` syntax and core CLI workflow remain the same: validate, deploy, and run.[5]

### Step 1: Install the Databricks CLI

```bash
# macOS (Homebrew)
brew tap databricks/tap
brew install databricks

# Linux / WSL
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verify
databricks --version
# Expected: modern Databricks CLI with bundle support
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
# Choose a bundle template suitable for jobs/pipelines in your workspace
# Enter project name: day5_orders_lab
# The exact template names can evolve across CLI releases; the important exam concept is bundle-based resource definition and deployment.[web:152]
```

### Step 4: Edit `databricks.yml`

Replace the generated content with the following. Note the use of **variables** for environment-specific configuration — a key exam topic:

```yaml
bundle:
  name: day5_orders_lab

variables:
  # Overridden per target to avoid hardcoding environment-specific values
  num_workers:
    default: 2
  schedule_pause_status:
    default: PAUSED   # safe default — always paused until explicitly enabled

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
    variables:
      num_workers: 1               # smaller cluster for dev cost savings
      schedule_pause_status: PAUSED

  prod:
    mode: production
    workspace:
      host: https://adb-<workspace-id>.azuredatabricks.net
    variables:
      num_workers: 4
      schedule_pause_status: UNPAUSED   # enable in prod
    run_as:
      service_principal_name: sp-day5-prod@company.com

resources:
  jobs:
    day5_orders_job:
      name: day5_orders_lab_${bundle.target}
      schedule:
        quartz_cron_expression: "0 0 6 ? * MON-FRI *"
        timezone_id: "Europe/Berlin"
        pause_status: ${var.schedule_pause_status}
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
        - task_key: run_ldp_pipeline
          depends_on:
            - task_key: validate
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
          # No cluster needed — LDP manages its own compute
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id:  "Standard_DS3_v2"   # Azure; change for AWS/GCP
            num_workers:   ${var.num_workers}

  pipelines:
    orders_pipeline:
      name: orders_pipeline_lab_${bundle.target}
      target: lab_catalog.day5_ldp
      libraries:
        - notebook:
            path: ./src/notebooks/ldp_orders_pipeline.py
```

### Step 5: Deploy and run

```bash
# Validate the bundle config (catches YAML errors, missing files, etc.)
databricks bundle validate --target dev

# Deploy to dev (creates/updates the job and pipeline in the workspace)
databricks bundle deploy --target dev

# Trigger an immediate run
databricks bundle run --target dev day5_orders_job

# Deploy to prod (uses mode: production — note the run_as service principal)
databricks bundle deploy --target prod
```

### Step 6: Observe dev vs prod behaviour (exam topic)

| Behaviour | `mode: development` | `mode: production` |
|---|---|---|
| Resource name prefix | `[dev <username>] ...` added | No prefix |
| Multiple devs deploying | Each gets their own namespaced copy | Single shared resource |
| `run_as` enforcement | Not required | Required (service principal) |
| Schedule default | Paused | Controlled by variable |

### Step 7: Understand Git integration and branch management

```bash
# Git-backed workspace folders / Repos — exam topic: CI/CD workflow
# In the workspace UI:
#   1. Open your Git-backed project folder and create a feature branch (feature/day5-update)
#   2. Edit the LDP notebook or bundle files and commit
#   3. Push the branch and open a pull request in GitHub/GitLab/Azure DevOps
#   4. After merge to main, CI/CD runs bundle validation and deployment, e.g.:
#      databricks bundle validate --target prod
#      databricks bundle deploy --target prod
```

> Databricks terminology around Git-backed folders can vary by workspace and cloud, but the exam focus is the SDLC pattern: branch, review, merge, validate, deploy.[5]

**✅ Check questions:**

1. What does `mode: development` add to resource names, and why is it useful when multiple developers share a workspace?
2. Why use a DABs variable for `num_workers` instead of hardcoding different `databricks.yml` files per environment?
3. What is the difference between `databricks bundle deploy` and `databricks bundle run`?
4. In a CI/CD workflow, which Databricks CLI commands would you call in a GitHub Actions pipeline to promote code from dev to prod?

***

## Task 5 — Delta Sharing & Lakehouse Federation (20 min) ⭐ New exam topic

**Goal:** Understand Delta Sharing for cross-platform data sharing, and Lakehouse Federation for querying external sources.

### Part A — Delta Sharing

Delta Sharing is an open protocol to share Delta Lake data securely with external recipients — including non-Databricks systems.

```sql
-- Step 1: Create a Share object in Unity Catalog
CREATE SHARE orders_share
  COMMENT 'Share completed orders with partner analytics teams';

-- Step 2: Add a table to the share (you can add partitions or specific columns)
ALTER SHARE orders_share
  ADD TABLE lab_catalog.day5_ldp.customer_order_summary;

-- Step 3: Create a Recipient (Databricks-to-Databricks sharing)
CREATE RECIPIENT partner_team
  USING ID 'databricks_account_id_of_recipient_workspace';
  -- For external (non-Databricks) recipients, omit USING ID — a token-based URL is generated

-- Step 4: Grant access
GRANT SELECT ON SHARE orders_share TO RECIPIENT partner_team;

-- Step 5: View what is shared
SHOW ALL IN SHARE orders_share;

-- Step 6: Revoke access
REVOKE SELECT ON SHARE orders_share FROM RECIPIENT partner_team;
```

**Key Delta Sharing concepts for the exam:**

| Concept | Detail |
|---|---|
| **Databricks-to-Databricks** | Uses metastore ID; recipient reads live Delta data directly |
| **External recipients** | A signed activation URL + credential file is generated; works with non-Databricks clients |
| **Cost consideration** | Data egress charges apply when sharing across cloud regions or cloud providers |
| **Limitations** | Recipients cannot write back; streaming via Delta Sharing has latency considerations |
| **Open protocol** | Any system supporting the Delta Sharing open protocol can consume the share |

**✅ Check:** When would you use Delta Sharing instead of granting SELECT on a table within the same metastore? What are the cost implications of sharing data across cloud providers?

### Part B — Lakehouse Federation

Lakehouse Federation lets you query external databases (PostgreSQL, MySQL, Redshift, Snowflake, etc.) through Unity Catalog **without moving the data**.

```sql
-- Step 1: Create a Connection to an external PostgreSQL database
CREATE CONNECTION postgres_prod
  TYPE POSTGRESQL
  OPTIONS (
    host     'prod-db.company.com',
    port     '5432',
    database 'sales',
    user     'readonly_user',
    password secret('my-scope', 'postgres-password')
  );

-- Step 2: Create a Foreign Catalog mapping the external database
CREATE FOREIGN CATALOG postgres_sales
  USING CONNECTION postgres_prod
  OPTIONS (database 'sales');

-- Step 3: Query the external table through Unity Catalog — no data movement
SELECT * FROM postgres_sales.public.transactions LIMIT 100;

-- Step 4: Join external data with internal Databricks data
SELECT
  t.transaction_id,
  t.amount,
  o.customer_id,
  o.status
FROM postgres_sales.public.transactions t
JOIN lab_catalog.day5_ldp.clean_orders o
  ON t.order_ref = o.order_id
LIMIT 50;
```

**Key Lakehouse Federation concepts for the exam:**

| Concept | Detail |
|---|---|
| **Use case** | Query external systems without ETL; useful during migration or for federated reporting |
| **Unity Catalog governed** | Foreign catalogs appear in UC lineage and access controls apply |
| **Data stays in place** | No replication; data is read at query time from the external system |
| **Supported sources** | PostgreSQL, MySQL, SQL Server, Snowflake, Redshift, BigQuery, and more |
| **Performance** | Slower than native Delta reads; best for low-volume or exploratory queries |

**✅ Check:** When should you use Lakehouse Federation instead of ingesting data with Lakeflow Connect? What are the limitations?

***

## Task 6 — Liquid Clustering & Predictive Optimization (20 min) ⭐ New exam topic

**Goal:** Understand Liquid Clustering as the modern replacement for partitioning and Z-ordering, and enable Predictive Optimization.

### Part A — Liquid Clustering

```sql
-- Create a table with Liquid Clustering instead of partitioning
CREATE TABLE lab_catalog.day5_ldp.orders_clustered
  CLUSTER BY (customer_id, order_date)
  AS SELECT * FROM lab_catalog.day5_ldp.clean_orders;

-- Or add clustering to an existing table
ALTER TABLE lab_catalog.day5_ldp.clean_orders
  CLUSTER BY (customer_id, order_date);

-- Trigger a clustering compaction run manually
OPTIMIZE lab_catalog.day5_ldp.orders_clustered;

-- Check clustering information
DESCRIBE DETAIL lab_catalog.day5_ldp.orders_clustered;
```

**Liquid Clustering vs. traditional partitioning (exam comparison):**

| Feature | Partitioning | Z-Ordering | Liquid Clustering |
|---|---|---|---|
| **Column cardinality** | Low (e.g., year/month) | Any | Any |
| **Multi-column support** | Nested directories | Yes (up to 4) | Yes (flexible) |
| **Rebalancing on change** | Full rewrite | Full OPTIMIZE needed | Incremental, online |
| **Filter push-down** | Yes | Partial | Yes |
| **Changing clustering keys** | Requires rewrite | Requires rewrite | `ALTER TABLE ... CLUSTER BY` |
| **Recommended for new tables?** | No | No | **Yes** |

```python
# Liquid Clustering with PySpark
df = spark.table("lab_catalog.day5_ldp.clean_orders")
df.write \
  .format("delta") \
  .clusterBy("customer_id", "order_date") \
  .mode("overwrite") \
  .saveAsTable("lab_catalog.day5_ldp.orders_clustered_v2")
```

### Part B — Predictive Optimization

```sql
-- Enable Predictive Optimization at the catalog level (Unity Catalog managed)
ALTER CATALOG lab_catalog ENABLE PREDICTIVE OPTIMIZATION;

-- Or at the schema level
ALTER SCHEMA lab_catalog.day5_ldp ENABLE PREDICTIVE OPTIMIZATION;

-- Verify
DESCRIBE CATALOG lab_catalog;
```

**What Predictive Optimization does:**

- Automatically runs `OPTIMIZE` and `VACUUM` in the background based on access patterns
- No manual scheduling needed — Databricks decides when and what to optimize
- Applies to Unity Catalog managed tables on supported tiers
- Removes the need to schedule `OPTIMIZE` jobs in Lakeflow Jobs

**✅ Check:** When would you choose Liquid Clustering over partitioning? What does Predictive Optimization remove the need for?

***

## Task 7 — Unity Catalog Governance (20 min)

**Goal:** Practice GRANT/REVOKE, column masking, row-level security (ABAC), and lineage tracking.

```sql
-- Step 1: Grant access to a group
GRANT USE CATALOG ON CATALOG lab_catalog              TO `analysts`;
GRANT USE SCHEMA  ON SCHEMA  lab_catalog.day5_ldp     TO `analysts`;
GRANT SELECT      ON TABLE   lab_catalog.day5_ldp.customer_order_summary TO `analysts`;

-- Step 2: Check permissions
SHOW GRANTS ON TABLE lab_catalog.day5_ldp.customer_order_summary;

-- Step 3: Column masking (hide email from non-privileged users)
CREATE OR REPLACE FUNCTION lab_catalog.day5_ldp.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-access') THEN email
  ELSE regexp_replace(email, '(^[^@]+)', '***')
END;

ALTER TABLE lab_catalog.day5_ldp.silver_customers_scd1
  ALTER COLUMN email SET MASK lab_catalog.day5_ldp.mask_email;

-- Verify: users not in 'pii-access' see ***@domain.com
SELECT id, name, email FROM lab_catalog.day5_ldp.silver_customers_scd1;

-- Step 4: Row-level security / ABAC (Attribute-Based Access Control)
-- Only show rows belonging to the user's region
CREATE OR REPLACE FUNCTION lab_catalog.day5_ldp.filter_by_region(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member(region)
    OR is_account_group_member('global-admins');

ALTER TABLE lab_catalog.day5_ldp.customer_order_summary
  SET ROW FILTER lab_catalog.day5_ldp.filter_by_region ON (customer_id);
-- (In practice, apply the filter on a region column, not customer_id)

-- Step 5: DENY (Unity Catalog DENY overrides GRANT)
DENY SELECT ON TABLE lab_catalog.day5_ldp.silver_customers_scd2 TO `contractors`;

-- Step 6: Check lineage
-- Navigate to: Catalog Explorer → lab_catalog → day5_ldp → clean_orders → Lineage tab
-- You should see: raw_orders (source) → clean_orders → customer_order_summary (downstream)

-- Step 7: Revoke
REVOKE SELECT ON TABLE lab_catalog.day5_ldp.customer_order_summary FROM `analysts`;
```

**✅ Check:** What is the difference between a **column mask** and a **row filter** in Unity Catalog? What does `DENY` do that `REVOKE` does not? What is ABAC in the context of Unity Catalog?

***

## Task 8 — Spark UI Performance Analysis (15 min)

**Goal:** Identify performance bottlenecks using the Spark UI — a dedicated exam section.

```python
# Run a join that may cause data skew and shuffling
orders_df   = spark.table("lab_catalog.day5_ldp.clean_orders")
customers_df = spark.range(1000).selectExpr("id AS customer_id", "concat('Customer_', id) AS name")

# This join triggers a shuffle — visible in Spark UI Stage details
result = orders_df.join(customers_df, on="customer_id", how="inner")
result.groupBy("name").sum("amount").show()
```

**In the Spark UI, look for:**

| Indicator | What it means | Fix |
|---|---|---|
| Large **shuffle read/write** in a stage | Expensive data movement between executors | Increase `spark.sql.shuffle.partitions`, use broadcast join for small tables |
| **Spill to disk** > 0 | Executor ran out of memory mid-shuffle | Increase `spark.executor.memory` or reduce partition size |
| One task taking 10x longer than others | **Data skew** — one partition is much larger | Salt the skewed key or use AQE (`spark.sql.adaptive.enabled=true`) |
| Stage with 200 tasks when 10 would suffice | Default shuffle partition count too high | Set `spark.sql.shuffle.partitions` to match actual data volume |

```python
# Tuning parameters — exam topic
spark.conf.set("spark.sql.shuffle.partitions", "20")       # default 200
spark.conf.set("spark.sql.adaptive.enabled", "true")       # AQE (on by default in DBR 14+)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  # auto-broadcast small tables
```

**✅ Check:** How do you identify data skew in the Spark UI? What is the difference between data spill and shuffle? Name two tuning parameters that affect shuffle performance.

***

## Task 9 — Exam-Style Quiz (15 min)

**Q1.** In a Lakeflow Spark Declarative Pipeline, you define:

```python
@dlt.expect_or_drop("positive_amount", "amount > 0")
def clean_orders(): ...
```

A row arrives with `amount = -5`. What happens?

- A) The pipeline fails immediately
- B) The row is logged as a warning and kept
- **C) The row is silently removed from the output** ✅
- D) The pipeline pauses and waits for operator input

***

**Q2.** A 6-task Lakeflow Job: tasks 1–3 succeeded, task 4 failed, tasks 5–6 were skipped. You click **Repair Run**. Which is correct?

- A) All 6 tasks run from the beginning
- B) Only task 4 re-runs; tasks 5–6 remain skipped
- **C) Tasks 4, 5, and 6 re-run; tasks 1–3 are not re-run** ✅
- D) Only failed tasks re-run; skipped tasks require a full new run

***

**Q3.** In an LDP notebook, you want to read from another streaming table in the same pipeline. Which function do you use?

- A) `spark.readStream("table_name")`
- B) `dlt.read("table_name")`
- **C) `dlt.read_stream("table_name")` ✅**
- D) `spark.table("LIVE.table_name")`

***

**Q4.** You need to share a Delta table with a partner company that uses Snowflake (not Databricks). Which approach is correct?

- A) Grant SELECT on the table in Unity Catalog and provide the partner's service principal
- **B) Create a Delta Share and generate an activation URL for an external recipient** ✅
- C) Use Lakehouse Federation to expose the table as a foreign catalog
- D) Export the table to Parquet and upload to the partner's S3 bucket

***

**Q5.** In Declarative Automation Bundles, what does `mode: production` enforce?

- A) Deploys automatically to the production workspace URL
- **B) Removes the `[dev <username>]` prefix from resource names and enforces `run_as` with a service principal** ✅
- C) Enables row-level security on all tables deployed by the bundle
- D) Activates Predictive Optimization on all Delta tables

***

**Q6.** Which statement about Liquid Clustering is correct?

- A) Liquid Clustering requires specifying partitions; it replaces Z-ordering only
- B) Liquid Clustering cannot be applied to existing tables
- **C) Liquid Clustering is the recommended approach for new Delta tables; it supports online, incremental rebalancing without a full table rewrite** ✅
- D) Liquid Clustering requires the Advanced LDP edition

***

**Q7.** Which Lakeflow Jobs trigger type should you use when a downstream pipeline should run immediately after new files appear in ADLS?

- A) Scheduled trigger with a 5-minute interval
- **B) File-arrival trigger pointing to the ADLS container** ✅
- C) Table-update trigger on the bronze streaming table
- D) Continuous pipeline mode in LDP

***

**Q8.** What does Predictive Optimization in Unity Catalog do?

- A) Automatically applies ML-based clustering to improve model training speed
- B) Suggests optimal query plans using AI in the SQL Editor
- **C) Automatically schedules and runs OPTIMIZE and VACUUM operations based on table access patterns, removing the need for manual maintenance jobs** ✅
- D) Enables predicate push-down for all Unity Catalog managed tables

***

**Q9.** In the Spark UI, a stage shows that one task took 45 seconds while all other tasks completed in under 2 seconds. What does this indicate?

- A) A library conflict causing serialization errors
- B) Insufficient `spark.executor.memory` for the stage
- **C) Data skew — one partition contains significantly more data than the others** ✅
- D) The Photon runtime is not enabled for this cluster

***

**Q10.** Which Unity Catalog feature lets you query a PostgreSQL database through Databricks SQL without moving or replicating the data?

- A) Delta Sharing external recipient
- B) Unity Catalog federated identity
- **C) Lakehouse Federation (Foreign Catalog)** ✅
- D) Lakeflow Connect managed connector

***

## ✅ Day 5 Completion Checklist

- [ ] Successfully ran a 3-layer LDP pipeline (Bronze Streaming Table → Silver Streaming Table → Gold Materialized View)
- [ ] Observed data quality expectation violations in the LDP pipeline UI
- [ ] Ran a CDC pipeline with `APPLY CHANGES INTO` and verified SCD1 vs SCD2 output
- [ ] Built a multi-task Lakeflow Job with task values, a pipeline task, and practiced Repair Run
- [ ] Understood all three Lakeflow Jobs trigger types (scheduled, file arrival, table update)
- [ ] Deployed a DABs bundle with environment-specific variables using `databricks bundle deploy`
- [ ] Understood Delta Sharing for both Databricks-to-Databricks and external recipients
- [ ] Understood Lakehouse Federation for querying external databases without data movement
- [ ] Practiced Liquid Clustering and compared it with traditional partitioning/Z-ordering
- [ ] Enabled Predictive Optimization and understood what it replaces
- [ ] Practiced GRANT/REVOKE/DENY, column masking, and row-level ABAC in Unity Catalog
- [ ] Identified data skew and shuffle bottlenecks in the Spark UI
- [ ] Scored 9/10 or better on the exam-style quiz

***

## 🔗 Official Resources

- [Lakeflow Spark Declarative Pipelines (LDP)](https://docs.databricks.com/aws/en/dlt/)
- [APPLY CHANGES INTO — CDC](https://docs.databricks.com/aws/en/dlt/cdc)
- [Lakeflow Jobs (Workflows)](https://docs.databricks.com/aws/en/jobs/)
- [Declarative Automation Bundles (DABs)](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Databricks CLI Reference](https://docs.databricks.com/aws/en/dev-tools/cli/)
- [Delta Sharing](https://docs.databricks.com/aws/en/delta-sharing/)
- [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/)
- [Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering)
- [Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)
- [Unity Catalog Privileges Reference](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)
- [Spark UI Guide](https://docs.databricks.com/aws/en/compute/spark-ui)
- [Official Exam Guide (May 4, 2026)](https://www.databricks.com/sites/default/files/2026-03/databricks-certified-data-engineer-associate-exam-guide-may-4-2026.pdf)
