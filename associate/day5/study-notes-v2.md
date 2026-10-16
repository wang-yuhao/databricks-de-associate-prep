# Day 5 — Productionizing Pipelines: Lakeflow Spark Declarative Pipelines, Lakeflow Jobs & CI/CD

> **Exam version:** Aligned with the **Databricks Certified Data Engineer Associate** exam guide effective **May 4, 2026**.
> **Exam weight:** ~20% | **Time budget:** 8–10 hours | **Environment:** Full production Databricks workspace (Azure, AWS, or GCP)
>
> **Key terminology changes in this version:**
> - **Delta Live Tables (DLT)** → **Lakeflow Spark Declarative Pipelines (LDP)**
> - **Databricks Workflows / Jobs** → **Lakeflow Jobs**
> - **Databricks Asset Bundles (DABs)** → **Declarative Automation Bundles (DABs)**
> - **DLT pipeline task inside Workflows** is **removed from the exam** — pipelines are run as a **pipeline task** within Lakeflow Jobs

---

## 1. Lakeflow Spark Declarative Pipelines (LDP)

### What is LDP?

Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT) is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define *what* the data should look like and LDP handles *how* to run, monitor, and recover the pipeline.

> **Note:** You will still see `import dlt` in Python code and the Python library is still named `dlt`. The *product* name is now Lakeflow Spark Declarative Pipelines, but the API is unchanged.

**Key advantages over manual Spark jobs:**

- Automatic dependency resolution between tables
- Built-in data quality enforcement (Expectations)
- Auto-restart on failure
- Full lineage tracking in Unity Catalog
- Native support for Change Data Capture (CDC) via `APPLY CHANGES INTO`

### LDP Table Types

| Type | SQL Keyword | Python Decorator | Materialized? | Notes |
|---|---|---|---|---|
| **Streaming Table** | `CREATE OR REFRESH STREAMING TABLE` | `@dlt.table` + `spark.readStream` | Yes | For append-only / streaming sources |
| **Materialized View** | `CREATE OR REFRESH MATERIALIZED VIEW` | `@dlt.table` + `spark.read` | Yes | Recomputed on each pipeline run |
| **View** | `CREATE LIVE VIEW` | `@dlt.view` | No | Computed at query time, not stored |

> 📌 **Exam tip:** Streaming Tables and Materialized Views are physically stored as Delta tables. Live Views are not — they are computed on demand like a regular SQL view.

```sql
-- Bronze: Streaming Table — Auto Loader from ADLS Gen2
CREATE OR REFRESH STREAMING TABLE raw_orders
COMMENT 'Raw orders ingested from ADLS Gen2 landing zone via Auto Loader'
AS SELECT * FROM cloud_files(
  "abfss://landing@<storage-account>.dfs.core.windows.net/orders/",
  "json",
  map("cloudFiles.inferColumnTypes",    "true",
      "cloudFiles.schemaLocation",      "abfss://checkpoints@<storage-account>.dfs.core.windows.net/raw_orders_schema",
      "cloudFiles.schemaEvolutionMode", "rescue")
);

-- Silver: Materialized View — clean and type-cast
CREATE OR REFRESH MATERIALIZED VIEW clean_orders
COMMENT 'Cleaned and typed orders from bronze layer'
AS SELECT
  order_id,
  CAST(order_date AS DATE)        AS order_date,
  customer_id,
  CAST(amount AS DECIMAL(10,2))   AS amount,
  status
FROM LIVE.raw_orders
WHERE order_id IS NOT NULL;

-- Gold: View — no storage, computed live from Silver
CREATE LIVE VIEW daily_revenue
COMMENT 'Daily revenue rollup — computed at query time'
AS SELECT
  order_date,
  SUM(amount)  AS revenue,
  COUNT(*)     AS order_count
FROM LIVE.clean_orders
GROUP BY order_date;
```

```python
# Python LDP equivalent
import dlt
from pyspark.sql.functions import col

@dlt.table(
    comment="Raw orders from ADLS Gen2 landing zone",
    table_properties={"pipelines.reset.allowed": "false"}  # prevent accidental full refresh
)
def raw_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format",             "json")
        .option("cloudFiles.inferColumnTypes",    "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation",
                "abfss://checkpoints@<storage>.dfs.core.windows.net/raw_orders_schema")
        .load("abfss://landing@<storage>.dfs.core.windows.net/orders/")
    )

@dlt.table(comment="Cleaned and typed orders")
def clean_orders():
    return (
        dlt.read_stream("raw_orders")
        .filter(col("order_id").isNotNull())
        .withColumn("order_date", col("order_date").cast("date"))
        .withColumn("amount",     col("amount").cast("decimal(10,2)"))
    )
```

### LDP Expectations (Data Quality)

Expectations define data quality rules with three enforcement modes:

| Mode | SQL | Python | Behavior |
|---|---|---|---|
| **Warn** | `CONSTRAINT ... EXPECT (cond)` | `@dlt.expect("name", cond)` | Log violations, keep all rows |
| **Drop** | `EXPECT ... ON VIOLATION DROP ROW` | `@dlt.expect_or_drop("name", cond)` | Remove bad rows silently |
| **Fail** | `EXPECT ... ON VIOLATION FAIL UPDATE` | `@dlt.expect_or_fail("name", cond)` | Stop the entire pipeline |

```sql
CREATE OR REFRESH MATERIALIZED VIEW valid_orders
  CONSTRAINT valid_amount  EXPECT (amount > 0)                ON VIOLATION DROP ROW,
  CONSTRAINT valid_date    EXPECT (order_date >= '2020-01-01') ON VIOLATION WARN,
  CONSTRAINT no_null_id    EXPECT (order_id IS NOT NULL)       ON VIOLATION FAIL UPDATE
AS SELECT * FROM LIVE.raw_orders;
```

```python
@dlt.table
@dlt.expect_or_fail("no_null_id",   "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect("valid_date",           "order_date >= '2020-01-01'")
def valid_orders():
    return dlt.read_stream("raw_orders")
```

> 📌 **Exam tip:** Expectations are defined **per table**, not per pipeline. Multiple expectations on the same table are evaluated independently.

### LDP Pipeline Modes

| Mode | Description | Use Case |
|---|---|---|
| **Triggered** | Runs once, processes all new data, cluster terminates | Batch workloads, cost-sensitive |
| **Continuous** | Always running, sub-minute latency | Near-real-time requirements |

**Development vs Production mode:**

| Feature | Development | Production |
|---|---|---|
| Cluster reuse | Yes (faster iteration) | No (fresh cluster each run) |
| Auto-retry on failure | No (errors surface immediately) | Yes |
| Resource isolation | Shared | Full isolation per run |

### LDP Schema Evolution (Auto Loader)

Auto Loader handles schema evolution automatically via `cloudFiles.schemaEvolutionMode`:

| Mode | Behavior |
|---|---|
| `addNewColumns` (default) | New source columns are added to the target schema |
| `rescue` | Unknown columns go into a `_rescued_data` JSON column |
| `failOnNewColumns` | Pipeline fails if source schema changes |
| `none` | Schema changes are ignored |

> 🔑 **Key difference vs Structured Streaming:** In vanilla Structured Streaming, schema changes require manual intervention, checkpoint deletion, and restart. LDP with Auto Loader handles this automatically using `schemaLocation` checkpointing — the schema is persisted and evolved in place.

---

## 2. APPLY CHANGES INTO — CDC in LDP

`APPLY CHANGES INTO` is LDP's built-in Change Data Capture (CDC) mechanism. It processes a CDC stream (inserts, updates, deletes) and automatically maintains the current state of a target Silver table.

### When to Use

- Source emits CDC events with an `operation` column (`INSERT`, `UPDATE`, `DELETE`) and a sequence key
- You want LDP to automatically handle upserts/deletes without writing `MERGE INTO` logic
- Typical sources: Debezium on Kafka, AWS DMS, Azure DMS, Fivetran CDC, Lakeflow Connect CDC connectors

### Syntax

```sql
-- Step 1: Declare the target streaming table first
CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT 'Current state of customers, maintained via CDC';

-- Step 2: Define the CDC logic
APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_customers_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (_rescued_data, operation, updated_at)
STORED AS SCD TYPE 1;
```

```python
# Python equivalent
import dlt
from pyspark.sql.functions import col

dlt.create_streaming_table(
    name    = "silver_customers",
    comment = "Current state of customers, maintained via CDC"
)

dlt.apply_changes(
    target             = "silver_customers",
    source             = "bronze_customers_cdc",
    keys               = ["customer_id"],
    sequence_by        = col("updated_at"),
    apply_as_deletes   = col("operation") == "DELETE",
    except_column_list = ["_rescued_data", "operation", "updated_at"],
    stored_as_scd_type = 1   # use 2 for full history
)
```

### SCD Type 1 vs SCD Type 2

| Feature | SCD Type 1 | SCD Type 2 |
|---|---|---|
| **Updates** | Overwrite current record in-place | Insert new record; mark old as inactive |
| **History** | No history preserved | Full history preserved |
| **Extra columns** | None | `__START_AT`, `__END_AT` (sequence key values) |
| **Use case** | Current state lookup tables | Audit trail, point-in-time analysis |
| **LDP keyword** | `STORED AS SCD TYPE 1` | `STORED AS SCD TYPE 2` |

> ⚠️ **Exam trap:** `APPLY CHANGES INTO` requires **LDP Pro or Advanced** edition — it is NOT available in Core. It is only valid inside an LDP pipeline notebook — you cannot call it from a regular notebook or SQL editor. Outside LDP, use `MERGE INTO` with Delta Lake CDF for CDC.

---

## 3. Lakeflow Jobs (formerly Databricks Workflows)

### Overview

Lakeflow Jobs orchestrates multi-step pipelines. Each unit of work is a **Task**, connected via dependencies that form a DAG (Directed Acyclic Graph).

### Task Types

| Task Type | Description |
|---|---|
| **Notebook** | Run a Databricks notebook |
| **Python Script** | Run a `.py` file from a Git repo |
| **Pipeline** | Trigger an LDP pipeline as a task |
| **SQL** | Run SQL queries or Databricks SQL dashboards |
| **dbt** | Run a dbt Cloud or dbt Core project |
| **Spark Submit** | Low-level JAR or Python submit |
| **Run Job** | Trigger another job (parent-child orchestration) |

> 📌 **Exam note:** The task type for running an LDP pipeline is simply called **Pipeline** — there is no longer a separate "DLT pipeline on Workflows" concept. The pipeline task integrates natively into the Lakeflow Jobs DAG.

### Task Dependencies and Execution Conditions

```
ingest_data → validate_data → transform_data → export_report
                   ↓
            alert_on_failure  (condition: AT_LEAST_ONE_FAILED)
```

| Condition | Meaning |
|---|---|
| `ALL_SUCCESS` (default) | Downstream runs only if all upstream tasks succeeded |
| `ALL_DONE` | Downstream runs regardless of upstream result |
| `AT_LEAST_ONE_SUCCESS` | Downstream runs if any upstream task succeeded |
| `AT_LEAST_ONE_FAILED` | Downstream runs only if at least one upstream failed |

### Job Trigger Types ⭐ Exam topic

| Trigger | When to use |
|---|---|
| **Manual** | Click "Run Now" in UI or `POST /api/2.1/jobs/run-now` |
| **Scheduled (Cron)** | Predictable batch cadence; data reliably available by a time |
| **File arrival** | React immediately when new files land in cloud storage (ADLS/S3/GCS) — eliminates polling |
| **Table update** | Chain jobs; downstream fires when an upstream Delta table changes |
| **Continuous** | Re-runs immediately after each completion (lowest latency without LDP Continuous mode) |

**Cron syntax (Quartz format — 7 fields):**

```
Seconds  Minutes  Hours  Day-of-Month  Month  Day-of-Week  Year
   0        0       6         *           *      MON-FRI      *
```
= Every weekday at 06:00 UTC

**Choosing between trigger types:**

- Use **scheduled** when data arrives reliably by a fixed time (e.g., upstream batch runs at 05:00)
- Use **file arrival** when you don't control when upstream data lands
- Use **table update** to build cascading pipeline dependencies without polling
- Prefer **file arrival** or **table update** over a tight cron schedule — they reduce latency and wasted runs

### Task Values (Passing Data Between Tasks)

```python
# Task 1 (ingest_raw): set a value
dbutils.jobs.taskValues.set(key="record_count", value=1_250_000)
dbutils.jobs.taskValues.set(key="run_date",     value="2024-02-01")

# Task 2 (validate_data): read value from task 1
count = dbutils.jobs.taskValues.get(
    taskKey    = "ingest_raw",
    key        = "record_count",
    default    = 0,
    debugValue = 9999   # used only in interactive runs, not actual job runs
)
print(f"Records from ingest: {count:,}")
```

### Repair & Re-run

- **Repair run:** Re-runs only failed and downstream-skipped tasks, preserving all outputs from already-succeeded tasks
- Does NOT create a new run ID — it extends the existing run
- Invoked via UI ("Repair Run" button) or API: `POST /api/2.1/jobs/runs/repair`

### Job Compute Options

| Option | Description | Best For |
|---|---|---|
| **Job Cluster** | New cluster provisioned per run, terminated on completion | Production isolation, cost control |
| **All-Purpose Cluster** | Reuse an existing interactive cluster | Development/testing only |
| **Serverless** | Databricks-managed, instant startup (< 10 s) | All production jobs — recommended default |

---

## 4. Declarative Automation Bundles (DABs)

### What are DABs?

Declarative Automation Bundles (formerly Databricks Asset Bundles) are the Databricks-native CI/CD framework. Define pipelines, jobs, notebooks, and cluster configs as **YAML** files committed to Git — enabling proper DevOps lifecycle management.

> The acronym **DABs** is unchanged. The Databricks CLI commands (`databricks bundle ...`) are unchanged. Only the official product name has changed.

### Bundle Structure

```
my_project/
├── databricks.yml              ← Root bundle config
├── resources/
│   ├── orders_job.yml          ← Lakeflow Job definition
│   └── orders_pipeline.yml     ← LDP pipeline definition
├── src/
│   ├── ingest.py
│   ├── transform.sql
│   └── pipelines/
│       └── orders_pipeline.py
└── tests/
    └── test_transforms.py
```

### `databricks.yml` — with variables for environment-specific config

```yaml
bundle:
  name: orders_etl

variables:
  # Override these per target — never hardcode environment-specific values
  catalog:
    description: Unity Catalog target catalog
    default: dev_catalog
  num_workers:
    default: 2
  schedule_pause_status:
    default: PAUSED   # safe default — always paused until explicitly enabled in prod

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<dev-id>.azuredatabricks.net
    variables:
      catalog: dev_catalog
      num_workers: 1

  staging:
    mode: production
    workspace:
      host: https://adb-<staging-id>.azuredatabricks.net
    variables:
      catalog: staging_catalog
      num_workers: 2
    run_as:
      service_principal_name: sp-staging@company.com

  prod:
    mode: production
    workspace:
      host: https://adb-<prod-id>.azuredatabricks.net
    variables:
      catalog: prod_catalog
      num_workers: 4
      schedule_pause_status: UNPAUSED
    run_as:
      service_principal_name: sp-prod@company.com

resources:
  jobs:
    orders_job:
      name: orders_etl_${bundle.target}
      schedule:
        quartz_cron_expression: "0 0 6 ? * MON-FRI *"
        timezone_id: Europe/Berlin
        pause_status: ${var.schedule_pause_status}
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/ingest.py
          job_cluster_key: main_cluster
        - task_key: run_pipeline
          depends_on:
            - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.orders_pipeline.id}
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: Standard_DS3_v2
            num_workers: ${var.num_workers}

  pipelines:
    orders_pipeline:
      name: orders_pipeline_${bundle.target}
      target: ${var.catalog}.orders_db
      libraries:
        - notebook:
            path: ./src/pipelines/orders_pipeline.py
```

### CLI Commands

```bash
# Validate (catches YAML errors, missing notebook paths, etc.)
databricks bundle validate --target dev

# Deploy to dev (creates/updates resources in the workspace)
databricks bundle deploy --target dev

# Trigger an immediate run
databricks bundle run --target dev orders_job

# Deploy to prod
databricks bundle deploy --target prod

# Tear down a dev deployment
databricks bundle destroy --target dev
```

### dev vs prod behaviour

| Behaviour | `mode: development` | `mode: production` |
|---|---|---|
| Resource name | `[dev <username>] ...` prefix added | No prefix |
| Multiple devs | Each gets their own namespaced copy | Single shared resource |
| Cluster per run | Optional (can reuse) | Always fresh, isolated |
| Auto-retry | Off (fail fast) | On |
| `run_as` | Not required | Required (service principal) |

### CI/CD with GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy to Databricks Production

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main   # official action — Go-based CLI v0.200+

      - name: Validate Bundle
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle validate --target prod

      - name: Deploy Bundle
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle deploy --target prod
```

> ⚠️ **Deprecated:** `pip install databricks-cli` (the old Python CLI). Always use `databricks/setup-cli@main` (Go-based CLI, v0.200+) in CI/CD.

### Databricks Git Integration (CI/CD workflow)

- **Repos / Git Folders** = Git-connected folders in the workspace
- Supported providers: GitHub, GitLab, Bitbucket, Azure DevOps
- Operations from the UI: clone, pull, push, create/switch branch, create PR
- **CI/CD workflow:** feature branch → PR → merge to main → GitHub Actions triggers `databricks bundle deploy --target prod`
- Sparse checkout lets you sync only a subdirectory of a monorepo

---

## 5. Delta Sharing ⭐ New exam topic

Delta Sharing is an **open protocol** for sharing live Delta Lake data securely with external recipients — including non-Databricks systems — without copying or moving data.

### Key Concepts

| Concept | Detail |
|---|---|
| **Share** | A named, curated collection of tables/views to be shared |
| **Recipient** | The entity (team or system) receiving access |
| **Databricks-to-Databricks** | Uses metastore ID; recipient queries live Delta data directly |
| **External recipient** | A signed activation URL + token file is generated; works with Spark, pandas, Power BI, etc. |
| **Open protocol** | Any system supporting the Delta Sharing spec can consume the share |

### SQL Commands

```sql
-- Create a Share
CREATE SHARE orders_share
  COMMENT 'Share completed orders with partner analytics teams';

-- Add tables (you can also add specific partitions or views)
ALTER SHARE orders_share
  ADD TABLE lab_catalog.day5_ldp.customer_order_summary;

-- Databricks-to-Databricks recipient (uses recipient metastore ID)
CREATE RECIPIENT partner_team
  USING ID 'databricks_metastore_id_of_recipient';

-- External recipient (generates a token-based activation URL)
CREATE RECIPIENT external_partner;
-- Then run: DESCRIBE RECIPIENT external_partner; — shows activation_link

-- Grant access
GRANT SELECT ON SHARE orders_share TO RECIPIENT partner_team;

-- View share contents
SHOW ALL IN SHARE orders_share;

-- Revoke
REVOKE SELECT ON SHARE orders_share FROM RECIPIENT partner_team;
```

### Cost Considerations (exam topic)

| Scenario | Cost implication |
|---|---|
| Same cloud region | Low — minimal egress |
| Across cloud regions (same provider) | Moderate — region-to-region egress fees |
| Across cloud providers (e.g., Azure → AWS) | High — cross-cloud egress is expensive |

### Delta Sharing vs. Unity Catalog GRANT

| Use case | Approach |
|---|---|
| Sharing with a team in **the same metastore** | `GRANT SELECT ON TABLE ... TO \`group\`` |
| Sharing with a team in **a different Databricks account/metastore** | Delta Sharing (Databricks-to-Databricks) |
| Sharing with a **non-Databricks system** (Power BI, pandas, Snowflake) | Delta Sharing (external recipient) |

---

## 6. Lakehouse Federation ⭐ New exam topic

Lakehouse Federation lets you query external databases through Unity Catalog **without moving the data**. The external system appears as a **Foreign Catalog** in your metastore.

### Supported Sources

PostgreSQL, MySQL, SQL Server, Snowflake, Redshift, BigQuery, Hive, and more.

### Setup

```sql
-- Step 1: Create a Connection to the external system
CREATE CONNECTION postgres_prod
  TYPE POSTGRESQL
  OPTIONS (
    host     'prod-db.company.com',
    port     '5432',
    database 'sales',
    user     'readonly_user',
    password secret('my-scope', 'postgres-password')
  );

-- Step 2: Map the external database as a Foreign Catalog
CREATE FOREIGN CATALOG postgres_sales
  USING CONNECTION postgres_prod
  OPTIONS (database 'sales');

-- Step 3: Query as if it were a native UC table
SELECT * FROM postgres_sales.public.transactions LIMIT 100;

-- Step 4: Join with native Databricks data
SELECT t.transaction_id, t.amount, o.customer_id
FROM postgres_sales.public.transactions t
JOIN lab_catalog.day5_ldp.clean_orders o
  ON t.order_ref = o.order_id;
```

### Lakehouse Federation vs. Lakeflow Connect

| Aspect | Lakehouse Federation | Lakeflow Connect |
|---|---|---|
| **Data movement** | None — reads from source at query time | Copies data into Delta tables |
| **Performance** | Slower (source DB latency) | Fast (Delta on object storage) |
| **Freshness** | Always current (live query) | Depends on sync frequency |
| **Best for** | Exploratory queries, migration period, low volume | Production pipelines, high-volume, governed |
| **Governance** | UC lineage + access controls apply | UC lineage + access controls apply |

---

## 7. Liquid Clustering ⭐ New exam topic

Liquid Clustering is the **recommended replacement** for both partitioning and Z-ordering for new Delta tables. It provides flexible, online, incremental rebalancing without a full table rewrite.

### Creating Tables with Liquid Clustering

```sql
-- New table with Liquid Clustering
CREATE TABLE lab_catalog.day5_ldp.orders_clustered
  CLUSTER BY (customer_id, order_date)
AS SELECT * FROM lab_catalog.day5_ldp.clean_orders;

-- Add clustering to an existing table (no full rewrite required)
ALTER TABLE lab_catalog.day5_ldp.clean_orders
  CLUSTER BY (customer_id, order_date);

-- Change clustering keys at any time (incremental — only new data is reclustered)
ALTER TABLE lab_catalog.day5_ldp.clean_orders
  CLUSTER BY (order_date, status);

-- Trigger compaction manually (also runs automatically with Predictive Optimization)
OPTIMIZE lab_catalog.day5_ldp.orders_clustered;
```

```python
# PySpark
df.write \
  .format("delta") \
  .clusterBy("customer_id", "order_date") \
  .mode("overwrite") \
  .saveAsTable("lab_catalog.day5_ldp.orders_clustered")
```

### Liquid Clustering vs. Traditional Approaches

| Feature | Partitioning | Z-Ordering | Liquid Clustering |
|---|---|---|---|
| **Column cardinality** | Must be low | Any | Any |
| **Multi-column** | Creates nested directories | Up to 4 columns | Flexible, any number |
| **Changing keys** | Full rewrite required | Full `OPTIMIZE` required | `ALTER TABLE` — incremental |
| **Auto-maintenance** | No | No | Yes (with Predictive Optimization) |
| **Recommended for new tables** | ❌ No | ❌ No | ✅ Yes |
| **Filter push-down** | Yes | Partial | Yes |

> 📌 **Exam tip:** If a question asks about the *modern, flexible* approach to data layout optimisation that replaces partitioning and Z-ordering → the answer is **Liquid Clustering**.

---

## 8. Predictive Optimization ⭐ New exam topic

Predictive Optimization automatically runs `OPTIMIZE` and `VACUUM` in the background based on observed table access patterns — removing the need to manually schedule maintenance jobs.

```sql
-- Enable at catalog level
ALTER CATALOG lab_catalog ENABLE PREDICTIVE OPTIMIZATION;

-- Enable at schema level
ALTER SCHEMA lab_catalog.day5_ldp ENABLE PREDICTIVE OPTIMIZATION;

-- Check status
DESCRIBE CATALOG lab_catalog;
```

**What it replaces:**

- Manual `OPTIMIZE` cron jobs in Lakeflow Jobs
- Manual `VACUUM` cron jobs
- Guesswork about when to run OPTIMIZE vs. how often

**Key constraints:**

- Available on Unity Catalog managed tables only
- Requires a supported Databricks tier (Premium and above)
- Databricks decides *when* and *what* to optimize based on query patterns

---

## 9. Serverless Compute

Serverless compute removes infrastructure management. Databricks automatically provisions, scales, and terminates compute.

### Serverless vs Classic Clusters

| Feature | Classic Clusters | Serverless |
|---|---|---|
| **Startup time** | 3–8 minutes | < 10 seconds |
| **Configuration** | Manual (VM type, workers, init scripts) | Fully automatic |
| **Scaling** | Auto-scaling (minutes to react) | Instant |
| **Cost model** | Pay for full cluster lifetime | Pay per task execution time only |
| **Idle cost** | High | Zero |
| **Customization** | Full | Limited |

### Serverless SQL Warehouses

```
Databricks SQL → SQL Warehouses → Create Warehouse
  → Type: Serverless
  → Size: Small / Medium / Large (auto-scales based on concurrency)
  → Auto Stop: 10 min (configurable, minimum 5 min)
```

> 🔑 **Exam tip:** Serverless SQL Warehouses are the **recommended default** for Databricks SQL workloads. They start in seconds and cost nothing when idle. Use them for Lakeflow Jobs tasks as well to eliminate cluster startup latency.

---

## 10. Spark UI — Performance Analysis ⭐ Exam topic

The exam explicitly tests your ability to diagnose performance bottlenecks using the Spark UI.

### Key Spark UI Tabs

| Tab | What to look for |
|---|---|
| **Jobs** | Total job duration; failed jobs |
| **Stages** | Stage duration, task distribution, skew |
| **SQL / DataFrame** | Query plan; Photon nodes (`WholeStageCodegenTransformer`) |
| **Storage** | Cached RDDs/DataFrames |
| **Environment** | Confirm Spark config values |

### Common Bottlenecks and Fixes

| Symptom in Spark UI | Root cause | Fix |
|---|---|---|
| One task takes 10× longer than others | **Data skew** — one partition has much more data | Salt the skewed join key; enable AQE |
| Large **shuffle read/write** MB in a stage | Expensive cross-executor data movement | Reduce `spark.sql.shuffle.partitions`; use broadcast join for small tables |
| **Spill to disk** > 0 | Executor OOM during shuffle | Increase `spark.executor.memory`; reduce partition size |
| 200 tasks for a small dataset | Default shuffle partition count too high | `spark.sql.shuffle.partitions = 20` (match data volume) |
| Query plan shows `SortMergeJoin` for a small table | Broadcast threshold too low | `spark.sql.autoBroadcastJoinThreshold = 50MB` |

### Key Tuning Parameters

```python
# Lower shuffle partitions for small datasets (default is 200)
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Adaptive Query Execution (AQE) — enabled by default in DBR 14+
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Auto-broadcast small tables in joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50 MB

# Parallelism for non-shuffle operations
spark.conf.set("spark.default.parallelism", "40")

# Executor memory
spark.conf.set("spark.executor.memory", "8g")
```

---

## 11. Unity Catalog — Governance in Production

Unity Catalog provides unified, fine-grained governance across all Databricks workspaces in a region.

### Three-Level Namespace

```
catalog  →  schema  →  table / view / function / volume
```

### Privilege Hierarchy

```sql
-- Grant in order: catalog → schema → object
GRANT USE CATALOG ON CATALOG prod_catalog        TO `data-engineers`;
GRANT USE SCHEMA  ON SCHEMA  prod_catalog.sales  TO `analysts`;
GRANT SELECT      ON TABLE   prod_catalog.sales.transactions TO `analysts`;
GRANT MODIFY      ON TABLE   prod_catalog.sales.transactions TO `data-engineers`;

-- REVOKE removes a previously granted privilege
REVOKE SELECT ON TABLE prod_catalog.sales.transactions FROM `contractors`;

-- DENY explicitly blocks access (overrides GRANT — stronger than REVOKE)
DENY SELECT ON TABLE prod_catalog.sales.sensitive_data TO `contractors`;
```

> 📌 **Exam tip:** `REVOKE` removes a privilege that was granted. `DENY` explicitly blocks access and **overrides any GRANT** — even if a user is a member of a group that has `SELECT`, a `DENY` on that user still blocks them.

### Column Masking

```sql
-- Define the masking function
CREATE OR REPLACE FUNCTION prod_catalog.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-access') THEN email
  ELSE regexp_replace(email, '(^[^@]+)', '***')
END;

-- Apply the mask to a column
ALTER TABLE prod_catalog.sales.customers
  ALTER COLUMN email SET MASK prod_catalog.security.mask_email;

-- Remove the mask
ALTER TABLE prod_catalog.sales.customers
  ALTER COLUMN email DROP MASK;
```

### Row-Level Security (Row Filters / ABAC)

ABAC = Attribute-Based Access Control. Row filters restrict which rows a user can see based on their group membership or attributes.

```sql
-- Define a row filter function
CREATE OR REPLACE FUNCTION prod_catalog.security.filter_by_region(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('global-admins')
    OR is_account_group_member(region);

-- Apply the row filter to the table
ALTER TABLE prod_catalog.sales.transactions
  SET ROW FILTER prod_catalog.security.filter_by_region ON (region);

-- Remove the row filter
ALTER TABLE prod_catalog.sales.transactions
  DROP ROW FILTER;
```

### Column Mask vs. Row Filter

| Feature | Column Mask | Row Filter |
|---|---|---|
| **Scope** | Single column | Entire rows |
| **Use case** | Hide PII (emails, SSNs, card numbers) from non-privileged users | Multi-tenant isolation; region-specific access |
| **Behaviour** | Returns a transformed/redacted value | Returns `true` (row visible) or `false` (row hidden) |
| **Application** | `ALTER COLUMN ... SET MASK` | `SET ROW FILTER ... ON (col)` |

### Lineage

- Automatically tracked in Unity Catalog — no configuration needed
- View in **Catalog Explorer → table → Lineage tab**
- Tracks: column-level lineage, notebook reads/writes, LDP pipeline flows, SQL queries

---

## 12. Monitoring & Alerting

### Lakeflow Jobs Monitoring

- **Email notifications:** on start, success, failure, skipped, no-new-runs
- **Webhook / Slack:** via System Notification Destinations in workspace admin settings
- **Run history:** UI → Lakeflow → Jobs → Runs — view logs, duration, cluster metrics, DAG view
- **Trend analysis:** compare current run time against historical baselines in the run history view

### LDP Pipeline Event Log

The LDP event log is a queryable Delta table exposed via the `event_log()` function:

```sql
-- All recent events for a pipeline
SELECT timestamp, event_type, level, message
FROM event_log("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
WHERE level IN ('ERROR', 'WARN')
  AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY timestamp DESC;

-- Expectation pass/fail rates per run
SELECT
  timestamp,
  details:flow_progress.data_quality.expectations[0].name           AS expectation_name,
  details:flow_progress.data_quality.expectations[0].passed_records AS passed,
  details:flow_progress.data_quality.expectations[0].failed_records AS failed
FROM event_log("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC;
```

### Databricks SQL Alerts

```
Databricks SQL → Alerts → New Alert
  → Attach a saved SQL query
  → Set threshold: e.g., "Value > 0"
  → Configure Notification Destination (Email / Slack / PagerDuty)
  → Schedule: every 1 hour
```

---

## 13. Exam-Focused Key Points

### LDP Must-Know

- `LIVE.table_name` = reference another table within the same pipeline
- `dlt.read()` for batch reads; `dlt.read_stream()` for streaming reads within LDP
- Expectations are per-table; multiple expectations on one table are evaluated independently
- A pipeline can mix Streaming Tables and Materialized Views freely
- `APPLY CHANGES INTO` = CDC; requires **Pro or Advanced** edition; not usable outside LDP
- Schema evolution via `cloudFiles.schemaEvolutionMode`; `rescue` mode is the safest for production

### Lakeflow Jobs Must-Know

- **Repair run** = re-run only failed + downstream-skipped tasks, not the full job
- Three trigger types: **scheduled**, **file arrival**, **table update** — know when to use each
- Job clusters terminate automatically after run completes
- `dbutils.jobs.taskValues` = typed key-value store for passing data between tasks within a run
- Pipeline tasks run an LDP pipeline from within the Lakeflow Jobs DAG

### DABs Must-Know

- `databricks.yml` = root config; `targets` = dev/staging/prod environments
- Use `variables:` for environment-specific values (cluster size, catalog, pause status)
- `mode: development` → `[dev <username>]` prefix, faster iteration
- `mode: production` → isolated cluster per run, auto-retry, `run_as` required
- New CLI: `databricks/setup-cli@main` (Go-based); **deprecated:** `pip install databricks-cli`
- Key commands: `validate`, `deploy`, `run`, `destroy`

### Delta Sharing Must-Know

- Open protocol — not Databricks-only
- Databricks-to-Databricks uses metastore ID; external uses activation URL
- Cross-cloud sharing has egress cost implications
- Recipients cannot write back; read-only access only

### Lakehouse Federation Must-Know

- No data movement — live query from source at query time
- Use for exploratory/migration scenarios, not high-volume production ETL
- Foreign Catalogs appear in UC lineage and access controls apply

### Liquid Clustering Must-Know

- Replaces both partitioning and Z-ordering for new tables
- Supports incremental, online rebalancing — no full table rewrite
- Can change clustering keys with `ALTER TABLE ... CLUSTER BY`
- Works best with Predictive Optimization (auto-runs `OPTIMIZE`)

### Unity Catalog Must-Know

- Three-level namespace: `catalog.schema.table`
- One metastore per region, shared across all workspaces
- `DENY` overrides `GRANT`; `REVOKE` only removes a previously granted privilege
- Column masking = hide values; Row filter = hide rows (ABAC)
- Lineage is automatic — no extra configuration

---

## 14. Interview Preparation Q&A

### Q1: When would you use `APPLY CHANGES INTO` vs `MERGE INTO`?

**A:** Use `APPLY CHANGES INTO` when you are inside an **LDP pipeline** and your source is a CDC stream. LDP manages ordering, deduplication, and SCD history automatically. Use `MERGE INTO` when working **outside LDP** — in a regular notebook, a Lakeflow Jobs task, or the SQL editor — where you need fine-grained control over match conditions and specific columns to upsert/delete. `MERGE INTO` is more flexible but requires you to handle ordering and deduplication yourself.

---

### Q2: How does LDP handle schema evolution differently from vanilla Structured Streaming?

**A:** In vanilla Structured Streaming, any schema change in the source causes the stream to fail. You must manually update the schema, delete the checkpoint, and restart. In LDP with Auto Loader, schema evolution is handled automatically via `cloudFiles.schemaLocation` — the schema is checkpointed and evolved in-place according to `cloudFiles.schemaEvolutionMode`. New columns can be added, unknown columns rescued into `_rescued_data`, or failures triggered — all without manual intervention.

---

### Q3: What is the difference between a Triggered and Continuous LDP pipeline?

**A:** A **Triggered** pipeline runs on demand or on a schedule, processes all data since the last run, then shuts down — minimising cost. A **Continuous** pipeline keeps the cluster running indefinitely, processing new data with sub-minute latency. Use Triggered for batch workloads where a few-minute delay is acceptable. Use Continuous only when near-real-time latency is a hard requirement, because the cluster cost is constant regardless of data volume.

---

### Q4: When should you choose a file-arrival trigger over a scheduled trigger in Lakeflow Jobs?

**A:** Choose **file-arrival** when you don't control when upstream data lands — for example, when a partner uploads files at unpredictable times. A scheduled trigger might fire before the file arrives (wasted run) or miss a file that arrived late. File-arrival triggers fire exactly when data is ready, reducing latency and eliminating wasted runs. Use **scheduled** when data reliably arrives by a fixed time and you want predictable pipeline windows.

---

### Q5: What is the difference between Delta Sharing and granting SELECT on a Unity Catalog table?

**A:** `GRANT SELECT` works within the same Unity Catalog metastore — both parties must be on the same Databricks account and region. Delta Sharing works across metastores, across Databricks accounts, across cloud providers, and even with non-Databricks systems (pandas, Spark, Power BI). Use `GRANT SELECT` for internal team access; use Delta Sharing for cross-organisation or cross-platform sharing.

---

### Q6: When should you use Lakehouse Federation instead of ingesting data with Lakeflow Connect?

**A:** Use **Lakehouse Federation** when you want to query external data without copying it — typically during a migration period, for exploratory analysis, or for low-volume lookups. It's also useful when the data cannot leave the source system for compliance reasons. Use **Lakeflow Connect** when you need a production pipeline with governed Delta tables, high-throughput ingestion, and the performance benefits of Delta Lake. Lakehouse Federation adds source-system latency to every query; Lakeflow Connect does not.

---

### Q7: Why is Liquid Clustering preferred over partitioning for new Delta tables?

**A:** Traditional partitioning requires choosing a low-cardinality column upfront, creates fixed directory structures that are expensive to change, and doesn't help for multi-column filters not aligned with the partition key. Liquid Clustering: (1) supports any column cardinality, (2) allows online, incremental rebalancing without a full table rewrite, (3) lets you change clustering keys at any time with `ALTER TABLE`, and (4) integrates with Predictive Optimization for automatic maintenance. For existing tables, you can adopt Liquid Clustering incrementally — new data is clustered first, old data is rebalanced gradually by `OPTIMIZE`.

---

### Q8: What does `DENY` do that `REVOKE` does not, and when would you use it?

**A:** `REVOKE` removes a privilege that was previously granted — if the privilege never existed, `REVOKE` has no effect. `DENY` explicitly blocks access regardless of any `GRANT` — it overrides group-level grants for a specific principal. Use `DENY` when you want to exclude a specific user or service principal from a resource even though they are a member of a group that has access (e.g., a contractor in the `data-engineers` group who should not see a particular sensitive table).

---

### Q9: How do you identify data skew in the Spark UI, and how do you fix it?

**A:** In the **Stages** tab, expand a stage and look at the task duration distribution — if one or a few tasks take orders of magnitude longer than the median, that is skew. The **Summary Metrics** row (min/25th/median/75th/max) shows the imbalance clearly. Fix options: (1) Enable **AQE** (`spark.sql.adaptive.enabled=true`) — it detects and handles skew automatically by splitting skewed partitions; (2) **Salt the skewed join key** by appending a random suffix to spread data across more partitions; (3) Use a **broadcast join** if one side of the join is small enough; (4) Repartition before the join using a higher-cardinality derived key.

---

### Q10: What are the three LDP table types, and when would you choose each one?

**A:** (1) **Streaming Table** — for append-only or CDC sources (Auto Loader, Kafka, CDC streams). Data is incrementally processed; only new records are read each run. (2) **Materialized View** — for aggregations or joins that depend on the full dataset. The result is persisted and recomputed (or incrementally refreshed) on each pipeline run; best for Gold-layer analytics tables. (3) **Live View** — for lightweight transformations that do not need to be stored. The view is computed at query time from upstream tables, adding no storage cost; best for reusable business logic applied across multiple downstream consumers.

---

## 📺 Recommended Videos

| Topic | Video | Duration |
|---|---|---|
| LDP Overview | [Delta Live Tables Deep Dive (Databricks)](https://www.youtube.com/watch?v=16xnq1l1xlY) | 45 min |
| LDP Expectations | [Data Quality with DLT](https://www.youtube.com/watch?v=F75l7RQO4L8) | 20 min |
| Lakeflow Jobs | [Orchestration with Workflows](https://www.youtube.com/watch?v=BI5Y5jVwGAo) | 30 min |
| DABs Intro | [Databricks Asset Bundles](https://www.youtube.com/watch?v=K-Z_bS1Wp48) | 25 min |

---

## 📖 Official Resources

- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [APPLY CHANGES INTO (CDC)](https://docs.databricks.com/aws/en/dlt/cdc)
- [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [Declarative Automation Bundles (DABs)](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [Databricks CLI Reference](https://docs.databricks.com/aws/en/dev-tools/cli/)
- [Delta Sharing](https://docs.databricks.com/aws/en/delta-sharing/)
- [Lakehouse Federation](https://docs.databricks.com/aws/en/query-federation/)
- [Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering)
- [Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)
- [Unity Catalog Privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)
- [Spark UI Guide](https://docs.databricks.com/aws/en/compute/spark-ui)
- [Official Exam Guide (May 4, 2026)](https://www.databricks.com/sites/default/files/2026-03/databricks-certified-data-engineer-associate-exam-guide-may-4-2026.pdf)
