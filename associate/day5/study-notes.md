# Day 5 — Productionizing Pipelines: DLT, Workflows & CI/CD

> **Exam weight:** ~20% | **Time budget:** 8–10 hours
> **Environment:** Full production Databricks workspace (Azure, AWS, or GCP)

---

## 1. Delta Live Tables (DLT)

### What is DLT?
Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define *what* the data should look like and DLT handles *how* to run, monitor, and recover the pipeline.

**Key advantages over manual Spark jobs:**
- Automatic dependency resolution between tables
- Built-in data quality enforcement (Expectations)
- Auto-restart on failure
- Full lineage tracking in Unity Catalog
- Native support for Change Data Capture (CDC)

### DLT Table Types

| Type | SQL Keyword | Python Decorator | Materialized? | Notes |
|------|-------------|-----------------|---------------|-------|
| Streaming Table | `CREATE OR REFRESH STREAMING TABLE` | `@dlt.table` + `spark.readStream` | Yes | For append-only / streaming sources |
| Materialized View | `CREATE OR REFRESH MATERIALIZED VIEW` | `@dlt.table` + `spark.read` | Yes | Refreshed on pipeline run |
| View | `CREATE LIVE VIEW` | `@dlt.view` | No | Computed at query time, not stored |

```sql
-- Bronze: Streaming Table — Auto Loader from ADLS Gen2
CREATE OR REFRESH STREAMING TABLE raw_orders
COMMENT 'Raw orders ingested from ADLS Gen2 landing zone via Auto Loader'
AS SELECT * FROM cloud_files(
  "abfss://landing@<storage-account>.dfs.core.windows.net/orders/",
  "json",
  map("cloudFiles.inferColumnTypes", "true",
      "cloudFiles.schemaLocation", "abfss://checkpoints@<storage-account>.dfs.core.windows.net/raw_orders_schema")
)

-- Silver: Materialized View — clean and type-cast
CREATE OR REFRESH MATERIALIZED VIEW clean_orders
COMMENT 'Cleaned and typed orders from bronze layer'
AS SELECT
  order_id,
  CAST(order_date AS DATE)    AS order_date,
  customer_id,
  CAST(amount AS DECIMAL(10,2)) AS amount,
  status
FROM LIVE.raw_orders
WHERE order_id IS NOT NULL

-- Gold: View — no storage, computed live
CREATE LIVE VIEW daily_revenue
COMMENT 'Daily revenue rollup — computed at query time'
AS SELECT
  order_date,
  SUM(amount) AS revenue,
  COUNT(*)    AS order_count
FROM LIVE.clean_orders
GROUP BY order_date
```

```python
# Python DLT equivalent
import dlt
from pyspark.sql.functions import col

@dlt.table(
    comment="Raw orders from ADLS Gen2 landing zone",
    table_properties={"pipelines.reset.allowed": "false"}  # prevent accidental full refresh
)
def raw_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
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

### DLT Expectations (Data Quality)

Expectations let you define data quality rules with three enforcement modes:

| Mode | SQL | Python | Behavior |
|------|-----|--------|----------|
| Warn | `CONSTRAINT ... EXPECT (cond)` | `@dlt.expect("name", cond)` | Log violations, continue processing |
| Drop | `EXPECT ... ON VIOLATION DROP ROW` | `@dlt.expect_or_drop("name", cond)` | Remove bad rows silently |
| Fail | `EXPECT ... ON VIOLATION FAIL UPDATE` | `@dlt.expect_or_fail("name", cond)` | Stop the entire pipeline |

```sql
CREATE OR REFRESH MATERIALIZED VIEW valid_orders
  CONSTRAINT valid_amount  EXPECT (amount > 0)               ON VIOLATION DROP ROW,
  CONSTRAINT valid_date    EXPECT (order_date >= '2020-01-01') ON VIOLATION WARN,
  CONSTRAINT no_null_id    EXPECT (order_id IS NOT NULL)       ON VIOLATION FAIL UPDATE
AS SELECT * FROM LIVE.raw_orders
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

### DLT Pipeline Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Triggered** | Runs once, processes all new data, cluster terminates | Batch workloads, cost-sensitive |
| **Continuous** | Always running, sub-minute latency | Near-real-time requirements |

**Development vs Production mode:**
- **Development:** Reuses cluster between runs, faster iteration, no automatic retry on failure
- **Production:** Provisions a fresh cluster per run, auto-retry on failure, full resource isolation

### DLT Pipeline Settings — Production-Grade JSON

```json
{
  "name": "orders_pipeline_prod",
  "target": "prod_catalog.orders_db",
  "libraries": [
    {"notebook": {"path": "/Repos/team/project/pipelines/orders_pipeline"}}
  ],
  "clusters": [
    {
      "label": "default",
      "num_workers": 4,
      "node_type_id": "Standard_DS3_v2",
      "spark_conf": {
        "spark.databricks.delta.optimizeWrite.enabled": "true"
      },
      "autoscale": {
        "min_workers": 2,
        "max_workers": 8,
        "mode": "ENHANCED"
      }
    }
  ],
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "continuous": false,
  "photon": true,
  "permissions": [
    {"group_name": "data-engineers", "permission_level": "CAN_MANAGE"},
    {"group_name": "analysts",       "permission_level": "CAN_VIEW"}
  ],
  "notifications": [
    {
      "email_recipients": ["data-eng-alerts@company.com"],
      "alerts": ["on-update-failure", "on-flow-failure"]
    }
  ]
}
```

### DLT Editions

| Edition | Features |
|---------|----------|
| **Core** | Streaming tables, materialized views, basic expectations |
| **Pro** | + Change Data Capture (`APPLY CHANGES INTO`) |
| **Advanced** | + Enhanced autoscaling, expectation metrics in event log |

### DLT Schema Evolution (Auto Loader)

Auto Loader handles schema evolution automatically via `cloudFiles.schemaEvolutionMode`:

| Mode | Behavior |
|------|----------|
| `addNewColumns` (default) | New source columns are added to the target schema |
| `rescue` | Unknown columns go into a `_rescued_data` JSON column |
| `failOnNewColumns` | Pipeline fails if source schema changes |
| `none` | Schema changes are ignored |

```sql
-- Force schema evolution to rescue mode
CREATE OR REFRESH STREAMING TABLE raw_events
TBLPROPERTIES ("delta.columnMapping.mode" = "name")
AS SELECT * FROM cloud_files(
  "abfss://landing@<storage>.dfs.core.windows.net/events/",
  "json",
  map("cloudFiles.schemaEvolutionMode", "rescue",
      "cloudFiles.schemaLocation",      "abfss://checkpoints@<storage>.dfs.core.windows.net/events_schema")
)
```

> 🔑 **Key difference vs Structured Streaming:** In vanilla Structured Streaming, schema changes require you to manually update the schema and restart the stream. DLT with Auto Loader handles this automatically using `schemaLocation` checkpointing — the schema is persisted and evolved in place.

---

## 2. Lakeflow Jobs (Databricks Workflows)

### Overview
Lakeflow Jobs (formerly Databricks Workflows) orchestrates multi-step pipelines. Each unit of work is a **Task**, connected via dependencies that form a DAG.

### Task Types

| Task Type | Description |
|-----------|-------------|
| Notebook | Run a Databricks notebook |
| Python Script | Run a `.py` file from a Git repo |
| DLT Pipeline | Trigger a DLT pipeline as a task |
| SQL | Run SQL queries or Databricks SQL dashboards |
| dbt | Run a dbt Cloud or dbt Core project |
| Spark Submit | Low-level JAR or Python submit |
| Run Job | Trigger another job (parent-child orchestration) |

### Task Dependencies and Execution Conditions

```
ingest_data → validate_data → transform_data → export_report
                   ↓
            alert_on_failure  (runs on ALL_DONE if validate_data failed)
```

| Condition | Meaning |
|-----------|---------|
| `ALL_SUCCESS` (default) | Downstream runs only if all upstream tasks succeeded |
| `ALL_DONE` | Downstream runs regardless of upstream result |
| `AT_LEAST_ONE_SUCCESS` | Downstream runs if any upstream task succeeded |
| `AT_LEAST_ONE_FAILED` | Downstream runs only if at least one upstream failed |

### Job Triggers

| Trigger | Description |
|---------|-------------|
| Manual | Click "Run Now" in UI or `POST /api/2.1/jobs/run-now` |
| Scheduled (Cron) | Quartz cron, e.g. `0 0 6 * * ?` = daily at 6:00 AM |
| File Arrival | Triggered when a file lands in a configured path |
| Continuous | Re-runs immediately after completion |
| Webhook / API | Triggered by external systems via REST API |

**Cron syntax for Databricks (Quartz format — 7 fields):**
```
Seconds  Minutes  Hours  Day-of-Month  Month  Day-of-Week  Year
   0        0       6         *           *      Mon-Fri      *
```
= Every weekday at 06:00 UTC

### Job Parameters and Task Values

```python
# ── In a notebook task: read job parameters via widgets ──
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
print(f"Running in environment: {env}")

# ── Task Values: pass data between tasks within the same run ──
# Task 1 (ingest_raw): set a value
dbutils.jobs.taskValues.set(key="record_count", value=1_250_000)

# Task 2 (validate_data): read value from task 1
count = dbutils.jobs.taskValues.get(
    taskKey="ingest_raw",
    key="record_count",
    default=0,
    debugValue=9999  # used only when running interactively (not in a job run)
)
print(f"Records from ingest: {count:,}")
```

### Repair & Re-run

- **Repair run:** Re-runs only failed and downstream-skipped tasks, preserving all outputs from already-succeeded tasks
- Invoked via UI ("Repair Run" button) or API: `POST /api/2.1/jobs/runs/repair`
- Repair runs **do not create a new run ID** — they extend the existing run

### Job Compute Options

| Option | Description | Best For |
|--------|-------------|----------|
| Job Cluster | New cluster provisioned per run, terminated on completion | Production isolation, cost control |
| All-Purpose Cluster | Reuse an existing interactive cluster | Development/testing only |
| Serverless | Databricks-managed, instant startup | Ad-hoc and bursty workloads |

---

## 3. Databricks Asset Bundles (DABs)

### What are DABs?
DABs are the Databricks-native CI/CD framework. Define pipelines, jobs, notebooks, and cluster configs as **YAML** files committed to Git — enabling proper DevOps lifecycle management.

### Bundle Structure

```
my_project/
├── databricks.yml            ← Root bundle config
├── resources/
│   ├── orders_job.yml        ← Job definition
│   └── orders_pipeline.yml   ← DLT pipeline definition
├── src/
│   ├── ingest.py
│   ├── transform.sql
│   └── pipelines/
│       └── orders_pipeline.ipynb
└── tests/
    └── test_transforms.py
```

**`databricks.yml` — production-grade example:**
```yaml
bundle:
  name: orders_etl

variables:
  catalog:
    description: Unity Catalog target catalog
    default: dev_catalog

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<dev-id>.azuredatabricks.net
    variables:
      catalog: dev_catalog

  staging:
    mode: production
    workspace:
      host: https://adb-<staging-id>.azuredatabricks.net
    variables:
      catalog: staging_catalog
    run_as:
      service_principal_name: sp-staging@company.com

  prod:
    mode: production
    workspace:
      host: https://adb-<prod-id>.azuredatabricks.net
    variables:
      catalog: prod_catalog
    run_as:
      service_principal_name: sp-prod@company.com

resources:
  jobs:
    orders_job:
      name: orders_etl_${bundle.target}
      include:
        - resources/orders_job.yml
  pipelines:
    orders_pipeline:
      name: orders_pipeline_${bundle.target}
      include:
        - resources/orders_pipeline.yml
```

**Deploy and run (updated Databricks CLI v0.200+):**
```bash
# Authenticate (OAuth or PAT)
databricks auth login --host https://adb-<workspace-id>.azuredatabricks.net

# Validate bundle configuration
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run the job interactively
databricks bundle run --target dev orders_job

# Deploy to production
databricks bundle deploy --target prod

# Destroy a dev deployment (cleanup)
databricks bundle destroy --target dev
```

### CI/CD with GitHub Actions (Updated CLI v2)

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
        uses: databricks/setup-cli@main   # official action — no pip install needed

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

      - name: Run Integration Tests
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle run --target prod orders_job --no-wait
```

> ⚠️ **Deprecated:** `pip install databricks-cli` (the old Python CLI). Always use `databricks/setup-cli@main` (the new Go-based CLI, v0.200+) in CI/CD pipelines.

### Databricks Repos (Git Integration)

- **Repos** = Git-connected folders in the workspace
- Supported Git providers: GitHub, GitLab, Bitbucket, Azure DevOps
- Operations: clone, pull, push, branch, create PR — all from the Databricks UI
- **Sparse checkout**: Only sync specific subdirectories from a monorepo
- Repos integrate with Unity Catalog for access control on notebook execution

---

## 4. Monitoring & Alerting

### Job Monitoring

- **Email notifications:** on start, success, failure, skipped, no-new-runs
- **Webhook / Slack:** via System Notification Destinations in workspace admin settings
- **Job run history:** UI → Jobs → Runs → view logs, duration, cluster metrics, errors
- **Metrics API:** `GET /api/2.1/jobs/runs/list` to fetch run history programmatically

### DLT Monitoring — Event Log

The DLT event log is a queryable Delta table exposed via the `event_log()` function:

```sql
-- Query the event log for a pipeline (use pipeline ID from DLT UI)
SELECT
  timestamp,
  event_type,
  level,
  details:flow_progress.metrics.num_output_rows  AS output_rows,
  details:flow_progress.data_quality.expectations AS expectations_json,
  message
FROM event_log("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
WHERE level IN ('ERROR', 'WARN')
  AND timestamp >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY timestamp DESC
LIMIT 100;
```

```sql
-- Monitor expectation pass/fail rates per pipeline run
SELECT
  timestamp,
  details:flow_progress.data_quality.expectations[0].name        AS expectation_name,
  details:flow_progress.data_quality.expectations[0].passed_records AS passed,
  details:flow_progress.data_quality.expectations[0].failed_records AS failed
FROM event_log("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```

### Databricks SQL Alerts

```
Databricks SQL → Alerts → New Alert
  → Attach a saved SQL query
  → Set threshold: e.g., "Value > 1000"
  → Configure Notification Destination (Email / Slack / PagerDuty)
  → Schedule: every 15 minutes / hourly / daily
```

---

## 5. APPLY CHANGES INTO — CDC in DLT

`APPLY CHANGES INTO` is DLT's built-in Change Data Capture (CDC) mechanism. It processes a CDC stream (inserts, updates, deletes) and automatically maintains the current state of a target Silver table.

### When to Use

- Source emits CDC events with an `operation` column (`INSERT`, `UPDATE`, `DELETE`) and a sequence key
- You want DLT to automatically handle upserts/deletes without writing MERGE INTO logic
- Typical sources: Debezium on Kafka, AWS DMS, Azure DMS, Fivetran CDC

### Syntax

```sql
-- Step 1: Declare the target streaming table first
CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT 'Current state of customers, maintained via CDC';

-- Step 2: Define the CDC logic
APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_customers_cdc)
KEYS (customer_id)                        -- primary key(s) for record matching
APPLY AS DELETE WHEN operation = 'DELETE' -- mark deletes explicitly
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'
SEQUENCE BY updated_at                    -- determines ordering (latest wins)
COLUMNS * EXCEPT (_rescued_data, operation, updated_at)  -- columns to propagate
STORED AS SCD TYPE 1;                     -- SCD1=overwrite, SCD2=keep history
```

```python
# Python equivalent
import dlt
from pyspark.sql.functions import col

dlt.create_streaming_table(
    name="silver_customers",
    comment="Current state of customers, maintained via CDC"
)

dlt.apply_changes(
    target          = "silver_customers",
    source          = "bronze_customers_cdc",
    keys            = ["customer_id"],
    sequence_by     = col("updated_at"),
    apply_as_deletes= col("operation") == "DELETE",
    except_column_list = ["_rescued_data", "operation", "updated_at"],
    stored_as_scd_type = 1   # use 2 for full history
)
```

### SCD Type 1 vs SCD Type 2

| Feature | SCD Type 1 | SCD Type 2 |
|---------|------------|------------|
| **Updates** | Overwrite current record in-place | Insert new record; mark old as inactive |
| **History** | No history preserved | Full history preserved |
| **Extra columns** | None | `__START_AT`, `__END_AT` (sequence key values) |
| **Use case** | Current state lookup tables | Audit trail, point-in-time analysis |
| **DLT keyword** | `STORED AS SCD TYPE 1` | `STORED AS SCD TYPE 2` |

> ⚠️ **Exam trap:** `APPLY CHANGES INTO` requires **DLT Pro or Advanced** edition — it is NOT available in Core. It is also only valid inside a DLT pipeline notebook — you cannot call it from a regular notebook or SQL editor.

---

## 6. Photon Engine

Photon is Databricks' **native vectorized query engine** written in C++. It is a drop-in replacement for the Apache Spark SQL execution engine and activates automatically when enabled on a cluster.

### When Does Photon Help?

| Workload | Photon Benefit |
|----------|---------------|
| Large table scans (Delta reads) | ✅ Large |
| SQL aggregations (GROUP BY, COUNT, SUM) | ✅ Large |
| Hash joins on large tables | ✅ Large |
| Delta writes / OPTIMIZE / VACUUM | ✅ Medium |
| String-heavy operations | ✅ Moderate |
| Complex Python UDFs | ❌ None — Python UDFs bypass the JVM and Photon entirely |
| Pandas UDFs (Arrow-optimized) | ⚡ Partial — serialization boundary still exists |

### How to Enable Photon

```
Cluster config → Enable Photon Acceleration (checkbox)
  OR
Select a DBR runtime with "Photon" in the name (e.g., "14.3 LTS (Photon)")
```

```python
# Verify Photon is active on the current cluster
spark.conf.get("spark.databricks.photon.enabled")  # returns "true"

# Check in SparkUI → SQL / DataFrame tab → look for "WholeStageCodegenTransformer"
# nodes in the query plan — these indicate Photon execution
```

> 🔑 **Exam points:** Photon is **only on Databricks** (not open-source Spark). Enabled **per cluster**. No code changes. Does NOT help Python UDFs.

---

## 7. Serverless Compute

Serverless compute removes infrastructure management. Databricks automatically provisions, scales, and terminates compute.

### Serverless vs Classic Clusters

| Feature | Classic Clusters | Serverless |
|---------|-----------------|------------|
| **Startup time** | 3–8 minutes | < 10 seconds |
| **Configuration** | Manual (VM type, workers, init scripts) | Fully automatic |
| **Scaling** | Auto-scaling (minutes) | Instant |
| **Cost model** | Pay for full cluster lifetime | Pay per task/query execution time only |
| **Idle cost** | High | Zero (scales to zero) |
| **Customization** | Full (init scripts, instance types) | Limited |
| **Unity Catalog** | Supported | Full support (required for some features) |

### Serverless SQL Warehouses

```
Databricks SQL → SQL Warehouses → Create Warehouse
  → Type: Serverless
  → Size: Small / Medium / Large (auto-scales based on concurrency)
  → Auto Stop: 10 min (configurable, minimum 5 min)
  → Spot Policy: Cost Optimized / Reliability Optimized
```

> 🔑 **Exam tip:** Serverless SQL Warehouses are the **recommended default** for Databricks SQL. They start in seconds and cost nothing when idle.

---

## 8. Databricks Runtime (DBR) Versions

### DBR Flavors

| Runtime | Description | Use Case |
|---------|-------------|----------|
| **DBR** (Standard) | Spark + Databricks optimizations + common libs | General ETL |
| **DBR ML** | Standard + MLflow, scikit-learn, PyTorch | ML model training |
| **DBR Photon** | Standard + Photon engine | SQL-heavy / BI workloads |
| **DBR GPU** | Standard + CUDA, cuDF, cuML | Deep learning |

- DBR version format: `15.4.x-scala2.12` (major.minor.patch-scala)
- **LTS** = Long-Term Support (2+ year maintenance) — always prefer LTS for production job clusters
- Job clusters use the DBR version specified in the job/pipeline config — they do not auto-upgrade

---

## 9. Databricks SQL (DBSQL)

### SQL Warehouse Types

| Type | Description | When to Use |
|------|-------------|------------|
| **Serverless** | Databricks-managed, instant start, auto-scale | Default for new deployments |
| **Pro** | Runs in customer VPC, predictable latency | Compliance, network isolation |
| **Classic** | Legacy, customer-managed cluster underneath | Legacy workloads only |

### Query Optimization

```sql
-- View query execution plan
EXPLAIN SELECT * FROM prod_catalog.orders.transactions WHERE customer_id = '123';
EXPLAIN FORMATTED SELECT ...;   -- more detailed with stats

-- Update table statistics (used by the query optimizer)
ANALYZE TABLE prod_catalog.orders.transactions COMPUTE STATISTICS;
ANALYZE TABLE prod_catalog.orders.transactions
  COMPUTE STATISTICS FOR COLUMNS customer_id, order_date, amount;

-- Cache frequently queried tables in Spark in-memory store
CACHE TABLE prod_catalog.orders.transactions;
UNCACHE TABLE prod_catalog.orders.transactions;
```

---

## 10. Unity Catalog — Governance in Production

Unity Catalog provides unified, fine-grained governance across all Databricks workspaces in a region.

### Three-Level Namespace

```
catalog  →  schema  →  table / view / function / volume
  │              │
  └── main       └── sales, marketing, finance
```

```sql
-- Full three-level qualified name
SELECT * FROM prod_catalog.sales.transactions;

-- Create catalog and schema (requires account admin / metastore admin)
CREATE CATALOG IF NOT EXISTS prod_catalog
  COMMENT 'Production data catalog';

CREATE SCHEMA IF NOT EXISTS prod_catalog.sales
  MANAGED LOCATION 'abfss://unity@<storage>.dfs.core.windows.net/prod/sales/';

-- Grant permissions
GRANT USE CATALOG ON CATALOG prod_catalog TO `data-engineers`;
GRANT USE SCHEMA  ON SCHEMA  prod_catalog.sales TO `analysts`;
GRANT SELECT      ON TABLE   prod_catalog.sales.transactions TO `analysts`;
GRANT MODIFY      ON TABLE   prod_catalog.sales.transactions TO `data-engineers`;

-- Revoke permissions
REVOKE SELECT ON TABLE prod_catalog.sales.transactions FROM `contractors`;
```

### Row-Level and Column-Level Security

```sql
-- Column masking: hide PII from non-privileged users
CREATE OR REPLACE FUNCTION prod_catalog.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-access') THEN email
  ELSE regexp_replace(email, '(^[^@]+)', '***')
END;

ALTER TABLE prod_catalog.sales.customers
  ALTER COLUMN email SET MASK prod_catalog.security.mask_email;

-- Row filter: only show rows matching user's region
CREATE OR REPLACE FUNCTION prod_catalog.security.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('global-access')
    OR region = current_user_region();  -- hypothetical function for illustration

ALTER TABLE prod_catalog.sales.transactions
  SET ROW FILTER prod_catalog.security.region_filter ON (region);
```

---

## 11. Exam-Focused Key Points

### DLT Must-Know
- `LIVE.table_name` = reference another table within the same pipeline
- `dlt.read()` for batch reads; `dlt.read_stream()` for streaming reads within DLT
- Expectations are per-table; multiple expectations on one table are independent
- A pipeline can mix streaming tables and materialized views freely
- `APPLY CHANGES INTO` = CDC; requires **Pro or Advanced** edition; not usable outside DLT
- Schema evolution in Auto Loader is controlled by `cloudFiles.schemaEvolutionMode`

### Workflows Must-Know
- **Repair run** = re-run only failed + downstream-skipped tasks, not the full job
- Job clusters terminate automatically after run completes
- `dbutils.jobs.taskValues` = typed key-value store for passing data between tasks
- Multiple trigger types: scheduled (cron), file arrival, continuous, manual, API/webhook

### DABs Must-Know
- `databricks.yml` = root config; `targets` defines dev/staging/prod environments
- `mode: development` → reuses clusters, faster iteration
- `mode: production` → new isolated cluster per run, auto-retry enabled
- New CLI: `databricks/setup-cli@main` (Go-based); **deprecated:** `pip install databricks-cli`

### Unity Catalog Must-Know
- Three-level namespace: `catalog.schema.table`
- One metastore per region, shared across all workspaces in that region
- `GRANT` / `REVOKE` for access control; column masking and row filters for PII
- Data lineage is automatically tracked — no extra configuration required

---

## 12. Interview Preparation Q&A

### Q1: When would you use `APPLY CHANGES INTO` vs `MERGE INTO`?

**A:** Use `APPLY CHANGES INTO` when you are inside a **DLT pipeline** and your source is a CDC stream (e.g., from Debezium or AWS DMS). DLT manages ordering, deduplication, and SCD history automatically. Use `MERGE INTO` when you are working **outside DLT** — in a regular notebook, a Workflow task, or a SQL editor — and you need fine-grained control over the match conditions and the specific columns to update/insert/delete. `MERGE INTO` is more flexible but requires you to handle ordering and deduplication yourself.

---

### Q2: How does DLT handle schema evolution differently from vanilla Structured Streaming?

**A:** In vanilla Structured Streaming, any schema change in the source causes the stream to fail with a `StreamingQueryException`. You must manually update the schema, delete the checkpoint, and restart. In DLT with Auto Loader, schema evolution is handled automatically via `cloudFiles.schemaLocation` — the schema is checkpointed to a cloud path and evolved in-place according to `cloudFiles.schemaEvolutionMode`. New columns can be added (`addNewColumns`), unknown columns rescued into `_rescued_data` (`rescue`), or failures triggered (`failOnNewColumns`) — all without manual intervention.

---

### Q3: What is the difference between a Triggered and Continuous DLT pipeline?

**A:** A **Triggered** pipeline runs on demand (or on a schedule), processes all data that has arrived since the last run, and then shuts down the cluster — minimising cost. A **Continuous** pipeline keeps the cluster running indefinitely, processing new data with sub-minute latency as it arrives. Use Triggered for batch workloads where a few-minute delay is acceptable. Use Continuous only when near-real-time latency is a hard requirement, because the cluster cost is constant regardless of data volume.

---

### Q4: A DLT pipeline has an expectation `ON VIOLATION DROP ROW`. 500 rows violate the constraint in one micro-batch. What happens to the pipeline?

**A:** The pipeline continues running — it does NOT fail. The 500 violating rows are silently dropped from the output table. The violation counts are recorded in the DLT event log and surfaced in the Data Quality tab of the pipeline UI. Only `ON VIOLATION FAIL UPDATE` stops the pipeline on a constraint violation.

---

### Q5: What is the purpose of `mode: development` vs `mode: production` in a Databricks Asset Bundle?

**A:** `mode: development` is designed for fast iteration: the bundle prepends `[dev <username>]` to resource names to namespace them, reuses existing clusters to avoid cold-start wait times, and does **not** auto-retry on failure (so errors surface immediately). `mode: production` provisions a fresh, isolated cluster for every run, enables automatic retry on transient failures, and uses the `run_as` service principal for least-privilege execution. You should always deploy production jobs with `mode: production` to get the isolation and retry guarantees needed for reliability.

---

### Q6: What are the three DLT table types, and when would you choose each one?

**A:** (1) **Streaming Table** — use for append-only or CDC sources (Auto Loader, Kafka, CDC streams). Data is incrementally processed; only new records are read each run. (2) **Materialized View** — use when the result depends on aggregations or joins across the full dataset. The entire result is recomputed (or incrementally refreshed where possible) on each pipeline run. (3) **Live View** — use for lightweight transformations that do not need to be stored. The view is computed at query time from its upstream tables, adding no storage cost. For exam purposes: if a question asks about incremental ingestion → Streaming Table; if it asks about a persisted aggregate → Materialized View; if it asks about a no-storage transformation → Live View.

---

### Q7: In a 5-task Workflows job (A→B→C→D→E), tasks A–C succeeded and task D failed. What does Repair Run do?

**A:** Repair Run re-executes only **task D** and any downstream tasks that were skipped as a result — in this case, **task E**. Tasks A, B, and C are not re-run; their outputs are preserved exactly as-is. This is the key operational advantage of Repair Run over a full job re-run: it saves time and avoids re-processing data that was already successfully written.

---

### Q8: Why should you avoid Python UDFs in Photon-accelerated workloads, and what is the alternative?

**A:** Python UDFs force data out of the JVM (where Photon operates) into the Python process via serialization, and then back again. This serialization boundary completely negates Photon's vectorized execution for that operation. Alternatives: (1) **Rewrite the logic in native Spark SQL or DataFrame API functions** — Photon accelerates these fully. (2) Use **Pandas UDFs (Arrow-based)** if custom logic is unavoidable — they reduce serialization overhead via Apache Arrow but still have a boundary cost. (3) Use **Scala UDFs** — they stay within the JVM and benefit from Photon where applicable.

---

## 📺 Recommended Videos

| Topic | Video | Duration |
|-------|-------|----------|
| DLT Overview | [Delta Live Tables Deep Dive (Databricks)](https://www.youtube.com/watch?v=16xnq1l1xlY) | 45 min |
| DLT Expectations | [Data Quality with DLT](https://www.youtube.com/watch?v=F75l7RQO4L8) | 20 min |
| Databricks Workflows | [Orchestration with Workflows](https://www.youtube.com/watch?v=BI5Y5jVwGAo) | 30 min |
| DABs Intro | [Databricks Asset Bundles](https://www.youtube.com/watch?v=K-Z_bS1Wp48) | 25 min |

## 📖 Recommended Reading

- [DLT Documentation](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Workflows Documentation](https://docs.databricks.com/workflows/index.html)
- [Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [APPLY CHANGES INTO (CDC)](https://docs.databricks.com/workflows/delta-live-tables/cdc.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks CLI v2 (Go)](https://docs.databricks.com/dev-tools/cli/index.html)
