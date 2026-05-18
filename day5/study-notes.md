# Day 5 — Productionizing Pipelines: DLT, Workflows & CI/CD

> **Exam weight:** ~20% | **Time budget:** 8–10 hours

---

## 1. Delta Live Tables (DLT)

### What is DLT?
Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define *what* the data should look like and DLT handles *how* to run, monitor, and recover the pipeline.

**Key advantages over manual Spark jobs:**
- Automatic dependency resolution between tables
- Built-in data quality enforcement (Expectations)
- Auto-restart on failure
- Full lineage tracking

### DLT Table Types

| Type | SQL Keyword | Python Decorator | Materialized? | Notes |
|------|-------------|-----------------|---------------|-------|
| Streaming Table | `CREATE OR REFRESH STREAMING TABLE` | `@dlt.table` + `spark.readStream` | Yes | For append-only / streaming sources |
| Materialized View | `CREATE OR REFRESH MATERIALIZED VIEW` | `@dlt.table` + `spark.read` | Yes | Refreshed on pipeline run |
| View | `CREATE LIVE VIEW` | `@dlt.view` | No | Computed at query time, not stored |

```sql
-- Streaming Table (Bronze layer ingestion)
CREATE OR REFRESH STREAMING TABLE raw_orders
AS SELECT * FROM cloud_files("/mnt/landing/orders", "json",
  map("cloudFiles.inferColumnTypes", "true"))

-- Materialized View (Silver layer transform)
CREATE OR REFRESH MATERIALIZED VIEW clean_orders
AS SELECT
  order_id,
  CAST(order_date AS DATE) AS order_date,
  customer_id,
  amount
FROM LIVE.raw_orders
WHERE order_id IS NOT NULL

-- View (Gold layer — no storage, computed live)
CREATE LIVE VIEW daily_revenue
AS SELECT order_date, SUM(amount) AS revenue
FROM LIVE.clean_orders
GROUP BY order_date
```

```python
# Python equivalent
import dlt
from pyspark.sql.functions import col, cast

@dlt.table(comment="Raw orders from landing zone")
def raw_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/landing/orders")
    )

@dlt.table(comment="Cleaned and typed orders")
def clean_orders():
    return (
        dlt.read_stream("raw_orders")
        .filter(col("order_id").isNotNull())
        .withColumn("order_date", col("order_date").cast("date"))
    )
```

### DLT Expectations (Data Quality)

Expectations let you define data quality rules with three enforcement modes:

| Mode | SQL | Python | Behavior |
|------|-----|--------|----------|
| Warn | `CONSTRAINT ... EXPECT (cond)` | `@dlt.expect("name", cond)` | Log violations, continue |
| Drop | `EXPECT ... ON VIOLATION DROP ROW` | `@dlt.expect_or_drop("name", cond)` | Remove bad rows |
| Fail | `EXPECT ... ON VIOLATION FAIL UPDATE` | `@dlt.expect_or_fail("name", cond)` | Stop pipeline on violation |

```sql
CREATE OR REFRESH MATERIALIZED VIEW valid_orders
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date   EXPECT (order_date >= '2020-01-01') ON VIOLATION WARN
AS SELECT * FROM LIVE.raw_orders
```

```python
@dlt.table
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect("valid_date", "order_date >= '2020-01-01'")
def valid_orders():
    return dlt.read_stream("raw_orders")
```

### DLT Pipeline Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Triggered** | Run once, process all new data, stop | Batch workloads, cost-sensitive |
| **Continuous** | Always running, low latency | Near-real-time requirements |

**Development vs Production mode:**
- **Development:** Reuses cluster, faster iteration, no retry on failure
- **Production:** New cluster per run, auto-retry on failure, full isolation

### DLT Pipeline Settings (JSON)
```json
{
  "name": "orders_pipeline",
  "target": "orders_db",
  "libraries": [{"notebook": {"path": "/Repos/user/project/pipeline"}}],
  "clusters": [{"num_workers": 2}],
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "continuous": false
}
```

### DLT Editions
| Edition | Features |
|---------|----------|
| **Core** | Streaming tables, materialized views |
| **Pro** | + Change Data Capture (CDC) |
| **Advanced** | + Enhanced autoscaling, expectations in events |

---

## 2. Lakeflow Jobs (Databricks Workflows)

### Overview
Lakeflow Jobs (formerly Databricks Workflows) orchestrates multi-step pipelines. Each unit of work is a **Task**, and you connect tasks with dependencies.

### Task Types
| Task Type | Description |
|-----------|-------------|
| Notebook | Run a Databricks notebook |
| Python Script | Run a `.py` file from a repo |
| DLT Pipeline | Trigger a DLT pipeline |
| SQL | Run SQL queries / dashboards |
| dbt | Run a dbt project |
| Spark Submit | Low-level JAR/Python submit |
| Run Job | Trigger another job (parent-child) |

### Task Dependencies
Tasks run in order based on `depends_on`:
```
ingest_data → validate_data → transform_data → export_report
                    ↓
               alert_on_failure  (runs only if validate_data fails)
```

**Dependency types:**
- Default: downstream runs after upstream succeeds
- `ALL_DONE`: runs regardless of upstream result
- `AT_LEAST_ONE_SUCCESS`: runs if any upstream succeeded

### Job Triggers
| Trigger | Description |
|---------|-------------|
| Manual | Click "Run Now" |
| Scheduled (Cron) | `0 2 * * *` = every day at 2 AM |
| File Arrival | Triggered when file lands in a path |
| Continuous | Re-runs immediately after completion |

**Cron syntax for Databricks:**
```
Seconds  Minutes  Hours  Day-of-Month  Month  Day-of-Week  Year
  0        0       6        *            *       Mon-Fri      *
```
= Every weekday at 6:00 AM UTC

### Job Parameters
```python
# Access job parameters in a notebook
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

# Or via task values (pass data between tasks)
# In task 1 — set a value:
dbutils.jobs.taskValues.set(key="record_count", value=1000)

# In task 2 — get that value:
count = dbutils.jobs.taskValues.get(taskKey="task1", key="record_count")
```

### Repair & Re-run
- **Repair run:** Re-run only failed/skipped tasks without re-running successful ones
- Preserves all task outputs from successful tasks
- Available via UI or API: `POST /api/2.1/jobs/runs/repair`

### Job Compute Options
| Option | Description | Cost |
|--------|-------------|------|
| Job Cluster | New cluster per run | Higher startup, lower idle |
| Existing All-Purpose Cluster | Reuse interactive cluster | Fast start, expensive if idle |
| Serverless | Auto-managed, instant start | Premium per DBU |

---

## 3. Databricks Asset Bundles (DABs)

### What are DABs?
DABs are the Databricks-native CI/CD system. They let you define pipelines, jobs, notebooks, and configurations as **YAML** files checked into Git — enabling proper DevOps practices.

### Bundle Structure
```
my_project/
├── databricks.yml          ← Root bundle config
├── resources/
│   ├── my_job.yml          ← Job definition
│   └── my_pipeline.yml     ← DLT pipeline definition
├── src/
│   ├── notebook.ipynb
│   └── pipeline.sql
└── tests/
    └── test_transforms.py
```

**`databricks.yml` example:**
```yaml
bundle:
  name: orders_etl

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-xxxx.azuredatabricks.net

  prod:
    mode: production
    workspace:
      host: https://adb-yyyy.azuredatabricks.net
    run_as:
      service_principal_name: prod-sp@company.com

resources:
  jobs:
    orders_job:
      name: orders_etl_${bundle.target}
      include:
        - resources/my_job.yml
```

**Deploy and run:**
```bash
# Authenticate
databricks configure --token

# Deploy to dev
databricks bundle deploy --target dev

# Run the job
databricks bundle run --target dev orders_job

# Deploy to production
databricks bundle deploy --target prod
```

### CI/CD with GitHub Actions
```yaml
# .github/workflows/deploy.yml
name: Deploy to Databricks

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks bundle deploy --target prod
```

### Databricks Repos (Git Integration)
- **Repos** = Git-connected folders in the workspace
- Support: GitHub, GitLab, Bitbucket, Azure DevOps
- Operations: clone, pull, push, branch, create PR
- **Sparse checkout**: Only check out specific folders from a large repo

---

## 4. Monitoring & Alerting

### Job Monitoring
- **Email notifications:** on start, success, failure, no-new-runs
- **Slack/Webhook:** via System Notification Destinations
- **Job run history:** UI → Jobs → Runs → view logs, duration, errors

### DLT Monitoring
- **Event log table:** `event_log("<pipeline_id>")` — queryable Delta table
  ```sql
  SELECT timestamp, level, message
  FROM event_log("my-pipeline-id")
  WHERE level IN ('ERROR', 'WARN')
  ORDER BY timestamp DESC
  LIMIT 50
  ```
- **Expectation metrics:** track pass/fail rates per expectation per pipeline run
- **Data quality tab:** visual dashboard in DLT UI

### Databricks SQL Alerts
```
Databricks SQL → Alerts → New Alert
→ Set query + threshold condition
→ Configure notification destination (Email/Slack)
→ Schedule: every 1 hour
```

---

## 5. Exam-Focused Key Points

### DLT Must-Know
- `LIVE.table_name` syntax for referencing tables within the same pipeline
- `dlt.read()` for batch reads, `dlt.read_stream()` for streaming reads within DLT
- Expectations are per-table, not per-pipeline
- A pipeline can mix streaming tables and materialized views
- **Change Data Capture (CDF) in DLT:** `APPLY CHANGES INTO` for CDC streams

### Workflows Must-Know
- **Repair run** = re-run only failed tasks (not the whole job)
- Job clusters are terminated after the job run completes
- `dbutils.jobs.taskValues` = pass data between tasks
- Multiple trigger types: scheduled, file arrival, continuous, manual

### DABs Must-Know
- `databricks.yml` = root config file
- `targets` section defines dev/staging/prod environments
- `mode: development` vs `mode: production` affects cluster reuse and error handling

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


---

## 6. Photon Engine

Photon is Databricks' **native vectorized query engine** written in C++. It's a drop-in replacement for the standard Apache Spark SQL engine and runs automatically when available.

### What is Photon?
- A **C++ implementation** of the Spark SQL execution engine
- Processes data in columnar batches (SIMD vectorization) instead of row-by-row
- **Transparent** — no code changes required; existing SQL and DataFrame code benefits automatically
- Available on clusters using **Databricks Runtime** (not open-source Spark)

### When Does Photon Help?
| Workload | Photon Benefit |
|---|---|
| Large table scans | Large (avoids row-by-row overhead) |
| SQL aggregations (GROUP BY, COUNT, SUM) | Large |
| Joins on large tables | Large |
| Delta writes / OPTIMIZE / VACUUM | Medium |
| String-heavy operations | Moderate |
| Complex Python UDFs | ❌ No benefit (Python UDFs bypass Photon) |

### How to Enable Photon
```
Cluster config → Enable Photon Acceleration (checkbox)
  OR
Cluster DBR >= 9.1 (Photon available on all DBR >= 9.1)
```

```python
# Check if Photon is active in SparkUI or via:
spark.conf.get("spark.databricks.photon.enabled")  # True if enabled
```

### Key Exam Points
- Photon is **only available on Databricks** (not open-source Spark)
- Photon is **enabled per cluster**, not per query
- Photon **does NOT help** with Python UDFs (they escape to Python process)
- Photon accelerates **SQL and DataFrame API** workloads
- Photon uses **DBU (Databricks Unit)** at a different rate than standard clusters (check pricing)

---

## 7. Serverless Compute

Serverless compute eliminates the need to configure and manage cluster infrastructure. Databricks automatically provisions resources and scales them.

### Types of Serverless Compute

| Type | Purpose | Notes |
|---|---|---|
| **Serverless SQL Warehouses** | Databricks SQL queries, BI tools | Scales to zero; instant startup |
| **Serverless Jobs** | Automated job runs | Auto-scales; no cluster config needed |
| **Serverless DLT** | Delta Live Tables pipelines | Managed infrastructure |
| **Serverless Notebooks** | Interactive notebook execution | GA in Databricks as of 2024 |

### Serverless vs Classic Clusters

| Feature | Classic Clusters | Serverless |
|---|---|---|
| **Startup time** | 3–8 minutes | < 10 seconds |
| **Configuration** | Manual (VM type, workers, etc.) | Automatic |
| **Scaling** | Auto-scaling (slow) | Instant |
| **Cost model** | Pay for cluster lifetime | Pay per query/task execution time |
| **Idle cost** | High (cluster stays on) | Zero (scales to zero) |
| **Customization** | Full (init scripts, instance types) | Limited |
| **Unity Catalog** | Required for row/column security | Full support |

### Serverless SQL Warehouses
```
Databricks SQL → SQL Warehouses → Create Warehouse
  → Type: Serverless
  → Size: Small / Medium / Large (auto-scales)
  → Auto Stop: 10 min (configurable)
```

> 🔑 **Exam tip:** Serverless SQL Warehouses are the **recommended default** for new Databricks SQL deployments. They start instantly and cost nothing when idle.

---

## 8. APPLY CHANGES INTO — CDC in DLT

`APPLY CHANGES INTO` is DLT's built-in Change Data Capture (CDC) mechanism. It processes CDC streams (inserts, updates, deletes) and maintains a Silver table with current state.

### When to Use
- Source emits CDC events: `INSERT`, `UPDATE`, `DELETE` with a sequence key
- You want DLT to automatically manage the current state of a target table
- Typical use case: CDC from databases (Debezium, Kafka CDC, DMS)

### Syntax

```sql
-- SQL DLT: APPLY CHANGES INTO
CREATE OR REFRESH STREAMING TABLE silver_customers;

APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_customers_cdc)
KEYS (customer_id)                     -- primary key for matching
SEQUENCE BY update_timestamp           -- determines latest version
COLUMNS * EXCEPT (_rescued_data)       -- columns to include
STORED AS SCD TYPE 1;                  -- SCD1 = overwrite, SCD2 = keep history
```

```python
# Python DLT: APPLY CHANGES INTO
import dlt
from pyspark.sql.functions import col

dlt.create_streaming_table("silver_customers")

dlt.apply_changes(
    target="silver_customers",
    source="bronze_customers_cdc",
    keys=["customer_id"],
    sequence_by=col("update_timestamp"),
    stored_as_scd_type=1  # or 2 for history
)
```

### SCD Type 1 vs SCD Type 2

| Feature | SCD Type 1 | SCD Type 2 |
|---|---|---|
| **Updates** | Overwrite current record | Add new record, keep old |
| **History** | No | Yes (full history preserved) |
| **Use case** | Current state only | Audit trail, time-based analysis |
| **DLT keyword** | `STORED AS SCD TYPE 1` | `STORED AS SCD TYPE 2` |

> ⚠️ **Exam trap:** `APPLY CHANGES INTO` requires **DLT Pro or Advanced** edition (not Core). It's only available within DLT pipelines — you cannot use it in regular notebooks.

---

## 9. Databricks Runtime (DBR) Versions

### DBR Flavors

| Runtime | Description | Use Case |
|---|---|---|
| **DBR** (Standard) | Apache Spark + Databricks optimizations + common libs | General-purpose ETL |
| **DBR ML** | Standard + MLflow, scikit-learn, TensorFlow, PyTorch | ML model training |
| **DBR Photon** | Standard + Photon engine | SQL-heavy workloads |
| **DBR GPU** | Standard + CUDA, cuDF, cuML | Deep learning, GPU workloads |

### Key DBR Facts
- DBR version = `15.x` format (e.g., DBR 15.4 LTS)
- **LTS** = Long-Term Support (maintained 2+ years) — recommended for production
- Databricks auto-upgrades cluster DBR only when you recreate the cluster
- Job clusters automatically get the DBR version specified in the job config

```python
# Check current runtime version
import databricks.sdk
print(spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion"))
```

---

## 10. Databricks SQL (DBSQL) — Complete Picture

### SQL Warehouse Types

| Type | Description | When to Use |
|---|---|---|
| **Serverless** | Databricks-managed, instant start, auto-scale | Default for new deployments |
| **Pro** | Customer VPC, predictable latency | Compliance, security requirements |
| **Classic** | Legacy, runs on customer cluster | Legacy only |

### Databricks SQL Features
- **SQL Editor:** Write and run SQL queries, save as named queries
- **Dashboards:** Visualize query results with charts, tables, counters
- **Alerts:** Set thresholds on query results, notify via email/Slack
- **Query History:** View all past queries, filter by user/status
- **Query Profiles:** Visual execution plan (like EXPLAIN EXTENDED)

### Query Optimization in DBSQL
```sql
-- Use EXPLAIN to view query plan
EXPLAIN SELECT * FROM orders WHERE customer_id = '123';
EXPLAIN FORMATTED SELECT ...;  -- more detailed

-- Use CACHE TABLE for repeated access
CACHE TABLE orders;             -- caches in Spark memory
UNCACHE TABLE orders;           -- releases from cache

-- Check table statistics (used by query optimizer)
ANALYZE TABLE orders COMPUTE STATISTICS;
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date;
```

---

## 11. Updated Exam-Focused Key Points

### New Must-Know Topics
- ✅ **Photon** = C++ vectorized engine; enabled per cluster; no code changes; doesn't help Python UDFs
- ✅ **Serverless SQL Warehouse** = instant startup, scales to zero, recommended for DBSQL
- ✅ **APPLY CHANGES INTO** = DLT CDC; requires Pro/Advanced edition; handles SCD Type 1 and 2
- ✅ **SCD Type 1** = overwrite (latest state only); **SCD Type 2** = keep full history
- ✅ **DBR LTS** = recommended for production (long-term support)
- ✅ **Serverless vs Classic clusters**: Serverless = instant, no config, scales to zero
- ✅ **ANALYZE TABLE** = updates column statistics for better query optimization
- ✅ **`CACHE TABLE`** = stores table in Spark in-memory cache for repeated queries
