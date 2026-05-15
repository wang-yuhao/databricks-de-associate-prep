# 📋 Databricks DE Associate — Complete Cheat Sheet

> Quick-reference for all exam-critical concepts. Print or keep open during study sessions.

---

## ☁️ AZURE DATABRICKS QUICK REFERENCE

### Workspace URL Format
```
https://adb-<workspace-id>.<random>.azuredatabricks.net
```

### Unity Catalog Default Namespace (this repo)
```sql
USE CATALOG training;
USE SCHEMA prep;
-- Full reference: training.prep.<table_name>
```

### Volume Paths (use instead of /tmp/)
```
/Volumes/<catalog>/<schema>/<volume>/           -- generic
/Volumes/training/prep/landing/                 -- this repo's landing zone
/Volumes/training/prep/landing/checkpoints/     -- streaming checkpoints
```

### ADLS Gen2 Access (for external tables / raw storage)
```
abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
```

### Azure Key Vault Secret Scope
```python
# Read a secret (NEVER hardcode credentials in notebooks)
dbutils.secrets.get(scope="azure-kv", key="my-secret")

# Setup: workspace URL → #secrets/createScope
#   DNS Name: https://<your-keyvault>.vault.azure.net/
#   Resource ID: /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<kv>
```

### Cluster Access Modes (Unity Catalog)
| Mode | UC Enforced | Python UDFs | Multi-user |
|---|---|---|---|
| **Single User** | ✅ | ✅ | ❌ |
| **Shared** | ✅ | ❌ | ✅ |
| **No Isolation** | ❌ (legacy) | ✅ | ✅ |

### Service Principal Auth (ADLS Gen2 via OAuth)
```python
spark.conf.set(f"fs.azure.account.auth.type.{storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage}.dfs.core.windows.net",
               dbutils.secrets.get("azure-kv", "sp-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage}.dfs.core.windows.net",
               dbutils.secrets.get("azure-kv", "sp-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
```

### Cost Controls (Azure-specific)
- Always set **Auto Termination: 30 min** on learning clusters
- Use **Single Node** (`Standard_DS3_v2`) for all practice tasks
- **Job clusters** auto-terminate → always cheaper than all-purpose for pipelines
- 14-day Premium Trial covers DBU costs

---

## 🏛️ DOMAIN 1: Databricks Intelligence Platform (~20%)

### Lakehouse Architecture

```
Lakehouse = Data Lake flexibility + Data Warehouse performance
         = ACID transactions on cloud object storage
         = Open formats (Delta Lake, Parquet, Iceberg)
         = Single source of truth for data + AI
```

### Cluster Types — CRITICAL COMPARISON

| Cluster Type | Use Case | Cost | Lifecycle |
|---|---|---|---|
| **All-Purpose** | Interactive notebooks, ad-hoc queries | Expensive | Manual stop |
| **Job Cluster** | Automated jobs/workflows | Cheaper | Auto-terminates |
| **SQL Warehouse** | SQL analytics, BI tools | Auto-scale | Auto-stop |
| **Instance Pools** | Reduce cluster start time | Pre-paid VMs | Persistent pool |

> 📌 **Exam tip:** Job clusters are CHEAPER than all-purpose. Pools REDUCE startup time.

### Unity Catalog 3-Level Namespace

```
catalog.schema.table
   │       │      └── Table, View, Materialized View, Streaming Table
   │       └───────── Schema (= database)
   └───────────────── Catalog (top-level container)

Example: training.prep.sales_large
```

### Unity Catalog Objects Hierarchy
```
Metastore (one per region)
  └── Catalog
        └── Schema
              ├── Table (Delta, external, or foreign)
              ├── View
              ├── Materialized View (DLT)
              ├── Streaming Table (DLT)
              ├── Function
              └── Volume  ← file storage inside UC
```

### Storage Hierarchy on Azure
```
Azure Data Lake Storage Gen2 (ADLS)
  └── Container (= filesystem)
        └── Folder path
              └── Files (Parquet, JSON, CSV, Delta log)

Mapped to Unity Catalog via:
  External Location → Storage Credential (Service Principal or Managed Identity)
  Volume → /Volumes/<catalog>/<schema>/<volume>/<path>
```

---

## 📥 DOMAIN 2: Development & Ingestion (~25%)

### Auto Loader — Incremental File Ingestion

```python
# MINIMAL: read JSON from a Volume incrementally
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/schema/my_source")
    .load("/Volumes/training/prep/landing/source_files/")
)

# Write to UC managed Delta table
(df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/Volumes/training/prep/landing/checkpoints/my_query")
   .trigger(availableNow=True)
   .toTable("training.prep.bronze_table")
)
```

> 📌 **Exam tip:** Auto Loader = `format("cloudFiles")`. Schema stored in `schemaLocation`. 
> On Azure, use `/Volumes/` or `abfss://` paths — NOT `/tmp/`.

### Write Modes

| Mode | Behavior | Use Case |
|---|---|---|
| `overwrite` | Replaces ALL existing data | Full refresh |
| `append` | Adds rows, never modifies | Event/log data |
| `ignore` | Does nothing if table exists | Idempotent init |
| `error` | Fails if table exists (default) | Safety check |

### PySpark Key Operations

```python
# Read from Unity Catalog table (preferred on Azure)
df = spark.table("training.prep.sales")

# Transformations
df.select("col1", "col2")
df.filter(col("amount") > 0)
df.withColumn("new_col", col("a") + col("b"))
df.groupBy("dept").agg(count("*"), avg("salary"))
df.join(other_df, "id", "left")
df.na.fill({"col": "default"})   # fill nulls
df.drop_duplicates(["id"])

# Save to UC table
df.write.mode("overwrite").saveAsTable("training.prep.result_table")
```

### Window Functions

```python
from pyspark.sql.functions import rank, row_number, lag, dense_rank
from pyspark.sql import Window

w = Window.partitionBy("dept").orderBy(col("salary").desc())
df.withColumn("rank",         rank().over(w))
  .withColumn("row_num",      row_number().over(w))
  .withColumn("dense_rank",   dense_rank().over(w))
  .withColumn("prev_salary",  lag("salary", 1).over(w))
```

---

## 🏔️ DOMAIN 3: Delta Lake & Streaming (~20%)

### Delta Lake Operations

```sql
-- Create managed table (UC handles storage — no LOCATION needed)
CREATE TABLE training.prep.sales (
  id BIGINT, name STRING, amount DOUBLE
) USING DELTA;

-- CRUD
INSERT INTO training.prep.sales VALUES (1, 'Alice', 100.0);
UPDATE training.prep.sales SET amount = 120.0 WHERE id = 1;
DELETE FROM training.prep.sales WHERE id = 1;

-- MERGE (upsert)
MERGE INTO training.prep.sales AS t
USING training.prep.updates AS s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET t.amount = s.amount
WHEN NOT MATCHED THEN INSERT *;

-- Time Travel
SELECT * FROM training.prep.sales VERSION AS OF 2;
SELECT * FROM training.prep.sales TIMESTAMP AS OF '2024-01-15';
RESTORE TABLE training.prep.sales TO VERSION AS OF 1;

-- Optimization
OPTIMIZE training.prep.sales ZORDER BY (region, sale_date);
VACUUM training.prep.sales RETAIN 168 HOURS;  -- never 0 in production!

-- Liquid Clustering (modern replacement for ZORDER)
CREATE TABLE training.prep.sales_v2
  CLUSTER BY (region, sale_date)
AS SELECT * FROM training.prep.sales;
OPTIMIZE training.prep.sales_v2;  -- incremental re-cluster

-- Transaction history
DESCRIBE HISTORY training.prep.sales;
```

### Delta Table Properties

```sql
-- Enable Change Data Feed
ALTER TABLE training.prep.sales
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Read changes
SELECT * FROM table_changes('training.prep.sales', 1)
ORDER BY _commit_version, _change_type;
-- _change_type: insert | update_preimage | update_postimage | delete
```

### Structured Streaming

```python
# Read stream from Delta table (UC)
source = spark.readStream.format("delta").table("training.prep.events")

# Write stream to Delta table (UC) — checkpoint in Volume
(source.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", "/Volumes/training/prep/landing/checkpoints/my_query")
   .trigger(availableNow=True)   # or processingTime='30 seconds'
   .toTable("training.prep.events_sink")
)
```

### Trigger Types

| Trigger | Code | Behaviour |
|---|---|---|
| `availableNow` | `.trigger(availableNow=True)` | Process all pending, stop. Replaces `once`. |
| `processingTime` | `.trigger(processingTime='30s')` | Micro-batch every N seconds |
| `once` | `.trigger(once=True)` | **DEPRECATED** — use `availableNow` |
| `continuous` | `.trigger(continuous='500ms')` | True streaming, limited ops |

### Windowed Aggregation + Watermark

```python
from pyspark.sql.functions import window, col

(stream
 .withWatermark("event_time", "10 minutes")   # max late data tolerance
 .groupBy(
     window(col("event_time"), "5 minutes"),   # 5-min tumbling window
     col("user_id")
 )
 .agg(count("*").alias("cnt"))
 .writeStream
 .outputMode("append")   # use append with watermark
 ...
)
```

### Output Modes

| Mode | Description | When to Use |
|---|---|---|
| `append` | Only new rows written | Most cases, watermarked agg |
| `complete` | Full result re-written | Aggregations without watermark |
| `update` | Only changed rows written | Aggregations, no Delta sink |

---

## 🚀 DOMAIN 4: Productionizing Pipelines (~20%)

### Delta Live Tables (DLT) — Python Syntax

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

# Streaming Table (Bronze) — append-only
@dlt.table(comment="Bronze: raw events")
def raw_events():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/dlt_schema")
        .load("/Volumes/training/prep/landing/events/")
    )

# Materialized View (Silver) with expectations
@dlt.table(comment="Silver: valid events")
@dlt.expect_or_drop("valid_id",     "id IS NOT NULL")
@dlt.expect_or_drop("positive_amt", "amount > 0")
@dlt.expect(         "recent",      "event_date >= '2020-01-01'")
def valid_events():
    return (
        dlt.read_stream("raw_events")          # read from Bronze
        .withColumn("processed_at", current_timestamp())
    )

# Materialized View (Gold) — batch aggregation
@dlt.table(comment="Gold: daily revenue")
def daily_revenue():
    return (
        dlt.read("valid_events")               # batch read from Silver
        .groupBy("event_date")
        .agg({"amount": "sum", "id": "count"})
    )
```

### DLT SQL Syntax

```sql
-- Bronze: streaming table
CREATE OR REFRESH STREAMING TABLE raw_events
COMMENT 'Bronze: raw events'
AS SELECT * FROM cloud_files(
  '/Volumes/training/prep/landing/events/', 'json',
  map('cloudFiles.inferColumnTypes', 'true')
);

-- Silver: with expectations
CREATE OR REFRESH MATERIALIZED VIEW valid_events
  CONSTRAINT valid_id     EXPECT (id IS NOT NULL)  ON VIOLATION DROP ROW
  CONSTRAINT positive_amt EXPECT (amount > 0)      ON VIOLATION DROP ROW
  CONSTRAINT recent       EXPECT (event_date >= '2020-01-01')  -- WARN only
AS SELECT *, current_timestamp() AS processed_at
FROM LIVE.raw_events;

-- Gold: aggregation
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
AS SELECT event_date, SUM(amount) AS revenue
FROM LIVE.valid_events GROUP BY event_date;
```

### DLT Expectation Handling

| Decorator | On Violation | Row kept? |
|---|---|---|
| `@dlt.expect("name", condition)` | WARN (metrics logged) | ✅ Yes |
| `@dlt.expect_or_drop("name", condition)` | DROP ROW | ❌ No |
| `@dlt.expect_or_fail("name", condition)` | FAIL PIPELINE | ❌ Pipeline stops |

### Streaming Table vs Materialized View

| | Streaming Table | Materialized View |
|---|---|---|
| **Source** | Append-only stream | Any query |
| **Processing** | Incremental (new rows only) | Full refresh or incremental |
| **Use case** | Bronze ingestion | Silver/Gold transforms |
| **DLT syntax** | `dlt.read_stream()` | `dlt.read()` |

### Lakeflow Jobs — Multi-Task Workflow

```python
# Pass values between tasks using taskValues
# Task A — set a value
dbutils.jobs.taskValues.set(key="row_count", value=df.count())

# Task B — get value from Task A
count = dbutils.jobs.taskValues.get(
    taskKey="task_a",
    key="row_count",
    default=0,
    debugValue=0   # used when running outside of job context
)
```

### Databricks Asset Bundles (DABs) — Azure Databricks

```yaml
# databricks.yml
bundle:
  name: my_etl_project

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-XXXX.azuredatabricks.net

  prod:
    mode: production
    workspace:
      host: https://adb-YYYY.azuredatabricks.net
    run_as:
      service_principal_name: prod-sp@yourcompany.com

resources:
  jobs:
    my_job:
      name: etl_pipeline_${bundle.target}
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/ingest.py
        - task_key: transform
          depends_on: [{task_key: ingest}]
          notebook_task:
            notebook_path: ./src/transform.py

# Deploy: databricks bundle deploy --target dev
# Run:    databricks bundle run   --target dev my_job
```

---

## 🛡️ DOMAIN 5: Data Governance & Unity Catalog (~15%)

### Grant / Revoke Syntax

```sql
-- Grant on table
GRANT SELECT ON TABLE training.prep.sales TO `user@company.com`;
GRANT SELECT ON TABLE training.prep.sales TO `analysts` ;  -- group

-- Grant on schema (inherits to all tables)
GRANT USE SCHEMA, SELECT ON SCHEMA training.prep TO `data_consumers`;

-- Grant on catalog
GRANT USE CATALOG ON CATALOG training TO `data_consumers`;

-- Revoke
REVOKE SELECT ON TABLE training.prep.sales FROM `user@company.com`;

-- Show grants
SHOW GRANTS ON TABLE training.prep.sales;
SHOW GRANTS ON SCHEMA training.prep;
```

### Row-Level Security (RLS)

```sql
CREATE OR REPLACE FUNCTION training.prep.row_filter(region STRING)
RETURNS BOOLEAN
RETURN region = current_user() OR is_member('admin_group');

ALTER TABLE training.prep.sales
SET ROW FILTER training.prep.row_filter ON (region);
```

### Column Masking

```sql
CREATE OR REPLACE FUNCTION training.prep.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_readers') THEN email
  ELSE CONCAT(LEFT(email, 2), '***@***.com')
END;

ALTER TABLE training.prep.customers
SET MASK training.prep.mask_email ON COLUMN (email);
```

### Key Unity Catalog Commands

```sql
SHOW CATALOGS;
SHOW SCHEMAS IN training;
SHOW TABLES  IN training.prep;
DESCRIBE TABLE EXTENDED training.prep.sales;
DESCRIBE DETAIL training.prep.sales;     -- file stats, location, format
DESCRIBE HISTORY training.prep.sales;   -- version history

-- External location (Azure ADLS)
CREATE EXTERNAL LOCATION my_adls
URL 'abfss://container@storage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL my_adls_cred);

-- Volume (file storage inside UC)
CREATE VOLUME training.prep.landing
COMMENT 'Landing zone for raw files';
-- Access: /Volumes/training/prep/landing/<path>
```

---

## 🧠 High-Frequency Exam Topics (Memorize These)

1. **VACUUM** removes old Parquet files. Default retention = **7 days (168 hours)**. `RETAIN 0 HOURS` breaks time travel.
2. **OPTIMIZE** compacts small files. **ZORDER BY** co-locates data for data skipping. **Liquid Clustering** = modern ZORDER.
3. **Auto Loader** = `format("cloudFiles")`. `schemaLocation` stores the inferred schema.
4. **trigger(availableNow=True)** replaces deprecated `trigger(once=True)`.
5. **Watermark** = max late data tolerance. `outputMode("append")` is correct with watermark.
6. **DLT `@dlt.expect`** = WARN. `@dlt.expect_or_drop` = DROP ROW. `@dlt.expect_or_fail` = FAIL PIPELINE.
7. **Streaming Table** = append-only, `dlt.read_stream()`. **Materialized View** = any query, `dlt.read()`.
8. **Job cluster** = cheaper than all-purpose, auto-terminates after job.
9. **Unity Catalog Volumes** = `/Volumes/<catalog>/<schema>/<volume>/`. Use instead of `/tmp/` or raw DBFS.
10. **MERGE** = upsert. Requires `WHEN MATCHED` and/or `WHEN NOT MATCHED`.
11. **CDF** = `delta.enableChangeDataFeed = true`. Read via `table_changes('table', startVersion)`.
12. **`taskValues.set/get`** = pass data between Lakeflow Job tasks.
