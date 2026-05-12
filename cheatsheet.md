# 📋 Databricks DE Associate — Complete Cheat Sheet

> Quick-reference for all exam-critical concepts. Print or keep open during study sessions.

---

## 🏛️ DOMAIN 1: Databricks Intelligence Platform (10%)

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
Metastore (one per region)
  └── Catalog (e.g., dev, prod)
        └── Schema / Database
              └── Table / View / Function

SQL: catalog_name.schema_name.table_name
```

### Databricks Workspace Components

- **Notebooks** — interactive: SQL, Python, Scala, R
- **Repos** — Git integration for version control (CI/CD)
- **DBFS** — Databricks File System (distributed storage)
- **Workflows / Lakeflow Jobs** — job orchestration
- **Catalog Explorer** — browse Unity Catalog objects

---

## 📥 DOMAIN 2: Development & Ingestion (30%)

### Delta Lake — Core Concepts

**ACID Properties:**
- **A**tomicity — all or nothing (no partial writes)
- **C**onsistency — schema enforcement
- **I**solation — concurrent ops don't conflict (optimistic concurrency)
- **D**urability — committed = permanent

**Delta Table Storage:**
```
table_directory/
  ├── _delta_log/          ← transaction log (JSON + Parquet)
  │   ├── 00000.json
  │   ├── 00001.json
  │   └── ...
  ├── part-00000.parquet
  └── part-00001.parquet
```

### Creating Delta Tables

```sql
-- SQL: Managed table (Databricks manages location)
CREATE TABLE catalog.schema.employees (
  id INT,
  name STRING,
  salary DOUBLE,
  dept STRING
) USING DELTA;

-- SQL: External table (you manage location)
CREATE TABLE catalog.schema.ext_table
USING DELTA
LOCATION 'abfss://container@storage.dfs.core.windows.net/path';
```

```python
# Python: Write as Delta
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.table")

# Python: Read Delta
df = spark.read.format("delta").load("/path/to/delta/table")
# OR
df = spark.table("catalog.schema.table")
```

### Auto Loader — Key Syntax

```python
# Auto Loader = incremental file ingestion from cloud storage
# Automatically detects NEW files only (no duplicates)

df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json")  # csv, parquet, avro, etc.
    .option("cloudFiles.schemaLocation", "/checkpoint/schema") \
    .load("/path/to/source/")

df.writeStream \
    .option("checkpointLocation", "/checkpoint/") \
    .trigger(availableNow=True) \
    .table("catalog.schema.target_table")
```

> 📌 **Auto Loader vs COPY INTO:**
> - **Auto Loader**: streaming, scalable to billions of files, schema inference, preferred for large scale
> - **COPY INTO**: SQL-based, idempotent batch load, simpler, good for thousands of files

### COPY INTO

```sql
COPY INTO catalog.schema.target_table
FROM '/path/to/source/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
-- COPY INTO is IDEMPOTENT: re-running won't load duplicate files
```

### Structured Streaming Basics

```python
# Read stream
df_stream = spark.readStream.format("delta").table("source_table")

# Write stream — append mode
df_stream.writeStream \
    .outputMode("append")   # also: complete, update
    .option("checkpointLocation", "/chk/") \
    .trigger(processingTime="10 seconds")  # or availableNow=True
    .table("target_table")
```

**Trigger types:**
- `processingTime="10 seconds"` — micro-batch every N seconds
- `availableNow=True` — process all available data, then stop (like batch)
- `once=True` — deprecated, use `availableNow`
- `continuous="1 second"` — continuous processing (millisecond latency)

---

## 🔄 DOMAIN 3: Data Processing & Transformations (31%)

### PySpark Fundamentals

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Read data
df = spark.read.format("delta").table("catalog.schema.table")

# Basic transformations
df_filtered = df.filter(F.col("salary") > 50000)
df_selected = df.select("name", "dept", F.col("salary") * 1.1)
df_renamed = df.withColumnRenamed("old_name", "new_name")
df_added = df.withColumn("bonus", F.col("salary") * 0.1)
df_dropped = df.drop("unnecessary_col")

# Aggregations
df_agg = df.groupBy("dept").agg(
    F.count("id").alias("employee_count"),
    F.avg("salary").alias("avg_salary"),
    F.sum("salary").alias("total_salary")
)

# Joins
df_joined = df.join(dept_df, on="dept_id", how="left")  # inner, left, right, full
```

### SQL in Databricks

```sql
-- CTEs
WITH dept_stats AS (
  SELECT dept, AVG(salary) AS avg_sal
  FROM employees
  GROUP BY dept
)
SELECT e.name, e.salary, d.avg_sal
FROM employees e
JOIN dept_stats d ON e.dept = d.dept;

-- Window Functions
SELECT name, dept, salary,
  ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank,
  AVG(salary) OVER (PARTITION BY dept) AS dept_avg,
  SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM employees;
```

### Higher-Order Functions (Array/Map)

```sql
-- TRANSFORM: apply function to each array element
SELECT TRANSFORM(scores, x -> x * 1.1) AS boosted_scores FROM students;

-- FILTER: keep elements matching condition
SELECT FILTER(tags, t -> t LIKE 'data%') AS data_tags FROM articles;

-- EXPLODE: one row per array element
SELECT id, EXPLODE(skills) AS skill FROM employees;

-- EXPLODE_OUTER: keeps rows with empty/null arrays
SELECT id, EXPLODE_OUTER(skills) AS skill FROM employees;
```

### User Defined Functions (UDFs)

```python
# Python UDF (slower — use sparingly)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def categorize_salary(salary):
    if salary > 100000: return "High"
    elif salary > 60000: return "Medium"
    return "Low"

df = df.withColumn("salary_cat", categorize_salary(F.col("salary")))

# SQL UDF (faster than Python UDF, optimized by Catalyst)
CREATE FUNCTION catalog.schema.categorize_salary(salary DOUBLE)
RETURNS STRING
RETURN CASE
  WHEN salary > 100000 THEN 'High'
  WHEN salary > 60000 THEN 'Medium'
  ELSE 'Low'
END;
```

### Performance & Optimization

```sql
-- OPTIMIZE: compact small files
OPTIMIZE catalog.schema.employees;

-- ZORDER: co-locate related data for faster queries on those columns
OPTIMIZE catalog.schema.events ZORDER BY (event_date, user_id);

-- Liquid Clustering (newer, replaces ZORDER for new tables)
CREATE TABLE events CLUSTER BY (event_date, user_id);
-- Then: ALTER TABLE events CLUSTER BY (event_date, region);

-- VACUUM: remove old files (default 7-day retention)
VACUUM catalog.schema.employees;          -- uses default 7 days
VACUUM catalog.schema.employees RETAIN 168 HOURS;  -- explicit 7 days
-- ⚠️ NEVER use RETAIN 0 HOURS in production — breaks time travel!
```

**Caching:**
```python
df.cache()          # persist in memory
df.persist()        # memory + disk
df.unpersist()      # release cache

# SQL:
CACHE TABLE my_table;
UNCACHE TABLE my_table;
```

---

## 🚀 DOMAIN 4: Productionizing Data Pipelines (18%)

### Delta Live Tables (DLT)

```python
import dlt
from pyspark.sql.functions import *

# Bronze layer — raw ingestion
@dlt.table(
    comment="Raw events from Auto Loader",
    table_properties={"quality": "bronze"}
)
def raw_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/raw/events/")
    )

# Silver layer — cleaned + validated
@dlt.table(comment="Validated events")
@dlt.expect("valid_user", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_event", "event_type IN ('click','view','purchase')")
@dlt.expect_or_fail("positive_amount", "amount >= 0")
def silver_events():
    return dlt.read_stream("raw_events").where("event_type IS NOT NULL")

# Gold layer — aggregated
@dlt.table(comment="Daily revenue aggregation")
def gold_daily_revenue():
    return (
        dlt.read("silver_events")
            .groupBy("date", "region")
            .agg(sum("amount").alias("total_revenue"))
    )
```

**DLT Expectation Behaviors:**

| Expectation | On Violation | Use Case |
|---|---|---|
| `@dlt.expect` | Keep row, record violation in metrics | Monitoring only |
| `@dlt.expect_or_drop` | Drop violating rows | Filter bad data |
| `@dlt.expect_or_fail` | Fail entire pipeline | Critical data integrity |

### Lakeflow Jobs (Workflows)

- **Task types:** Notebook, DLT Pipeline, SQL, Python script, dbt
- **Dependencies:** Sequential, parallel, conditional (if/else branches)
- **Job Clusters vs All-Purpose:** Always use job clusters for production
- **Retry logic:** Configure retries per task
- **Alerts:** Email/webhook on success/failure/timeout

```python
# Task dependency example (JSON structure)
{
  "tasks": [
    {"task_key": "ingest", "notebook_task": {...}},
    {"task_key": "transform", "depends_on": [{"task_key": "ingest"}]},
    {"task_key": "report", "depends_on": [{"task_key": "transform"}]}
  ]
}
```

### CI/CD with Databricks

- **Repos:** Connect to GitHub/GitLab/Bitbucket
- **Databricks CLI:** `databricks bundle deploy`
- **Databricks Asset Bundles (DAB):** Infrastructure-as-code for jobs, DLT, permissions
- **Branch strategy:** dev → staging → prod environments
- **Secrets:** Store in Databricks Secret Scopes, never hardcode

```bash
# Databricks CLI commands
databricks bundle validate
databricks bundle deploy --target prod
databricks runs submit --json @job_config.json
```

---

## 🔐 DOMAIN 5: Data Governance & Quality (11%)

### Unity Catalog Permissions

```sql
-- Grant access
GRANT SELECT ON TABLE catalog.schema.employees TO `user@company.com`;
GRANT USAGE ON SCHEMA catalog.schema TO `data-eng-group`;
GRANT CREATE TABLE ON SCHEMA catalog.schema TO `role_name`;

-- Revoke access
REVOKE SELECT ON TABLE catalog.schema.employees FROM `user@company.com`;

-- Show grants
SHOW GRANTS ON TABLE catalog.schema.employees;
```

**Permission hierarchy:** Must grant USAGE at each level
```
GRANT USAGE ON CATALOG → GRANT USAGE ON SCHEMA → GRANT SELECT ON TABLE
```

### Row-Level & Column-Level Security

```sql
-- Dynamic View for row-level security
CREATE VIEW secure_employees AS
SELECT * FROM employees
WHERE dept = current_user_dept()  -- filter rows based on user
   OR IS_ACCOUNT_GROUP_MEMBER('hr-team');

-- Column masking (dynamic view)
CREATE VIEW masked_employees AS
SELECT
  id,
  name,
  CASE WHEN IS_ACCOUNT_GROUP_MEMBER('payroll')
       THEN salary
       ELSE '***REDACTED***'
  END AS salary
FROM employees;
```

### Delta Lake Constraints (Data Quality)

```sql
-- Add NOT NULL constraint
ALTER TABLE employees ADD CONSTRAINT not_null_id CHECK (id IS NOT NULL);

-- Add check constraint
ALTER TABLE employees ADD CONSTRAINT positive_salary CHECK (salary > 0);

-- View constraints
DESCRIBE DETAIL employees;
-- Violations raise an error on INSERT/UPDATE
```

### Delta Sharing

```sql
-- Create a share (provider side)
CREATE SHARE company_data_share;
ALTER SHARE company_data_share ADD TABLE catalog.schema.public_data;

-- Create recipient
CREATE RECIPIENT partner_company;

-- Grant share to recipient
GRANT SELECT ON SHARE company_data_share TO RECIPIENT partner_company;
```

### Lakehouse Federation

- Connect to external databases (PostgreSQL, MySQL, Snowflake, etc.) without copying data
- Creates a **foreign catalog** in Unity Catalog
- Data stays in source system — Unity Catalog provides governance overlay

```sql
CREATE CONNECTION postgres_prod
TYPE POSTGRESQL
OPTIONS (
  host 'my-postgres.example.com',
  port '5432',
  database 'prod_db'
);

CREATE FOREIGN CATALOG postgres_catalog
USING CONNECTION postgres_prod
OPTIONS (database 'prod_db');
```

---

## 🔑 Key Commands Quick Reference

```sql
-- Time Travel
SELECT * FROM table VERSION AS OF 5;
SELECT * FROM table TIMESTAMP AS OF '2024-01-15';
RESTORE TABLE table TO VERSION AS OF 10;

-- Table Management
DESCRIBE HISTORY table_name;      -- see all versions
DESCRIBE DETAIL table_name;       -- file stats, partitioning
DESCRIBE EXTENDED table_name;     -- schema + metadata

-- File Management
OPTIMIZE table_name;              -- compact small files
OPTIMIZE table_name ZORDER BY (col1, col2);  -- with clustering
VACUUM table_name [RETAIN N HOURS]; -- remove old files

-- Schema Changes
ALTER TABLE t ADD COLUMNS (new_col STRING);
ALTER TABLE t ADD CONSTRAINT c CHECK (col > 0);

-- Managed vs External tables
-- Managed: DROP TABLE deletes data
-- External: DROP TABLE does NOT delete underlying files
```

---

## ⚠️ Common Exam Traps

1. **VACUUM default** = 7 days (168 hours). Never less in production.
2. **Job cluster** = cheaper, terminates after job. All-purpose = persistent, shared.
3. **Auto Loader** uses `cloudFiles` format, requires `schemaLocation` checkpoint.
4. **COPY INTO** is idempotent — safe to re-run.
5. **DLT `expect`** keeps bad rows; `expect_or_drop` removes them; `expect_or_fail` stops pipeline.
6. **Managed table** = Databricks owns data location; external = you own it.
7. **Unity Catalog** requires USAGE grant at CATALOG → SCHEMA → TABLE hierarchy.
8. **Time travel** works up to `VACUUM` retention period.
9. **Liquid Clustering** replaces ZORDER for new tables — no upfront partition decision needed.
10. **`availableNow=True`** is the replacement for deprecated `once=True` trigger.
