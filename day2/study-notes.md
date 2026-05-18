# Day 2 — Development & Ingestion: PySpark + SQL + Auto Loader

## ⏰ Schedule
- **Morning (2h):** Read all sections + run notebook cells alongside reading
- **Mid-morning (2h):** Complete practice-tasks.md Task 1–3
- **Afternoon (2h):** Complete practice-tasks.md Task 4–5
- **Evening (1h):** CertSafari — ELT/Spark/SQL topic, 20 questions

---

## 2.1 Reading Data in Databricks

### Reading from files
```python
# CSV
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/path/to/file.csv")

# JSON
df = spark.read.format("json").load("/path/to/data.json")

# Parquet
df = spark.read.format("parquet").load("/path/to/data.parquet")

# Delta
df = spark.read.format("delta").load("/path/to/delta_table")
# Or using table name
df = spark.read.table("catalog.schema.tablename")
```

### Reading options to know
```python
# Common options for CSV
spark.read.format("csv") \
    .option("header", True)          # first row is column names
    .option("inferSchema", True)     # auto-detect types (slower)
    .option("sep", ";")              # delimiter
    .option("nullValue", "NA")       # treat 'NA' as null
    .option("escape", '"')           # escape character
    .load("path")

# Provide explicit schema (faster, safer in production)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
df = spark.read.schema(schema).csv("/path")
```

---

## 2.2 DataFrame Transformations

### Core operations
```python
# Select columns
df.select("col1", "col2")
df.select(df.col1, df.col2)
df.select(col("col1"), col("col2"))  # import col from pyspark.sql.functions

# Filter rows
df.filter(df.age > 18)
df.filter("age > 18")  # SQL string
df.where(df.status == "active")  # .where() = .filter()

# Add or rename columns
df.withColumn("full_name", concat(col("first"), lit(" "), col("last")))
df.withColumnRenamed("old_name", "new_name")

# Drop columns
df.drop("col_to_remove")

# Distinct
df.distinct()
df.dropDuplicates(["col1", "col2"])  # duplicates based on subset

# Sort
df.orderBy("age")            # ascending
df.orderBy(col("age").desc()) # descending

# Limit
df.limit(100)
```

### Aggregations
```python
from pyspark.sql.functions import count, sum, avg, max, min, countDistinct

# GroupBy + agg
df.groupBy("department") \
  .agg(
      count("*").alias("total_employees"),
      avg("salary").alias("avg_salary"),
      sum("salary").alias("total_salary")
  )

# Pivot
df.groupBy("year").pivot("quarter").sum("revenue")
```

### Joins
```python
# Inner join (default)
df1.join(df2, df1.id == df2.id)
df1.join(df2, "id")  # same column name — cleaner syntax

# Join types
df1.join(df2, "id", how="inner")   # default
df1.join(df2, "id", how="left")    # left outer
df1.join(df2, "id", how="right")   # right outer
df1.join(df2, "id", how="full")    # full outer
df1.join(df2, "id", how="semi")    # left semi (keep left rows that match)
df1.join(df2, "id", how="anti")    # left anti (keep left rows that DON'T match)

# Broadcast join (for small tables — forces broadcast)
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")
```

### Common Functions
```python
from pyspark.sql.functions import (
    col, lit, when, coalesce, isnan, isnull,
    upper, lower, trim, length, substring, concat, concat_ws, split,
    to_date, to_timestamp, date_format, current_date, current_timestamp, datediff,
    year, month, dayofmonth, hour, minute,
    round, floor, ceil, abs,
    array, explode, size, array_contains,
    struct, to_json, from_json,
    regexp_replace, regexp_extract
)

# Conditional expressions
df.withColumn("category",
    when(col("score") >= 90, "A")
    .when(col("score") >= 80, "B")
    .when(col("score") >= 70, "C")
    .otherwise("F")
)

# Handle nulls
df.na.fill(0)               # fill all null numerics with 0
df.na.fill({"age": 0, "name": "Unknown"})  # per column
df.na.drop()                # drop rows with any null
df.na.drop(subset=["age"])  # drop rows with null in 'age'
```

---

## 2.3 Writing Data

```python
# Write modes
df.write.mode("overwrite").format("delta").save("/path/to/table")
df.write.mode("append").format("delta").save("/path/to/table")
df.write.mode("ignore")   # do nothing if path exists
df.write.mode("error")    # default; fail if path exists

# Save as managed table
df.write.saveAsTable("catalog.schema.tablename")

# With options
df.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true")  # for Delta schema changes
  .saveAsTable("my_table")

# Partition by column
df.write.partitionBy("year", "month").format("delta").save("/path")
```

---

## 2.4 SQL in Databricks

```sql
-- Create database/schema
CREATE DATABASE IF NOT EXISTS my_catalog.my_schema;
USE CATALOG my_catalog;
USE SCHEMA my_schema;

-- Create table from query
CREATE OR REPLACE TABLE orders AS
SELECT * FROM raw_orders WHERE status = 'complete';

-- CTAS (Create Table As Select)
CREATE TABLE silver_customers
USING DELTA
AS SELECT id, name, email FROM bronze_customers;

-- CREATE OR REPLACE VIEW
CREATE OR REPLACE VIEW vw_active_users AS
SELECT * FROM users WHERE active = true;

-- INSERT INTO
INSERT INTO my_table VALUES (1, 'Alice');
INSERT INTO my_table SELECT * FROM staging_table;

-- MERGE (upsert)
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.name = s.name
WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)
WHEN NOT MATCHED BY SOURCE THEN DELETE;  -- deletes rows in target not in source

-- Common Table Expressions (CTEs)
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
  FROM employees
)
SELECT * FROM ranked WHERE rn = 1;

-- Window functions
SELECT
  id,
  salary,
  AVG(salary) OVER (PARTITION BY dept) AS dept_avg,
  RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS salary_rank,
  LAG(salary, 1) OVER (ORDER BY hire_date) AS prev_salary,
  SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM employees;
```

---

## 2.5 Auto Loader (cloudFiles)

Auto Loader is Databricks' incremental file ingestion tool. It watches a cloud storage directory and processes **only new files** as they arrive.

### Key features:
- **Incremental processing**: tracks which files have been ingested (checkpoint)
- **Scalable file discovery**: can handle billions of files via `queueing` mode
- **Schema inference and evolution**: can detect schema changes
- **Two discovery modes:**
  - `directory listing` (default): lists files each run — simpler but less scalable
  - `file notification` (queue): uses cloud event notifications — scalable for large file volumes

### Auto Loader Syntax
```python
# Basic Auto Loader (streaming)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")       # source format: csv, json, parquet, delta, etc.
    .option("cloudFiles.schemaLocation", "/checkpoint/schema")  # schema inference storage
    .option("header", "true")
    .load("/source/path/")  # directory to monitor
)

# Write to Delta with checkpoint
(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoint/data")
    .trigger(availableNow=True)  # process all available, then stop
    .table("catalog.schema.bronze_table")
)
```

### Auto Loader options
```python
# Schema hints (help inference)
.option("cloudFiles.schemaHints", "id INT, timestamp TIMESTAMP")

# Include file metadata columns
.option("cloudFiles.includeExistingFiles", "false")  # skip files already there at startup

# Use SQS/Event Grid for file notification mode
.option("cloudFiles.useNotifications", "true")
.option("cloudFiles.region", "us-east-1")
```

---

## 2.6 Key Exam-Focus Points for This Domain

1. ✅ Know the **4 write modes**: overwrite, append, ignore, error
2. ✅ Know **MERGE syntax** — especially `WHEN NOT MATCHED BY SOURCE THEN DELETE`
3. ✅ Know **broadcast joins** — when and why
4. ✅ Know **Auto Loader** uses `cloudFiles` format; requires checkpoint + schema location
5. ✅ Know difference between `inferSchema` (runtime) vs explicit schema definition
6. ✅ Know common **window functions**: RANK, ROW_NUMBER, LAG, LEAD, SUM OVER
7. ✅ Know `.na.fill()`, `.na.drop()`, `coalesce()`, `when().otherwise()` for null handling
8. ✅ `select()` vs `withColumn()` vs `drop()` differences

---

## 📺 Videos to Watch Today

1. [PySpark DataFrame API](https://www.youtube.com/watch?v=XrpSRCwISdk) — Basic DataFrame operations (~30 min)
2. [Auto Loader Deep Dive](https://www.youtube.com/watch?v=7fgQpxe0amc) — (~20 min)

---

## 🔗 Reading Links
- [PySpark SQL Functions Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions/index.html)
- [Auto Loader Docs](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [DataFrame Reader/Writer](https://docs.databricks.com/en/ingestion/index.html)


---

## 2.7 Spark Execution Model — DAG, Stages, Tasks

Understanding how Spark executes work is critical for performance optimization and interview questions.

### Key Concepts

| Term | Definition |
|---|---|
| **Job** | Triggered by an action (e.g., `.count()`, `.show()`, `.write()`). One job per action. |
| **Stage** | A set of tasks that can run in parallel without a shuffle. Stage boundaries = wide transformations. |
| **Task** | Smallest unit of work. One task per partition. Runs on one executor core. |
| **DAG** | Directed Acyclic Graph — Spark's execution plan. DAG Scheduler splits it into stages. |
| **Executor** | JVM process on a worker node. Runs tasks and caches data. |
| **Driver** | Coordinates the job. Runs the main program, creates SparkContext. |

### Narrow vs. Wide Transformations

| Type | Definition | Examples | Shuffle? |
|---|---|---|---|
| **Narrow** | Each partition produces exactly one output partition. No data movement across executors. | `map()`, `filter()`, `select()`, `withColumn()`, `union()` | ❌ No |
| **Wide** | Multiple input partitions contribute to multiple output partitions. Requires **shuffle**. | `groupBy()`, `join()`, `distinct()`, `orderBy()`, `repartition()` | ✅ Yes |

> 🔑 **Interview key point:** Wide transformations trigger a shuffle, which is the most expensive operation in Spark. Optimizing shuffle = optimizing Spark.

---

## 2.8 Shuffle — The Most Important Spark Performance Concept

A **shuffle** is the redistribution of data across the network between executors. It happens when Spark needs to reorganize data across partitions — for example, to group all records with the same key together.

### What Triggers a Shuffle?
- `groupBy()` + aggregation
- `join()` (unless broadcast join)
- `distinct()`
- `orderBy()` / `sort()`
- `repartition()`
- Window functions with `PARTITION BY`

### Why Is Shuffle Expensive?
1. **Disk I/O** — shuffle data is written to disk (spill)
2. **Network I/O** — data travels across the network between executors
3. **Serialization/deserialization** — data must be serialized for transfer

### Key Shuffle Setting
```python
# Number of shuffle partitions (default = 200)
# Too many = overhead; Too few = large partitions, OOM risk
spark.conf.set("spark.sql.shuffle.partitions", "200")  # default
spark.conf.set("spark.sql.shuffle.partitions", "400")  # scale up for large data
spark.conf.set("spark.sql.shuffle.partitions", "50")   # scale down for small data

# Check current setting
spark.conf.get("spark.sql.shuffle.partitions")
```

> ⚠️ **Exam & Interview trap:** The default `spark.sql.shuffle.partitions=200` is often wrong for your data size. Too many partitions → many small tasks, overhead. Too few → OOM errors. Tune based on data volume.

### Adaptive Query Execution (AQE) — Automatic Shuffle Optimization
Databricks enables **AQE by default** (Spark 3.0+). AQE automatically:
- **Coalesces shuffle partitions** — merges small partitions after shuffle
- **Switches join strategies** — converts sort-merge join to broadcast join if one side is small
- **Optimizes skew joins** — splits skewed partitions

```python
# AQE is ON by default in Databricks
spark.conf.get("spark.sql.adaptive.enabled")  # True

# AQE coalesce: merges shuffle partitions that are small
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Shuffle Optimization Strategies

| Strategy | When to Use | Code |
|---|---|---|
| **Broadcast join** | One table < 10 MB (auto) or < broadcast threshold | `broadcast(small_df)` or auto |
| **Reduce shuffle partitions** | Small/medium datasets | `spark.sql.shuffle.partitions = 50` |
| **Increase shuffle partitions** | Large datasets (>100GB) | `spark.sql.shuffle.partitions = 800` |
| **Pre-sort/pre-partition** | Repeated joins on same key | Partition by join key beforehand |
| **AQE** | Always — enabled by default | No action needed |

---

## 2.9 Partitioning — Controlling Data Layout

### RDD/DataFrame Partitions
Every DataFrame is split into **partitions** — chunks of data processed independently by executors.

```python
# Check current number of partitions
df.rdd.getNumPartitions()  # e.g., 8

# Increase partitions (causes a shuffle)
df_repartitioned = df.repartition(100)
df_repartitioned = df.repartition(100, col("customer_id"))  # hash-partition by column

# Decrease partitions (NO shuffle — just merges)
df_coalesced = df.coalesce(10)  # cannot increase with coalesce
```

### `repartition()` vs `coalesce()`
| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| **Shuffle** | ✅ Always shuffles | ❌ No shuffle (avoids it) |
| **Can increase partitions** | ✅ Yes | ❌ No |
| **Output partition size** | Even distribution | May be uneven |
| **Use case** | Need exact N partitions, or even distribution | Reduce partitions efficiently after filter |

> 🔑 **Pattern:** After a heavy filter that reduces data significantly, use `coalesce()` to reduce partitions without a shuffle before writing.

### Writing with Partitions
```python
# Partition data on disk by column (creates folder hierarchy)
df.write \
    .partitionBy("year", "month") \
    .format("delta") \
    .save("/mnt/data/sales")

# Results in:
# /mnt/data/sales/year=2024/month=01/part-00001.parquet
# /mnt/data/sales/year=2024/month=02/part-00001.parquet
```

### Partition Pruning (Data Skipping)
When you read a partitioned table with a filter on the partition column, Spark **only reads the relevant folders** — skipping the rest. This is called **partition pruning**.

```sql
-- Spark reads only year=2024/month=01/ folder — skips all others
SELECT * FROM sales WHERE year = 2024 AND month = 1;
```

> ⚠️ **Exam trap:** Partition pruning only works when you filter on the **partition column** (the column used in `partitionBy`). Filtering on non-partition columns doesn't prune.

### When to Partition on Disk
- **High-cardinality columns**: bad choice (too many folders, too many small files)
- **Low-to-medium cardinality + frequently filtered**: ideal (e.g., `year`, `month`, `country`, `status`)
- Rule of thumb: each partition should be **>= 128 MB** of data

---

## 2.10 UDFs — User Defined Functions

### Python UDFs
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a Python function
def categorize_age(age):
    if age < 18:
        return "minor"
    elif age < 65:
        return "adult"
    else:
        return "senior"

# Register as UDF
categorize_udf = udf(categorize_age, StringType())

# Use in DataFrame
df.withColumn("age_group", categorize_udf(col("age")))
```

### Pandas UDFs (Vectorized UDFs) — Preferred
Pandas UDFs are **much faster** than regular Python UDFs because they use Apache Arrow for serialization and process data in batches.

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@pandas_udf(StringType())
def categorize_age_pd(ages: pd.Series) -> pd.Series:
    return ages.apply(lambda a: "minor" if a < 18 else "adult" if a < 65 else "senior")

df.withColumn("age_group", categorize_age_pd(col("age")))
```

### SQL UDFs (Unity Catalog)
```sql
-- Create a persistent SQL UDF in Unity Catalog
CREATE OR REPLACE FUNCTION main.utils.age_group(age INT)
RETURNS STRING
RETURN CASE
    WHEN age < 18 THEN 'minor'
    WHEN age < 65 THEN 'adult'
    ELSE 'senior'
END;

-- Use it
SELECT name, main.utils.age_group(age) AS group FROM users;
```

### UDF Performance Comparison
| Type | Speed | Serialization | When to Use |
|---|---|---|---|
| **Built-in functions** | ⚡ Fastest | None (native JVM) | Always prefer these |
| **SQL UDF** | Fast | SQL execution | Reusable logic, stored in catalog |
| **Pandas UDF** | Moderate | Arrow (columnar) | Complex Python logic on large data |
| **Python UDF** | 🐢 Slowest | Row-by-row pickle | Avoid if possible |

> 🔑 **Best practice:** Always prefer built-in Spark functions. Use UDFs only when built-ins can't do what you need.

---

## 2.11 `dbutils` — Databricks Utilities

`dbutils` is Databricks-specific. Not available in standard Spark.

### File System Utilities (`dbutils.fs`)
```python
# List files
dbutils.fs.ls("/mnt/data/")
dbutils.fs.ls("/Volumes/main/schema/volume/")

# Copy a file
dbutils.fs.cp("/mnt/src/file.csv", "/mnt/dst/file.csv")

# Move a file
dbutils.fs.mv("/mnt/src/file.csv", "/mnt/dst/file.csv")

# Delete a file or directory
dbutils.fs.rm("/mnt/old/", recurse=True)

# Create directory
dbutils.fs.mkdirs("/mnt/new_dir/")

# Read a text file
dbutils.fs.head("/mnt/data/file.txt", 1000)  # first 1000 bytes
```

### Secrets (`dbutils.secrets`)
```python
# Access secrets stored in Databricks Secret Scope
password = dbutils.secrets.get(scope="my-scope", key="db-password")
token = dbutils.secrets.get(scope="azure-kv", key="storage-account-key")

# List scopes and secrets (won't reveal values)
dbutils.secrets.listScopes()
dbutils.secrets.list("my-scope")
```

### Notebook Utilities (`dbutils.notebook`)
```python
# Run another notebook and pass parameters
result = dbutils.notebook.run("/path/to/notebook", timeout_seconds=300,
                               arguments={"env": "prod", "date": "2024-01-15"})
# Return a value from a notebook
dbutils.notebook.exit("SUCCESS")
```

### Widgets (`dbutils.widgets`)
```python
# Create a text widget (parameter input)
dbutils.widgets.text("date", "2024-01-01", "Processing Date")

# Create a dropdown widget
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")

# Get widget value
date_val = dbutils.widgets.get("date")
env_val = dbutils.widgets.get("env")

# Remove widgets
dbutils.widgets.remove("date")
dbutils.widgets.removeAll()
```

---

## 2.12 Updated Key Exam-Focus Points

9. ✅ Know **narrow vs wide transformations** — wide = shuffle (groupBy, join, distinct, orderBy)
10. ✅ Know **`spark.sql.shuffle.partitions`** — default 200; tune based on data size
11. ✅ Know **AQE (Adaptive Query Execution)** — enabled by default; auto-coalesces shuffle partitions
12. ✅ Know **`repartition()` vs `coalesce()`** — repartition shuffles, coalesce doesn't
13. ✅ Know **partition pruning** — only works when filtering on the partitioned column
14. ✅ Know **broadcast join** — use for small tables; avoids shuffle
15. ✅ Know **UDF performance order**: built-in > SQL UDF > Pandas UDF > Python UDF
16. ✅ Know **`dbutils.secrets`** for secure credential access, **`dbutils.widgets`** for notebook parameters
