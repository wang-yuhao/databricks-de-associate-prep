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
