# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 — PySpark & SQL Fundamentals on Azure Databricks
# MAGIC
# MAGIC **Environment:** Azure Databricks (Unity Catalog enabled)  
# MAGIC **Cluster:** Single Node, DBR 15.x LTS, `Standard_DS3_v2`  
# MAGIC **Estimated time:** 2–3 hours
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC 1. Attach this notebook to your `learning-cluster` (created in Day 1)
# MAGIC 2. All data is stored as Unity Catalog managed tables — no external storage config needed
# MAGIC 3. Run cells top-to-bottom

# COMMAND ----------
# MAGIC %md ## Cell 0 — Create Practice Catalog & Schema

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create a dedicated catalog and schema for Day 2 practice
# MAGIC -- Note: requires CREATE CATALOG privilege (workspace admin has this by default)
# MAGIC CREATE CATALOG IF NOT EXISTS training
# MAGIC   COMMENT 'Practice catalog for DE Associate exam prep';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS training.day2
# MAGIC   COMMENT 'Day 2: PySpark and SQL exercises';
# MAGIC
# MAGIC USE CATALOG training;
# MAGIC USE SCHEMA day2;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------
# MAGIC %md ## Cell 1 — Create Sample DataFrames

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, avg, count, sum as spark_sum, broadcast

# Employee data
emp_data = [
    (1, "Alice",   "Engineering", 95000, "2020-01-15"),
    (2, "Bob",     "Marketing",   72000, "2019-03-01"),
    (3, "Charlie", "Engineering", 88000, "2021-06-01"),
    (4, "Diana",   "HR",          65000, "2018-07-20"),
    (5, "Eve",     "Marketing",   78000, "2022-01-10"),
    (6, "Frank",   None,          92000, "2020-04-15"),  # null department
]
emp_schema = StructType([
    StructField("id",         IntegerType(), True),
    StructField("name",       StringType(),  True),
    StructField("department", StringType(),  True),
    StructField("salary",     IntegerType(), True),
    StructField("hire_date",  StringType(),  True),
])
employees = spark.createDataFrame(emp_data, emp_schema)

# Department data
dept_data = [
    ("Engineering", "Munich"),
    ("Marketing",   "Berlin"),
    ("HR",          "Hamburg"),
]
departments = spark.createDataFrame(dept_data, ["dept_name", "city"])

print("DataFrames created successfully")
employees.show()
departments.show()

# COMMAND ----------
# MAGIC %md ## Cell 2 — Save as Unity Catalog Managed Tables

# COMMAND ----------
# Save as managed Delta tables in Unity Catalog (no LOCATION needed — UC manages storage)
employees.write.mode("overwrite").saveAsTable("training.day2.employees")
departments.write.mode("overwrite").saveAsTable("training.day2.departments")

print("Tables saved to Unity Catalog:")
print("  - training.day2.employees")
print("  - training.day2.departments")

# COMMAND ----------
# MAGIC %md ## Cell 3 — Read from Unity Catalog Tables

# COMMAND ----------
# Always read from Unity Catalog tables in Azure Databricks — not from /tmp/ paths
df_emp  = spark.table("training.day2.employees")
df_dept = spark.table("training.day2.departments")

print("Schema:")
df_emp.printSchema()
df_emp.show()

# COMMAND ----------
# MAGIC %md ## Cell 4 — DataFrame Transformations

# COMMAND ----------
# 4a: Select + computed column
result_4a = (
    df_emp
    .select("id", "name", "salary")
    .withColumn("senior", when(col("salary") > 85000, True).otherwise(False))
)
result_4a.show()

# 4b: Filter
result_4b = df_emp.filter(
    (col("department") == "Engineering") & (col("salary") > 90000)
)
result_4b.show()

# 4c: Handle nulls
result_4c = df_emp.na.fill({"department": "Unknown"})
result_4c.show()

# 4d: Group + aggregate
result_4d = (
    df_emp
    .na.fill({"department": "Unknown"})
    .groupBy("department")
    .agg(
        count("*").alias("headcount"),
        avg("salary").alias("avg_salary"),
    )
    .orderBy(col("avg_salary").desc())
)
result_4d.show()

# COMMAND ----------
# MAGIC %md ## Cell 5 — Joins

# COMMAND ----------
# Inner join (default)
df_inner = df_emp.join(df_dept, df_emp.department == df_dept.dept_name, "inner")
df_inner.select("name", "department", "city", "salary").show()

# Left join — keep all employees, even those with no matching dept
df_left = df_emp.join(df_dept, df_emp.department == df_dept.dept_name, "left")
df_left.select("name", "department", "city").show()

# Broadcast join — force small table to be broadcast (avoids shuffle)
df_broadcast = df_emp.join(broadcast(df_dept), df_emp.department == df_dept.dept_name)
df_broadcast.select("name", "department", "city").show()
print("Broadcast join complete — note the broadcast hint in query plan:")
df_broadcast.explain()

# COMMAND ----------
# MAGIC %md ## Cell 6 — Window Functions

# COMMAND ----------
from pyspark.sql.functions import rank, row_number, lag, dense_rank
from pyspark.sql import Window

window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
window_global = Window.orderBy("hire_date")

df_window = (
    df_emp
    .na.fill({"department": "Unknown"})
    .withColumn("rank_in_dept",    rank().over(window_dept))
    .withColumn("row_num_in_dept", row_number().over(window_dept))
    .withColumn("dense_rank",      dense_rank().over(window_dept))
    .withColumn("prev_hire_salary", lag("salary", 1).over(window_global))
)
df_window.show()

# COMMAND ----------
# MAGIC %md ## Cell 7 — Reading Different File Formats (from Unity Catalog Volumes)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create a volume to use as a file landing area (replaces /tmp/ in Community Edition)
# MAGIC CREATE VOLUME IF NOT EXISTS training.day2.files
# MAGIC   COMMENT 'Landing area for sample files in Day 2 practice';

# COMMAND ----------
# Write a sample CSV to the volume
sample_df = spark.createDataFrame(
    [(1, "Alice", 95000.0), (2, "Bob", 72000.0), (3, "Charlie", 88000.0)],
    ["id", "name", "salary"]
)

# Volume path: /Volumes/{catalog}/{schema}/{volume}/
volume_path = "/Volumes/training/day2/files"
sample_df.write.mode("overwrite").option("header", True).csv(f"{volume_path}/employees_sample")
print(f"Written CSV to volume: {volume_path}/employees_sample/")

# COMMAND ----------
# Read CSV back from the volume
df_csv = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .load(f"{volume_path}/employees_sample")
)
print("Read from volume CSV:")
df_csv.show()

# Write Parquet to volume
sample_df.write.mode("overwrite").parquet(f"{volume_path}/employees_parquet")

# Read Parquet back
df_parquet = spark.read.parquet(f"{volume_path}/employees_parquet")
print("Read Parquet from volume:")
df_parquet.show()

# COMMAND ----------
# MAGIC %md ## Cell 8 — Auto Loader (cloudFiles) on Unity Catalog Volume

# COMMAND ----------
# Auto Loader reads NEW files incrementally from a directory
# On Azure Databricks: use /Volumes/ path (UC volume) or abfss:// (ADLS Gen2)

# Step 1: Write some JSON files to volume to simulate landing data
import json

orders = [
    {"order_id": "O001", "customer": "Alice", "amount": 150.0, "status": "completed"},
    {"order_id": "O002", "customer": "Bob",   "amount":  75.5, "status": "pending"},
]

orders_df = spark.createDataFrame(orders)
orders_df.write.mode("overwrite").json(f"{volume_path}/landing/orders_batch1")
print("Simulated landing data written to volume")

# COMMAND ----------
# Step 2: Auto Loader reads from the volume directory
auto_loader_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    # schemaLocation stores the inferred schema checkpoint
    .option("cloudFiles.schemaLocation", f"{volume_path}/autoloader_schema")
    .load(f"{volume_path}/landing/")
)

# Write stream to Unity Catalog table
auto_loader_query = (
    auto_loader_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{volume_path}/autoloader_checkpoint")
    .trigger(availableNow=True)  # process all available files then stop
    .toTable("training.day2.orders_bronze")  # Unity Catalog managed table
)

auto_loader_query.awaitTermination()
print("Auto Loader finished. Checking results...")

# COMMAND ----------
# Step 3: Verify
result = spark.table("training.day2.orders_bronze")
print(f"Rows ingested: {result.count()}")
result.show()

# COMMAND ----------
# MAGIC %md ## Cell 9 — Write Modes Demo

# COMMAND ----------
base_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
new_df  = spark.createDataFrame([(3, "C"), (4, "D")], ["id", "val"])

# overwrite — replaces ALL existing data
base_df.write.mode("overwrite").saveAsTable("training.day2.write_demo")
print("After overwrite:", spark.table("training.day2.write_demo").count(), "rows")

# append — adds to existing data
new_df.write.mode("append").saveAsTable("training.day2.write_demo")
print("After append:",   spark.table("training.day2.write_demo").count(), "rows")

# ignore — does nothing if table exists
new_df.write.mode("ignore").saveAsTable("training.day2.write_demo")
print("After ignore:",   spark.table("training.day2.write_demo").count(), "rows (unchanged)")

spark.table("training.day2.write_demo").show()

# COMMAND ----------
# MAGIC %md ## Cell 10 — SQL Exercises

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Run these in SQL cells to practice
# MAGIC
# MAGIC -- Basic SELECT
# MAGIC SELECT department, COUNT(*) AS headcount, ROUND(AVG(salary), 2) AS avg_salary
# MAGIC FROM training.day2.employees
# MAGIC GROUP BY department
# MAGIC ORDER BY avg_salary DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- CTE + Window function
# MAGIC WITH dept_stats AS (
# MAGIC   SELECT department, AVG(salary) AS dept_avg
# MAGIC   FROM training.day2.employees
# MAGIC   WHERE department IS NOT NULL
# MAGIC   GROUP BY department
# MAGIC )
# MAGIC SELECT
# MAGIC   e.name,
# MAGIC   e.department,
# MAGIC   e.salary,
# MAGIC   d.dept_avg,
# MAGIC   RANK() OVER (PARTITION BY e.department ORDER BY e.salary DESC) AS salary_rank
# MAGIC FROM training.day2.employees e
# MAGIC JOIN dept_stats d ON e.department = d.department;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- MERGE upsert into Unity Catalog table
# MAGIC CREATE OR REPLACE TABLE training.day2.employees_updates (
# MAGIC   id INT, name STRING, department STRING, salary INT, hire_date STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO training.day2.employees_updates VALUES
# MAGIC   (1, 'Alice',  'Engineering', 110000, '2020-01-15'),  -- update salary
# MAGIC   (7, 'Grace',  'Finance',      80000, '2023-05-01');  -- new employee
# MAGIC
# MAGIC MERGE INTO training.day2.employees AS t
# MAGIC USING training.day2.employees_updates AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET t.salary = s.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, name, department, salary, hire_date)
# MAGIC   VALUES (s.id, s.name, s.department, s.salary, s.hire_date);
# MAGIC
# MAGIC SELECT * FROM training.day2.employees ORDER BY id;

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 2 Notebook Complete
# MAGIC
# MAGIC **What you practiced:**
# MAGIC - Creating Unity Catalog managed tables (`training.day2.*`)
# MAGIC - DataFrame transformations: filter, select, withColumn, groupBy, agg
# MAGIC - All join types including broadcast join
# MAGIC - Window functions: rank, row_number, lag
# MAGIC - Reading/writing CSV and Parquet via Unity Catalog Volumes
# MAGIC - Auto Loader (`cloudFiles`) ingesting from a Volume into a UC table
# MAGIC - Write modes: overwrite, append, ignore
# MAGIC - SQL: CTEs, window functions, MERGE
# MAGIC
# MAGIC **Azure-specific patterns used:**
# MAGIC - Unity Catalog: `training.day2.<table>` — no DBFS, no /tmp/
# MAGIC - Volumes: `/Volumes/training/day2/files/` — replaces /tmp/ for files
# MAGIC - `toTable("catalog.schema.table")` for streaming writes
# MAGIC - Auto Loader with Volume path as source
