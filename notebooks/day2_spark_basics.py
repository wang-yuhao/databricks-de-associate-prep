# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2: Spark Basics & Data Ingestion
# MAGIC **Import this file into Databricks Community Edition:**
# MAGIC 1. Go to Workspace → Import
# MAGIC 2. Select "File" and upload this `.py` file
# MAGIC 3. Attach to a cluster (any runtime ≥ 13.x LTS)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Reading Different File Formats

# COMMAND ----------
# Read CSV
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/databricks-datasets/samples/population-vs-price/data_geo.csv")
display(df_csv.limit(5))

# COMMAND ----------
# Read JSON
df_json = spark.read \
    .option("multiLine", "true") \
    .json("/databricks-datasets/iot/iot_devices.json")
display(df_json.limit(5))
print("Schema:")
df_json.printSchema()

# COMMAND ----------
# Read Parquet
df_parquet = spark.read.parquet("/databricks-datasets/samples/lending_club/parquet/")
display(df_parquet.limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. DataFrame Transformations

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Use the IoT devices dataset
df = df_json

# Select & rename
df_selected = df.select(
    F.col("device_id"),
    F.col("device_name"),
    F.col("ip"),
    F.col("battery_level"),
    F.col("signal"),
    F.col("timestamp")
)
print("Selected columns:")
display(df_selected.limit(3))

# COMMAND ----------
# Filter
df_low_battery = df.filter(F.col("battery_level") < 5)
print(f"Devices with battery < 5: {df_low_battery.count()}")

# Multiple conditions
df_filtered = df.filter(
    (F.col("battery_level") < 5) & 
    (F.col("signal") > 10)
)
print(f"Low battery AND good signal: {df_filtered.count()}")

# COMMAND ----------
# GroupBy and Aggregation
df_agg = df.groupBy("lcd") \
    .agg(
        F.count("*").alias("device_count"),
        F.avg("battery_level").alias("avg_battery"),
        F.max("signal").alias("max_signal"),
        F.min("signal").alias("min_signal")
    ) \
    .orderBy(F.col("device_count").desc())
display(df_agg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. SQL on DataFrames

# COMMAND ----------
# Register temp view
df.createOrReplaceTempView("iot_devices")

# Now query with SQL
result = spark.sql("""
    SELECT 
        lcd,
        COUNT(*) as device_count,
        ROUND(AVG(battery_level), 2) as avg_battery,
        MAX(signal) as max_signal
    FROM iot_devices
    WHERE battery_level > 0
    GROUP BY lcd
    ORDER BY device_count DESC
    LIMIT 10
""")
display(result)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Window Functions

# COMMAND ----------
from pyspark.sql.window import Window

# Rank devices by signal within each lcd group
window_spec = Window.partitionBy("lcd").orderBy(F.col("signal").desc())

df_ranked = df.withColumn("rank", F.rank().over(window_spec)) \
               .withColumn("dense_rank", F.dense_rank().over(window_spec)) \
               .withColumn("row_number", F.row_number().over(window_spec))

display(df_ranked.select("device_id", "lcd", "signal", "rank", "dense_rank", "row_number").limit(20))

# COMMAND ----------
# Running average (rolling window)
window_rows = Window.partitionBy("lcd").orderBy("timestamp").rowsBetween(-2, 0)

df_rolling = df.withColumn(
    "rolling_avg_battery",
    F.avg("battery_level").over(window_rows)
)
display(df_rolling.select("device_id", "lcd", "timestamp", "battery_level", "rolling_avg_battery").limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Joins

# COMMAND ----------
# Create two small DataFrames to demonstrate join types
from pyspark.sql import Row

employees = spark.createDataFrame([
    Row(emp_id=1, name="Alice", dept_id=10),
    Row(emp_id=2, name="Bob", dept_id=20),
    Row(emp_id=3, name="Charlie", dept_id=30),
    Row(emp_id=4, name="Diana", dept_id=10),
    Row(emp_id=5, name="Eve", dept_id=99),  # no matching dept
])

departments = spark.createDataFrame([
    Row(dept_id=10, dept_name="Engineering"),
    Row(dept_id=20, dept_name="Marketing"),
    Row(dept_id=30, dept_name="Sales"),
    # dept_id 99 missing — will show null in left join
])

# INNER JOIN — only matched rows
inner = employees.join(departments, on="dept_id", how="inner")
print("INNER JOIN:")
display(inner)

# LEFT JOIN — all employees, null for unmatched dept
left = employees.join(departments, on="dept_id", how="left")
print("LEFT JOIN:")
display(left)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Writing Data

# COMMAND ----------
# Write to Delta (default)
df_agg.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/tmp/day2_demo/iot_agg")

print("Written to Delta!")

# Verify
df_verify = spark.read.format("delta").load("/tmp/day2_demo/iot_agg")
display(df_verify)

# COMMAND ----------
# Write as Parquet
df_agg.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("/tmp/day2_demo/iot_agg_parquet")

# Write as CSV
df_agg.write \
    .mode("overwrite") \
    .option("header", "true") \
    .format("csv") \
    .save("/tmp/day2_demo/iot_agg_csv")

print("Written to Parquet and CSV!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Auto Loader (cloud file ingestion)

# COMMAND ----------
# Auto Loader reads new files incrementally
# In Community Edition we simulate with a directory

import os

# First, write sample CSV files to simulate an incoming directory
df_csv.limit(100).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/tmp/day2_incoming/batch_001")

# Auto Loader (cloudFiles format)
auto_loader_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/tmp/day2_autoloader_schema") \
    .option("header", "true") \
    .load("/tmp/day2_incoming")

# Write stream to Delta
query = auto_loader_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/day2_autoloader_checkpoint") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start("/tmp/day2_autoloader_output")

query.awaitTermination()

# Verify results
display(spark.read.format("delta").load("/tmp/day2_autoloader_output").limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 2 Practice Challenges
# MAGIC
# MAGIC 1. Read `/databricks-datasets/wine-quality/winequality-red.csv` and find the top 5 wines by quality
# MAGIC 2. Using window functions, rank wines within each quality group by alcohol content
# MAGIC 3. Join two datasets of your choice and write the result to Delta format
# MAGIC 4. Use Auto Loader to ingest a directory of CSV files into a Delta table
