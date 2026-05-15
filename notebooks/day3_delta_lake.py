# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 — Delta Lake Deep Dive on Azure Databricks
# MAGIC
# MAGIC **Environment:** Azure Databricks (Unity Catalog enabled)  
# MAGIC **Cluster:** Single Node, DBR 15.x LTS, `Standard_DS3_v2`  
# MAGIC **Estimated time:** 3–4 hours
# MAGIC
# MAGIC ## What you will practice:
# MAGIC - Creating Delta tables as Unity Catalog managed tables
# MAGIC - CRUD operations: INSERT, UPDATE, DELETE, MERGE
# MAGIC - Time travel: VERSION AS OF, TIMESTAMP AS OF, RESTORE
# MAGIC - OPTIMIZE, ZORDER, Liquid Clustering
# MAGIC - Schema evolution and enforcement
# MAGIC - Change Data Feed (CDF)

# COMMAND ----------
# MAGIC %md ## Setup — Create Catalog & Schema

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS training
# MAGIC   COMMENT 'Practice catalog for DE Associate exam prep';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS training.day3
# MAGIC   COMMENT 'Day 3: Delta Lake deep dive exercises';
# MAGIC
# MAGIC USE CATALOG training;
# MAGIC USE SCHEMA day3;
# MAGIC SELECT current_catalog(), current_schema();

# COMMAND ----------
# MAGIC %md ## Task 1 — Create a Delta Table and Explore the Transaction Log

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create a managed Delta table (Unity Catalog manages storage — no LOCATION needed)
# MAGIC CREATE OR REPLACE TABLE training.day3.sales (
# MAGIC   sale_id   BIGINT,
# MAGIC   product   STRING,
# MAGIC   region    STRING,
# MAGIC   amount    DOUBLE,
# MAGIC   sale_date DATE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Sales fact table for Delta Lake exercises';
# MAGIC
# MAGIC -- Insert first batch (version 1)
# MAGIC INSERT INTO training.day3.sales VALUES
# MAGIC   (1, 'Widget A', 'EU',   99.99,  '2024-01-10'),
# MAGIC   (2, 'Widget B', 'US',   149.50, '2024-01-11'),
# MAGIC   (3, 'Gadget X', 'EU',   299.00, '2024-01-12');
# MAGIC
# MAGIC -- View the transaction log history
# MAGIC DESCRIBE HISTORY training.day3.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Inspect table details (location, format, partitions, file count)
# MAGIC DESCRIBE DETAIL training.day3.sales;
# MAGIC -- Note: 'location' shows the Unity Catalog-managed ADLS Gen2 path automatically

# COMMAND ----------
# MAGIC %md ## Task 2 — CRUD Operations + MERGE

# COMMAND ----------
# MAGIC %sql
# MAGIC -- UPDATE: apply a price increase for EU region
# MAGIC UPDATE training.day3.sales
# MAGIC SET amount = ROUND(amount * 1.20, 2)
# MAGIC WHERE region = 'EU';
# MAGIC
# MAGIC SELECT * FROM training.day3.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- DELETE
# MAGIC DELETE FROM training.day3.sales
# MAGIC WHERE product = 'Gadget X';
# MAGIC
# MAGIC SELECT * FROM training.day3.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create a source table for MERGE (also a managed UC table)
# MAGIC CREATE OR REPLACE TABLE training.day3.sales_updates (
# MAGIC   sale_id   BIGINT,
# MAGIC   product   STRING,
# MAGIC   region    STRING,
# MAGIC   amount    DOUBLE,
# MAGIC   sale_date DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO training.day3.sales_updates VALUES
# MAGIC   (2, 'Widget B', 'US',  199.99, '2024-02-01'),  -- update existing
# MAGIC   (4, 'Gadget Y', 'APAC', 450.00, '2024-02-02'); -- new row
# MAGIC
# MAGIC -- MERGE: upsert from updates into target
# MAGIC MERGE INTO training.day3.sales AS t
# MAGIC USING training.day3.sales_updates AS s
# MAGIC ON t.sale_id = s.sale_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET t.amount = s.amount, t.sale_date = s.sale_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (sale_id, product, region, amount, sale_date)
# MAGIC   VALUES (s.sale_id, s.product, s.region, s.amount, s.sale_date);
# MAGIC
# MAGIC SELECT * FROM training.day3.sales ORDER BY sale_id;
# MAGIC -- Expected: sale_id=2 has 199.99; sale_id=4 is added; sale_id=3 is gone

# COMMAND ----------
# MAGIC %md ## Task 3 — Time Travel

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Show full version history
# MAGIC DESCRIBE HISTORY training.day3.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Query version 1 (the initial INSERT)
# MAGIC SELECT * FROM training.day3.sales VERSION AS OF 1;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Query by timestamp (replace with a value from DESCRIBE HISTORY above)
# MAGIC -- SELECT * FROM training.day3.sales TIMESTAMP AS OF '2024-01-11 00:00:00';
# MAGIC
# MAGIC -- Python equivalent:

# COMMAND ----------
# Python time travel
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("training.day3.sales")
print("Version 1 data:")
df_v1.show()

# COMMAND ----------
# MAGIC %sql
# MAGIC -- RESTORE to version 1 (undo all changes)
# MAGIC RESTORE TABLE training.day3.sales TO VERSION AS OF 1;
# MAGIC SELECT * FROM training.day3.sales;
# MAGIC -- Expected: original 3 rows

# COMMAND ----------
# MAGIC %md ## Task 4 — OPTIMIZE, ZORDER, and Liquid Clustering

# COMMAND ----------
# Generate a larger dataset to demonstrate OPTIMIZE benefits
from pyspark.sql import Row
from datetime import date, timedelta
import random

random.seed(42)
regions  = ["EU", "US", "APAC", "LATAM"]
products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Device Z"]

big_data = [
    Row(
        sale_id=i,
        product=random.choice(products),
        region=random.choice(regions),
        amount=round(random.uniform(10, 500), 2),
        sale_date=(date(2024, 1, 1) + timedelta(days=i % 365)),
    )
    for i in range(1, 10001)
]

big_df = spark.createDataFrame(big_data)
big_df.write.mode("overwrite").saveAsTable("training.day3.sales_large")
print(f"Created training.day3.sales_large with {big_df.count()} rows")

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Check file count BEFORE optimize
# MAGIC DESCRIBE DETAIL training.day3.sales_large;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- OPTIMIZE: compact small files into larger ones
# MAGIC OPTIMIZE training.day3.sales_large;
# MAGIC
# MAGIC -- Check file count AFTER optimize
# MAGIC DESCRIBE DETAIL training.day3.sales_large;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- ZORDER BY: co-locate data to speed up filters on these columns
# MAGIC OPTIMIZE training.day3.sales_large ZORDER BY (region, sale_date);
# MAGIC
# MAGIC -- Now queries filtering on region or sale_date will skip more files
# MAGIC SELECT COUNT(*) FROM training.day3.sales_large WHERE region = 'EU' AND sale_date >= '2024-06-01';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Liquid Clustering (modern alternative to ZORDER for new tables)
# MAGIC -- Define on table creation:
# MAGIC CREATE OR REPLACE TABLE training.day3.sales_clustered
# MAGIC   CLUSTER BY (region, sale_date)
# MAGIC   COMMENT 'Sales table using Liquid Clustering (no ZORDER needed)'
# MAGIC AS SELECT * FROM training.day3.sales_large;
# MAGIC
# MAGIC -- Incrementally cluster new data:
# MAGIC OPTIMIZE training.day3.sales_clustered;
# MAGIC
# MAGIC DESCRIBE DETAIL training.day3.sales_clustered;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- VACUUM: remove files older than retention threshold
# MAGIC -- Default retention = 7 days (168 hours)
# MAGIC -- ⚠️ NEVER use RETAIN 0 HOURS in production — it breaks time travel!
# MAGIC VACUUM training.day3.sales_large RETAIN 168 HOURS;

# COMMAND ----------
# MAGIC %md ## Task 5 — Schema Evolution and Enforcement

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE training.day3.schema_demo (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO training.day3.schema_demo VALUES (1, 'Alice'), (2, 'Bob');
# MAGIC SELECT * FROM training.day3.schema_demo;

# COMMAND ----------
# Schema enforcement: adding an extra column FAILS by default
try:
    new_data = spark.createDataFrame([(3, "Charlie", "Engineering")], ["id", "name", "department"])
    new_data.write.mode("append").saveAsTable("training.day3.schema_demo")
except Exception as e:
    print(f"Schema enforcement worked — write rejected:\n{e}")

# COMMAND ----------
# Schema evolution: mergeSchema allows adding new columns
new_data = spark.createDataFrame([(3, "Charlie", "Engineering")], ["id", "name", "department"])
new_data.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("training.day3.schema_demo")

print("After schema evolution (mergeSchema=true):")
spark.table("training.day3.schema_demo").show()
# Note: existing rows have NULL for the new 'department' column

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Alternatively use ALTER TABLE to add a column
# MAGIC ALTER TABLE training.day3.schema_demo ADD COLUMN email STRING;
# MAGIC DESCRIBE training.day3.schema_demo;

# COMMAND ----------
# MAGIC %md ## Task 6 — Change Data Feed (CDF)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Enable CDF on a Delta table
# MAGIC CREATE OR REPLACE TABLE training.day3.cdf_demo (
# MAGIC   id     INT,
# MAGIC   name   STRING,
# MAGIC   salary DOUBLE
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC INSERT INTO training.day3.cdf_demo VALUES (1, 'Alice', 80000), (2, 'Bob', 70000);
# MAGIC UPDATE training.day3.cdf_demo SET salary = 90000 WHERE id = 1;
# MAGIC DELETE FROM training.day3.cdf_demo WHERE id = 2;
# MAGIC
# MAGIC -- View the change feed (includes _change_type column)
# MAGIC SELECT * FROM table_changes('training.day3.cdf_demo', 1)
# MAGIC ORDER BY _commit_version, _change_type;
# MAGIC -- _change_type: insert | update_preimage | update_postimage | delete

# COMMAND ----------
# Read CDF as a stream (used in medallion pipeline: bronze→silver propagation)
cdf_stream = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("training.day3.cdf_demo")
)

print("CDF schema (includes _change_type, _commit_version, _commit_timestamp):")
cdf_stream.printSchema()

# COMMAND ----------
# MAGIC %md ## Task 7 — Medallion Architecture Demo

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bronze: raw ingest
# MAGIC CREATE OR REPLACE TABLE training.day3.bronze_orders (
# MAGIC   raw_order_id STRING,
# MAGIC   raw_date     STRING,
# MAGIC   customer_id  STRING,
# MAGIC   raw_amount   STRING,   -- string because raw data is unvalidated
# MAGIC   status       STRING
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Bronze: raw orders as-landed, no transformations';
# MAGIC
# MAGIC INSERT INTO training.day3.bronze_orders VALUES
# MAGIC   ('ORD001', '2024-01-15', 'C001', '150.00',  'completed'),
# MAGIC   ('ORD002', '2024-01-16', 'C002', '-50.00',  'completed'),  -- bad: negative
# MAGIC   ('ORD003', '2024-01-17', 'C003', '75.00',   'pending'),
# MAGIC   ('ORD004', '2024-01-18', 'C004', 'NULL',    'completed'),  -- bad: null
# MAGIC   ('ORD005', '2024-01-19', 'C005', '310.00',  'cancelled');

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: cleaned, typed, filtered
# MAGIC CREATE OR REPLACE TABLE training.day3.silver_orders
# MAGIC USING DELTA
# MAGIC COMMENT 'Silver: validated and typed orders'
# MAGIC AS
# MAGIC SELECT
# MAGIC   raw_order_id                      AS order_id,
# MAGIC   CAST(raw_date   AS DATE)          AS order_date,
# MAGIC   customer_id,
# MAGIC   CAST(raw_amount AS DOUBLE)        AS amount,
# MAGIC   status,
# MAGIC   current_timestamp()               AS processed_at
# MAGIC FROM training.day3.bronze_orders
# MAGIC WHERE raw_amount NOT IN ('NULL', '')
# MAGIC   AND CAST(raw_amount AS DOUBLE) > 0;
# MAGIC
# MAGIC SELECT * FROM training.day3.silver_orders;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: aggregated business metric
# MAGIC CREATE OR REPLACE TABLE training.day3.gold_daily_revenue
# MAGIC USING DELTA
# MAGIC COMMENT 'Gold: daily revenue aggregation for BI'
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*)       AS order_count,
# MAGIC   SUM(amount)    AS total_revenue,
# MAGIC   AVG(amount)    AS avg_order_value
# MAGIC FROM training.day3.silver_orders
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date;
# MAGIC
# MAGIC SELECT * FROM training.day3.gold_daily_revenue;

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 3 Notebook Complete
# MAGIC
# MAGIC **What you practiced:**
# MAGIC - Delta CRUD: INSERT, UPDATE, DELETE, MERGE
# MAGIC - Time travel: VERSION AS OF, TIMESTAMP AS OF, RESTORE
# MAGIC - OPTIMIZE, ZORDER, Liquid Clustering, VACUUM
# MAGIC - Schema enforcement and mergeSchema evolution
# MAGIC - Change Data Feed (CDF) for incremental propagation
# MAGIC - Full Bronze → Silver → Gold medallion pipeline
# MAGIC
# MAGIC **Azure-specific patterns:**
# MAGIC - All tables stored as Unity Catalog managed tables (`training.day3.*`)
# MAGIC - No LOCATION clause needed — Unity Catalog auto-manages ADLS Gen2 storage
# MAGIC - `table_changes()` for CDF works natively with UC tables
