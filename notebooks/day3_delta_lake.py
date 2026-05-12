# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3: Delta Lake Deep Dive
# MAGIC **Import this file into Databricks Community Edition:**
# MAGIC 1. Workspace → Import → File → upload `day3_delta_lake.py`
# MAGIC 2. Attach to cluster (Runtime 13.x LTS or higher)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Creating Delta Tables

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create a Delta table using SQL
# MAGIC DROP TABLE IF EXISTS default.sales;
# MAGIC
# MAGIC CREATE TABLE default.sales (
# MAGIC   sale_id    BIGINT,
# MAGIC   product    STRING,
# MAGIC   category   STRING,
# MAGIC   amount     DOUBLE,
# MAGIC   sale_date  DATE,
# MAGIC   region     STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert sample data
# MAGIC INSERT INTO default.sales VALUES
# MAGIC   (1, 'Laptop',   'Electronics', 1200.00, '2024-01-01', 'North'),
# MAGIC   (2, 'Mouse',    'Electronics',   25.00, '2024-01-02', 'South'),
# MAGIC   (3, 'Desk',     'Furniture',    450.00, '2024-01-03', 'East'),
# MAGIC   (4, 'Chair',    'Furniture',    250.00, '2024-01-04', 'West'),
# MAGIC   (5, 'Monitor',  'Electronics',  400.00, '2024-01-05', 'North'),
# MAGIC   (6, 'Keyboard', 'Electronics',   75.00, '2024-01-06', 'South'),
# MAGIC   (7, 'Lamp',     'Furniture',     80.00, '2024-01-07', 'East'),
# MAGIC   (8, 'Tablet',   'Electronics',  350.00, '2024-01-08', 'West');
# MAGIC
# MAGIC SELECT * FROM default.sales;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. ACID Transactions — MERGE (Upsert)

# COMMAND ----------
from pyspark.sql import functions as F, Row

# Create an "updates" DataFrame with new and changed records
updates = spark.createDataFrame([
    Row(sale_id=3, product='Standing Desk', category='Furniture', amount=899.00, sale_date='2024-01-03', region='East'),  # UPDATE
    Row(sale_id=9, product='Webcam', category='Electronics', amount=130.00, sale_date='2024-01-09', region='North'),     # INSERT
    Row(sale_id=10, product='Headphones', category='Electronics', amount=200.00, sale_date='2024-01-10', region='South'), # INSERT
]).withColumn("sale_date", F.col("sale_date").cast("date"))

updates.createOrReplaceTempView("sales_updates")

# COMMAND ----------
# MAGIC %sql
# MAGIC -- MERGE: upsert pattern
# MAGIC MERGE INTO default.sales AS target
# MAGIC USING sales_updates AS source
# MAGIC ON target.sale_id = source.sale_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.product   = source.product,
# MAGIC     target.amount    = source.amount
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (sale_id, product, category, amount, sale_date, region)
# MAGIC   VALUES (source.sale_id, source.product, source.category, source.amount, source.sale_date, source.region);
# MAGIC
# MAGIC SELECT * FROM default.sales ORDER BY sale_id;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Time Travel

# COMMAND ----------
# MAGIC %sql
# MAGIC -- View history of all transactions
# MAGIC DESCRIBE HISTORY default.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Read an older version (version 0 = initial insert)
# MAGIC SELECT * FROM default.sales VERSION AS OF 0
# MAGIC ORDER BY sale_id;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Read by timestamp (replace with an actual timestamp from DESCRIBE HISTORY above)
# MAGIC -- SELECT * FROM default.sales TIMESTAMP AS OF '2024-01-01 00:00:00';
# MAGIC
# MAGIC -- Restore to a previous version
# MAGIC -- RESTORE TABLE default.sales TO VERSION AS OF 0;
# MAGIC
# MAGIC -- How many versions exist?
# MAGIC SELECT COUNT(*) AS num_versions FROM (DESCRIBE HISTORY default.sales);

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Schema Evolution & Enforcement

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Schema enforcement: this WILL FAIL because 'discount' column doesn't exist
# MAGIC -- INSERT INTO default.sales VALUES (99, 'Projector', 'Electronics', 600.00, '2024-02-01', 'North', 0.10);

# COMMAND ----------
# Schema evolution: add a column with mergeSchema
new_data = spark.createDataFrame([
    Row(sale_id=11, product='Projector', category='Electronics', amount=600.00, sale_date='2024-02-01', region='North', discount=0.10),
]).withColumn("sale_date", F.col("sale_date").cast("date"))

new_data.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.sales")

display(spark.sql("SELECT * FROM default.sales WHERE sale_id = 11"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Change Data Feed (CDF)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Enable CDF on the table
# MAGIC ALTER TABLE default.sales SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
# MAGIC
# MAGIC -- Make a change
# MAGIC UPDATE default.sales SET amount = 1350.00 WHERE sale_id = 1;
# MAGIC DELETE FROM default.sales WHERE sale_id = 7;
# MAGIC
# MAGIC -- Read the change feed — shows _change_type: insert, update_preimage, update_postimage, delete
# MAGIC SELECT * FROM table_changes('default.sales', 3) ORDER BY _commit_version, _change_type;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. OPTIMIZE and ZORDER

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Compact small files into larger ones
# MAGIC OPTIMIZE default.sales;
# MAGIC
# MAGIC -- ZORDER: co-locate related data for fast range queries
# MAGIC OPTIMIZE default.sales ZORDER BY (category, region);
# MAGIC
# MAGIC -- Clean up old files (after OPTIMIZE, old versions are kept for time travel)
# MAGIC -- VACUUM default.sales RETAIN 168 HOURS;  -- keep 7 days
# MAGIC -- VACUUM default.sales DRY RUN;             -- preview what would be deleted

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Delta Table Properties & Metadata

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Detailed table info
# MAGIC DESCRIBE DETAIL default.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Table properties
# MAGIC SHOW TBLPROPERTIES default.sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Partition info (not partitioned in this example, but common exam topic)
# MAGIC -- CREATE TABLE default.sales_partitioned
# MAGIC -- USING DELTA
# MAGIC -- PARTITIONED BY (region)
# MAGIC -- AS SELECT * FROM default.sales;
# MAGIC
# MAGIC -- DESCRIBE EXTENDED default.sales_partitioned;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Medallion Architecture Demo

# COMMAND ----------
# Bronze: raw ingestion (no transformations)
bronze_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/databricks-datasets/samples/population-vs-price/data_geo.csv") \
    .withColumn("_ingest_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.input_file_name())

bronze_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/tmp/medallion/bronze/population")

print(f"Bronze rows: {bronze_df.count()}")
display(bronze_df.limit(3))

# COMMAND ----------
# Silver: cleaned, deduplicated, validated
silver_df = spark.read.format("delta").load("/tmp/medallion/bronze/population") \
    .dropDuplicates() \
    .dropna(subset=["City", "2014 rank"]) \
    .withColumnRenamed("2014 rank", "rank_2014") \
    .withColumnRenamed("City", "city") \
    .withColumnRenamed("State", "state") \
    .select("city", "state", "rank_2014")

silver_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/tmp/medallion/silver/population")

print(f"Silver rows: {silver_df.count()}")
display(silver_df.limit(5))

# COMMAND ----------
# Gold: aggregated, business-ready
gold_df = spark.read.format("delta").load("/tmp/medallion/silver/population") \
    .groupBy("state") \
    .agg(
        F.count("city").alias("city_count"),
        F.min("rank_2014").alias("highest_ranked_city_rank")
    ) \
    .orderBy(F.col("city_count").desc())

gold_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/tmp/medallion/gold/population_by_state")

print("Gold layer:")
display(gold_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 3 Practice Challenges
# MAGIC
# MAGIC 1. Create a Delta table, insert data, then UPDATE some rows and use `DESCRIBE HISTORY` to verify versions
# MAGIC 2. Perform a MERGE (upsert) combining new records + updates into your table
# MAGIC 3. Read version 0 of your table using time travel syntax
# MAGIC 4. Enable CDF on your table, make changes, then read the change feed
# MAGIC 5. Build a 3-layer medallion pipeline using any `/databricks-datasets/` dataset
