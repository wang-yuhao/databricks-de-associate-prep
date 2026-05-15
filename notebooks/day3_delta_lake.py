# Databricks notebook source
# MAGIC %md
# MAGIC # Day 3 — Delta Lake Deep Dive
# MAGIC ### ☁️ Azure Databricks Edition
# MAGIC
# MAGIC **Catalog / Schema:** `training.prep`
# MAGIC **Volume:** `/Volumes/training/prep/landing/`
# MAGIC
# MAGIC All tables are Unity Catalog **managed** Delta tables — no raw ADLS paths needed.

# COMMAND ----------
spark.sql("USE CATALOG training")
spark.sql("USE SCHEMA prep")
print("Ready:", spark.sql("SELECT current_catalog(), current_schema()").collect()[0])

# COMMAND ----------
# MAGIC %md ## 1. Create Delta Table + Inspect Transaction Log

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE training.prep.d3_sales (
# MAGIC   sale_id   BIGINT,
# MAGIC   product   STRING,
# MAGIC   region    STRING,
# MAGIC   amount    DOUBLE,
# MAGIC   sale_date DATE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Day 3 Delta Lake practice table';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Version 1: initial INSERT
# MAGIC INSERT INTO training.prep.d3_sales VALUES
# MAGIC   (1, 'Widget A', 'EU',   99.99,  '2024-01-10'),
# MAGIC   (2, 'Widget B', 'US',  149.50,  '2024-01-11'),
# MAGIC   (3, 'Gadget X', 'EU',  299.00,  '2024-01-12');

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Inspect transaction log (version history)
# MAGIC DESCRIBE HISTORY training.prep.d3_sales;
# MAGIC -- Version 0: CREATE TABLE
# MAGIC -- Version 1: WRITE (INSERT)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Table detail: storage location managed by UC, file stats
# MAGIC DESCRIBE DETAIL training.prep.d3_sales;

# COMMAND ----------
# MAGIC %md ## 2. UPDATE, DELETE, MERGE

# COMMAND ----------
# MAGIC %sql
# MAGIC -- UPDATE: increase EU prices 20%
# MAGIC UPDATE training.prep.d3_sales
# MAGIC SET amount = ROUND(amount * 1.20, 2)
# MAGIC WHERE region = 'EU';
# MAGIC
# MAGIC SELECT * FROM training.prep.d3_sales ORDER BY sale_id;
# MAGIC -- sale_id=1: 119.99, sale_id=3: 358.80

# COMMAND ----------
# MAGIC %sql
# MAGIC -- DELETE
# MAGIC DELETE FROM training.prep.d3_sales WHERE product = 'Gadget X';
# MAGIC SELECT * FROM training.prep.d3_sales ORDER BY sale_id;
# MAGIC -- sale_id=3 gone

# COMMAND ----------
# MAGIC %sql
# MAGIC -- MERGE (upsert)
# MAGIC CREATE OR REPLACE TABLE training.prep.d3_sales_updates (
# MAGIC   sale_id BIGINT, product STRING, region STRING, amount DOUBLE, sale_date DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO training.prep.d3_sales_updates VALUES
# MAGIC   (2, 'Widget B', 'US', 199.99, '2024-02-01'),   -- update existing
# MAGIC   (4, 'Gadget Y', 'APAC', 450.00, '2024-02-02'); -- new row
# MAGIC
# MAGIC MERGE INTO training.prep.d3_sales AS t
# MAGIC USING training.prep.d3_sales_updates AS s
# MAGIC ON t.sale_id = s.sale_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET t.amount = s.amount, t.sale_date = s.sale_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (sale_id, product, region, amount, sale_date)
# MAGIC   VALUES (s.sale_id, s.product, s.region, s.amount, s.sale_date);
# MAGIC
# MAGIC SELECT * FROM training.prep.d3_sales ORDER BY sale_id;
# MAGIC -- sale_id=2 amount=199.99; sale_id=4 present

# COMMAND ----------
# MAGIC %md ## 3. Time Travel

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Show full history
# MAGIC DESCRIBE HISTORY training.prep.d3_sales;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Query a specific version (before any updates)
# MAGIC SELECT * FROM training.prep.d3_sales VERSION AS OF 1;
# MAGIC -- Returns original 3 rows, original prices

# COMMAND ----------
# PySpark time travel
df_v1 = (spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .table("training.prep.d3_sales")
)
print("Version 1 row count:", df_v1.count())  # 3
df_v1.show()

# COMMAND ----------
# MAGIC %sql
# MAGIC -- RESTORE table to version 1
# MAGIC RESTORE TABLE training.prep.d3_sales TO VERSION AS OF 1;
# MAGIC SELECT * FROM training.prep.d3_sales ORDER BY sale_id;
# MAGIC -- Back to original 3 rows, original prices

# COMMAND ----------
# MAGIC %md ## 4. OPTIMIZE, ZORDER, and Liquid Clustering

# COMMAND ----------
from pyspark.sql import Row
from datetime import date, timedelta
import random

random.seed(42)
base = date(2024, 1, 1)
products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Device Z"]
regions  = ["EU", "US", "APAC", "LATAM"]

rows = [Row(
    sale_id=i,
    product=random.choice(products),
    region=random.choice(regions),
    amount=round(random.uniform(10, 500), 2),
    sale_date=base + timedelta(days=random.randint(0, 364)),
) for i in range(1, 5001)]

spark.createDataFrame(rows) \
    .write.mode("overwrite") \
    .saveAsTable("training.prep.d3_sales_large")
print("Written:", spark.table("training.prep.d3_sales_large").count(), "rows")
spark.sql("DESCRIBE DETAIL training.prep.d3_sales_large").select("numFiles").show()

# COMMAND ----------
# MAGIC %sql
# MAGIC -- OPTIMIZE: compact small files
# MAGIC OPTIMIZE training.prep.d3_sales_large;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- ZORDER: co-locate rows by filter columns for data skipping
# MAGIC OPTIMIZE training.prep.d3_sales_large ZORDER BY (sale_date, region);
# MAGIC DESCRIBE DETAIL training.prep.d3_sales_large;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Liquid Clustering: modern replacement for ZORDER (preferred for new tables)
# MAGIC CREATE OR REPLACE TABLE training.prep.d3_sales_clustered
# MAGIC CLUSTER BY (sale_date, region)
# MAGIC AS SELECT * FROM training.prep.d3_sales_large;
# MAGIC
# MAGIC -- Incremental clustering (no full rewrite needed)
# MAGIC OPTIMIZE training.prep.d3_sales_clustered;
# MAGIC
# MAGIC DESCRIBE DETAIL training.prep.d3_sales_clustered;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- VACUUM: delete old Parquet files beyond retention period
# MAGIC -- Default retention = 7 days = 168 hours
# MAGIC VACUUM training.prep.d3_sales;
# MAGIC -- WARNING: NEVER use RETAIN 0 HOURS in production — breaks time travel!

# COMMAND ----------
# MAGIC %md ## 5. Schema Evolution + Change Data Feed

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Add a new column (schema evolution via ALTER TABLE)
# MAGIC ALTER TABLE training.prep.d3_sales ADD COLUMN discount DOUBLE;
# MAGIC
# MAGIC INSERT INTO training.prep.d3_sales VALUES
# MAGIC   (10, 'Widget C', 'EU', 200.0, '2024-03-01', 0.10);
# MAGIC
# MAGIC SELECT * FROM training.prep.d3_sales ORDER BY sale_id;
# MAGIC -- Old rows have discount=NULL; row 10 has discount=0.10

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Enable Change Data Feed
# MAGIC ALTER TABLE training.prep.d3_sales
# MAGIC   SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC
# MAGIC UPDATE training.prep.d3_sales SET amount = 999.0 WHERE sale_id = 10;
# MAGIC
# MAGIC -- Read change feed
# MAGIC SELECT * FROM table_changes('training.prep.d3_sales', 1)
# MAGIC ORDER BY _commit_version, _change_type;
# MAGIC -- _change_type values: insert | update_preimage | update_postimage | delete

# COMMAND ----------
# MAGIC %md ## 6. Medallion Architecture (Bronze → Silver → Gold)

# COMMAND ----------
import json

# Write raw JSON files to Volume (simulating cloud file landing)
raw_events = [
    {"id": 1, "ts": "2024-01-15", "product": "Widget A", "amount": 100.0, "region": "EU"},
    {"id": 2, "ts": "2024-01-15", "product": "Widget B", "amount": -50.0, "region": "US"},  # bad
    {"id": 3, "ts": "2024-01-16", "product": "Gadget X", "amount": 300.0, "region": "EU"},
    {"id": 4, "ts": "2024-01-16", "product": None,       "amount": 150.0, "region": "APAC"}, # bad
    {"id": 5, "ts": "2024-01-17", "product": "Device Z", "amount": 500.0, "region": "US"},
]
for i, rec in enumerate(raw_events):
    dbutils.fs.put(
        f"/Volumes/training/prep/landing/medallion/raw_{i:04d}.json",
        json.dumps(rec), overwrite=True
    )
print("Raw files written")

# Bronze: ingest all raw data as-is
bronze_df = spark.read.option("inferSchema", "true").json("/Volumes/training/prep/landing/medallion/")
bronze_df.write.mode("overwrite").saveAsTable("training.prep.d3_bronze_events")
print("Bronze:", spark.table("training.prep.d3_bronze_events").count(), "rows")

# COMMAND ----------
from pyspark.sql.functions import col, to_date

# Silver: validate + clean
silver_df = (spark.table("training.prep.d3_bronze_events")
    .filter(col("amount") > 0)
    .filter(col("product").isNotNull())
    .withColumn("event_date", to_date(col("ts")))
    .drop("ts")
    .withColumnRenamed("id", "event_id")
)
silver_df.write.mode("overwrite").saveAsTable("training.prep.d3_silver_events")
print("Silver:", spark.table("training.prep.d3_silver_events").count(), "rows (expected 3)")
silver_df.show()

# COMMAND ----------
from pyspark.sql.functions import sum as spark_sum, count

# Gold: business aggregation
gold_df = (spark.table("training.prep.d3_silver_events")
    .groupBy("event_date", "region")
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("event_id").alias("transaction_count")
    )
    .orderBy("event_date", "region")
)
gold_df.write.mode("overwrite").saveAsTable("training.prep.d3_gold_daily_revenue")
gold_df.show()

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Verify all three medallion tables
# MAGIC SHOW TABLES IN training.prep LIKE 'd3_*';
# MAGIC SELECT 'bronze' AS layer, COUNT(*) AS rows FROM training.prep.d3_bronze_events
# MAGIC UNION ALL
# MAGIC SELECT 'silver',                 COUNT(*) FROM training.prep.d3_silver_events
# MAGIC UNION ALL
# MAGIC SELECT 'gold',                   COUNT(*) FROM training.prep.d3_gold_daily_revenue;
