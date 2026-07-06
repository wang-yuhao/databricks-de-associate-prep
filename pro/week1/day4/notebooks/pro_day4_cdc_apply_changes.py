# Databricks notebook source
# MAGIC %md
# MAGIC # Day 4 — CDC with APPLY CHANGES INTO
# MAGIC **Exam weight: ~10%**

# COMMAND ----------
# MAGIC %md ## Setup

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS training")
spark.sql("CREATE SCHEMA IF NOT EXISTS training.prep")
spark.sql("CREATE VOLUME IF NOT EXISTS training.prep.landing")

import json
from datetime import datetime

base_path = "/Volumes/training/prep/landing"
cdc_path  = f"{base_path}/cdc"
chk_path  = f"{base_path}/checkpoints/cdc_schema"

dbutils.fs.mkdirs(cdc_path)
dbutils.fs.mkdirs(chk_path)

print("Paths ready")

# COMMAND ----------
# MAGIC %md ## 4.1 Generate CDC sample data

# COMMAND ----------
cdc_records = [
    {"order_id": 1, "customer": "Alice",   "amount": 99.99,  "operation": "INSERT", "event_timestamp": "2024-01-01T10:00:00Z"},
    {"order_id": 2, "customer": "Bob",     "amount": 149.00, "operation": "INSERT", "event_timestamp": "2024-01-01T10:01:00Z"},
    {"order_id": 1, "customer": "Alice",   "amount": 109.99, "operation": "UPDATE", "event_timestamp": "2024-01-01T10:05:00Z"},
    {"order_id": 2, "customer": "Bob",     "amount": 149.00, "operation": "DELETE", "event_timestamp": "2024-01-01T10:10:00Z"},
]

for i, rec in enumerate(cdc_records):
    path = f"{cdc_path}/record_{i:03d}.json"
    dbutils.fs.put(path, json.dumps(rec), overwrite=True)

print(f"Wrote {len(cdc_records)} CDC records to {cdc_path}")

# COMMAND ----------
# MAGIC %md ## 4.2 APPLY CHANGES INTO — SCD Type 1 (SQL)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- In a real DLT/Lakeflow pipeline these would be pipeline cells.
# MAGIC -- Here we demo the logic in batch to verify understanding.
# MAGIC
# MAGIC -- Read raw CDC events
# MAGIC CREATE OR REPLACE TEMP VIEW orders_cdc_raw_view AS
# MAGIC SELECT *
# MAGIC FROM json.`/Volumes/training/prep/landing/cdc/`;
# MAGIC
# MAGIC SELECT * FROM orders_cdc_raw_view;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create target table
# MAGIC CREATE OR REPLACE TABLE training.prep.orders_silver (
# MAGIC   order_id    BIGINT,
# MAGIC   customer    STRING,
# MAGIC   amount      DOUBLE
# MAGIC ) USING DELTA;

# COMMAND ----------
# MAGIC %md
# MAGIC ### Simulate APPLY CHANGES INTO (SCD Type 1) with MERGE

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F

cdc_df = spark.read.json(cdc_path)

# Deduplicate: keep latest record per order_id
latest_df = (
    cdc_df
    .withColumn("_rank", F.rank().over(
        __import__('pyspark.sql.window', fromlist=['Window'])
        .Window.partitionBy("order_id")
        .orderBy(F.col("event_timestamp").desc())
    ))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

# Apply changes
target = DeltaTable.forName(spark, "training.prep.orders_silver")

target.alias("t").merge(
    latest_df.filter("operation != 'DELETE'").alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdate(set={"customer": "s.customer", "amount": "s.amount"}) \
 .whenNotMatchedInsert(values={"order_id": "s.order_id", "customer": "s.customer", "amount": "s.amount"}) \
 .execute()

# Handle DELETEs
deletes_df = cdc_df.filter("operation = 'DELETE'")
if deletes_df.count() > 0:
    delete_ids = [r.order_id for r in deletes_df.select("order_id").collect()]
    target.delete(F.col("order_id").isin(delete_ids))

spark.table("training.prep.orders_silver").show()

# COMMAND ----------
# MAGIC %md ## 4.3 APPLY CHANGES INTO — SCD Type 2 demo

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE training.prep.customers_silver_scd2 (
# MAGIC   customer_id   BIGINT,
# MAGIC   name          STRING,
# MAGIC   email         STRING,
# MAGIC   __START_AT    STRING,
# MAGIC   __END_AT      STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Insert initial record
# MAGIC INSERT INTO training.prep.customers_silver_scd2 VALUES
# MAGIC   (1, 'Alice', 'alice@old.com', '2024-01-01', NULL),
# MAGIC   (2, 'Bob',   'bob@example.com', '2024-01-01', NULL);

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Simulate SCD2 update: Alice changes email
# MAGIC -- Step 1: close old record
# MAGIC UPDATE training.prep.customers_silver_scd2
# MAGIC SET __END_AT = '2024-06-01'
# MAGIC WHERE customer_id = 1 AND __END_AT IS NULL;
# MAGIC
# MAGIC -- Step 2: insert new record
# MAGIC INSERT INTO training.prep.customers_silver_scd2 VALUES
# MAGIC   (1, 'Alice', 'alice@new.com', '2024-06-01', NULL);
# MAGIC
# MAGIC -- Query current records
# MAGIC SELECT * FROM training.prep.customers_silver_scd2
# MAGIC WHERE __END_AT IS NULL;

# COMMAND ----------
# MAGIC %md ## 4.4 DLT / Lakeflow Pipeline Reference (read-only)
# MAGIC
# MAGIC ```sql
# MAGIC -- Real DLT pipeline syntax (run in a Lakeflow pipeline, not standalone notebook)
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_cdc_raw
# MAGIC AS SELECT * FROM STREAM read_files(
# MAGIC   '/Volumes/training/prep/landing/cdc/',
# MAGIC   format => 'json'
# MAGIC );
# MAGIC
# MAGIC APPLY CHANGES INTO LIVE.orders_silver
# MAGIC FROM STREAM(LIVE.orders_cdc_raw)
# MAGIC KEYS (order_id)
# MAGIC APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC SEQUENCE BY event_timestamp
# MAGIC COLUMNS * EXCEPT (operation)
# MAGIC STORED AS SCD TYPE 1;
# MAGIC ```

# COMMAND ----------
# MAGIC %md ## ✅ Key Takeaways
# MAGIC - `APPLY CHANGES INTO` is the DLT API for CDC; handles INSERT/UPDATE/DELETE automatically
# MAGIC - SCD Type 1 = overwrite (no history); SCD Type 2 = new row per change
# MAGIC - `__START_AT` / `__END_AT` are auto-generated for SCD Type 2
# MAGIC - `SEQUENCE BY` determines ordering; `KEYS` identifies the primary key
# MAGIC - Without DLT: replicate with `MERGE INTO` + explicit DELETE handling
