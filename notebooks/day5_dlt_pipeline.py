# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 — Delta Live Tables (DLT) Pipeline on Azure Databricks
# MAGIC
# MAGIC **Environment:** Azure Databricks (DLT is FULLY available — not available in Community Edition)  
# MAGIC **Cluster:** This notebook is designed to run as a **DLT Pipeline**, not on a regular cluster  
# MAGIC **Estimated time:** 2–3 hours
# MAGIC
# MAGIC ## How to Run This Notebook as a DLT Pipeline
# MAGIC
# MAGIC 1. In your Azure Databricks workspace, go to **Workflows** → **Delta Live Tables**
# MAGIC 2. Click **Create pipeline**
# MAGIC 3. Fill in:
# MAGIC    - **Pipeline name**: `orders-medallion-pipeline`
# MAGIC    - **Product edition**: `Advanced`
# MAGIC    - **Pipeline mode**: `Triggered`
# MAGIC    - **Source code**: Browse to this notebook
# MAGIC    - **Target catalog**: `training`
# MAGIC    - **Target schema**: `day5_dlt`
# MAGIC 4. Click **Create** then **Start**
# MAGIC 5. Watch the pipeline graph build in real time
# MAGIC
# MAGIC > ✔️ On Azure Databricks, DLT (Delta Live Tables) is fully supported.
# MAGIC > DLT automatically provisions a cluster, manages retries, and enforces data quality.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Architecture
# MAGIC
# MAGIC ```
# MAGIC raw_orders (Streaming Table / Bronze)
# MAGIC     │  Auto Loader from /Volumes/training/day5/files/orders/
# MAGIC     ▼
# MAGIC valid_orders (Materialized View / Silver)
# MAGIC     │  Expectations: valid_amount, valid_order_id, valid_date
# MAGIC     ▼
# MAGIC daily_revenue (Materialized View / Gold)
# MAGIC     │  Aggregated revenue per order_date
# MAGIC     ▼
# MAGIC customer_summary (Materialized View / Gold)
# MAGIC         Revenue and order count per customer
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Prepare Landing Data (Run This Cell BEFORE Creating the Pipeline)
# MAGIC
# MAGIC Run this in a **regular notebook** on your `learning-cluster` first, to create the source data
# MAGIC that the DLT pipeline will ingest.

# COMMAND ----------
# RUN THIS CELL ON A REGULAR CLUSTER (not inside the DLT pipeline)
# It creates the source data in a UC Volume for the pipeline to consume

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS training;
# MAGIC CREATE SCHEMA  IF NOT EXISTS training.day5;
# MAGIC CREATE VOLUME  IF NOT EXISTS training.day5.files
# MAGIC   COMMENT 'Source data landing area for Day 5 DLT pipeline';

# COMMAND ----------
# Write raw order JSON files to the Volume
# Run this on your regular learning-cluster before starting the DLT pipeline

raw_orders = [
    {"order_id": "ORD001", "order_date": "2024-01-15", "customer_id": "C001", "amount": 150.00, "status": "completed"},
    {"order_id": "ORD002", "order_date": "2024-01-16", "customer_id": "C002", "amount": -50.00, "status": "completed"},  # bad: negative
    {"order_id": None,    "order_date": "2024-01-17", "customer_id": "C003", "amount":  75.00, "status": "pending"},    # bad: null id
    {"order_id": "ORD004", "order_date": "2024-01-18", "customer_id": "C004", "amount": 200.00, "status": "completed"},
    {"order_id": "ORD005", "order_date": "2024-01-19", "customer_id": "C005", "amount": 310.00, "status": "cancelled"},
    {"order_id": "ORD006", "order_date": "2024-01-20", "customer_id": "C001", "amount": 425.00, "status": "completed"},
    {"order_id": "ORD007", "order_date": "2019-12-31", "customer_id": "C006", "amount":  90.00, "status": "completed"},  # bad: old date
]

source_df = spark.createDataFrame(raw_orders)
source_df.write \
    .mode("overwrite") \
    .json("/Volumes/training/day5/files/orders/batch1")

print("Source data written to /Volumes/training/day5/files/orders/batch1")
source_df.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## DLT Pipeline Code Begins Below
# MAGIC
# MAGIC The cells below define the actual DLT pipeline.
# MAGIC They use the `dlt` library which is ONLY available when running inside a DLT pipeline context.
# MAGIC **Do not run these cells on a regular cluster** — import this notebook into a DLT pipeline.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# COMMAND ----------
# MAGIC %md ### Bronze Layer — Raw Ingestion via Auto Loader

# COMMAND ----------
@dlt.table(
    name="raw_orders",
    comment="Bronze: raw orders as-landed from source volume. No transformations.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def raw_orders():
    """
    Streaming table: ingests new JSON files from the Volume using Auto Loader.
    In DLT, this is a Streaming Table (append-only source).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        # Source: Unity Catalog Volume (persistent, Azure ADLS-backed)
        .load("/Volumes/training/day5/files/orders/")
    )

# COMMAND ----------
# MAGIC %md ### Silver Layer — Validated and Typed Orders

# COMMAND ----------
@dlt.table(
    name="valid_orders",
    comment="Silver: cleaned, typed, and validated orders. Bad rows dropped or flagged.",
    table_properties={"quality": "silver"},
)
# Expectation 1: WARN only — log the violation but keep the row
@dlt.expect("valid_date", "order_date >= '2020-01-01'")
# Expectation 2: DROP bad rows silently
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
# Expectation 3: DROP bad rows silently
@dlt.expect_or_drop("positive_amount", "amount > 0")
def valid_orders():
    """
    Materialized View: reads from the bronze streaming table.
    DLT automatically tracks which records have been processed.
    """
    return (
        dlt.read_stream("raw_orders")  # read from the bronze table defined above
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("amount", F.col("amount").cast("double"))
        .select(
            "order_id",
            "order_date",
            "customer_id",
            "amount",
            "status",
            F.current_timestamp().alias("processed_at"),
        )
    )

# COMMAND ----------
# MAGIC %md ### Gold Layer — Aggregated Business Metrics

# COMMAND ----------
@dlt.table(
    name="daily_revenue",
    comment="Gold: total revenue and order count per day. Used by BI dashboards.",
    table_properties={"quality": "gold"},
)
def daily_revenue():
    """
    Materialized View reading from silver layer.
    DLT will refresh this on each pipeline run.
    """
    return (
        dlt.read("valid_orders")  # batch read (not streaming) for Gold layer
        .groupBy("order_date")
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.round(F.avg("amount"), 2).alias("avg_order_value"),
        )
        .orderBy("order_date")
    )

# COMMAND ----------
@dlt.table(
    name="customer_summary",
    comment="Gold: per-customer revenue and order count.",
    table_properties={"quality": "gold"},
)
def customer_summary():
    return (
        dlt.read("valid_orders")
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("lifetime_value"),
            F.max("order_date").alias("last_order_date"),
        )
        .orderBy(F.col("lifetime_value").desc())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Querying DLT Results After Pipeline Run
# MAGIC
# MAGIC After the pipeline runs, query results from a regular notebook:

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Run these in a regular notebook AFTER the DLT pipeline has completed
# MAGIC
# MAGIC -- Check pipeline output tables in Unity Catalog
# MAGIC SHOW TABLES IN training.day5_dlt;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM training.day5_dlt.daily_revenue;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM training.day5_dlt.customer_summary;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Check expectation metrics via the DLT event log
# MAGIC -- Replace <pipeline_id> with your actual pipeline ID from the DLT UI
# MAGIC -- SELECT * FROM event_log('<pipeline_id>')
# MAGIC -- WHERE details:flow_progress:data_quality IS NOT NULL
# MAGIC -- ORDER BY timestamp DESC LIMIT 20;

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## DLT SQL Equivalent (Alternative Syntax)
# MAGIC
# MAGIC The same pipeline can also be written in SQL:

# COMMAND ----------
# MAGIC %md
# MAGIC ```sql
# MAGIC -- Bronze
# MAGIC CREATE OR REFRESH STREAMING TABLE raw_orders
# MAGIC COMMENT 'Bronze: raw orders from landing volume'
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   '/Volumes/training/day5/files/orders/',
# MAGIC   'json',
# MAGIC   map('cloudFiles.inferColumnTypes', 'true')
# MAGIC );
# MAGIC
# MAGIC -- Silver with Expectations
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW valid_orders
# MAGIC   CONSTRAINT valid_date     EXPECT (order_date >= '2020-01-01'),
# MAGIC   CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL)     ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT positive_amount EXPECT (amount > 0)               ON VIOLATION DROP ROW
# MAGIC COMMENT 'Silver: validated and typed orders'
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   CAST(order_date AS DATE) AS order_date,
# MAGIC   customer_id,
# MAGIC   CAST(amount AS DOUBLE)   AS amount,
# MAGIC   status,
# MAGIC   current_timestamp()      AS processed_at
# MAGIC FROM LIVE.raw_orders;
# MAGIC
# MAGIC -- Gold: daily revenue
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
# MAGIC COMMENT 'Gold: daily revenue for BI'
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(order_id)     AS order_count,
# MAGIC   ROUND(SUM(amount), 2) AS total_revenue,
# MAGIC   ROUND(AVG(amount), 2) AS avg_order_value
# MAGIC FROM LIVE.valid_orders
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date;
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 5 Notebook Complete
# MAGIC
# MAGIC **What you practiced:**
# MAGIC - Real DLT pipeline with Bronze → Silver → Gold architecture
# MAGIC - DLT table types: Streaming Table (Bronze), Materialized View (Silver/Gold)
# MAGIC - DLT Expectations: `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`
# MAGIC - Auto Loader as DLT source (`cloudFiles` from Unity Catalog Volume)
# MAGIC - `dlt.read_stream()` vs `dlt.read()` — streaming vs batch reads within DLT
# MAGIC - SQL equivalent syntax for DLT
# MAGIC - Querying DLT results from Unity Catalog after pipeline run
# MAGIC
# MAGIC **Azure-specific:**
# MAGIC - DLT is fully available on Azure Databricks — no simulation needed
# MAGIC - Target catalog/schema set in pipeline config: `training.day5_dlt`
# MAGIC - Source data in Unity Catalog Volume: `/Volumes/training/day5/files/orders/`
# MAGIC - DLT auto-provisions cluster, manages retries, and writes to UC automatically
