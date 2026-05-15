# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5 — Delta Live Tables (DLT) Pipeline
# MAGIC ### ☁️ Azure Databricks Edition
# MAGIC
# MAGIC **✅ This notebook is a real DLT pipeline definition.**
# MAGIC Do NOT run it directly (cells will error outside DLT context).
# MAGIC Attach it to a **Delta Live Tables pipeline** in the Workflows UI.
# MAGIC
# MAGIC ## Setup Instructions
# MAGIC 1. Run `day5_dlt_setup` notebook first to write source JSON files
# MAGIC 2. Go to **Delta Live Tables** → **Create pipeline**
# MAGIC 3. Source code: this notebook
# MAGIC 4. Target schema: `training.prep`
# MAGIC 5. Pipeline mode: **Triggered**
# MAGIC 6. Click **Start**

# COMMAND ----------
import dlt
from pyspark.sql.functions import col, to_date, current_timestamp

# COMMAND ----------
# MAGIC %md ## Bronze — Raw Ingestion from Volume

# COMMAND ----------
@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from JSON landing zone via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    """
    Streaming Auto Loader source.
    Reads all JSON files dropped into the Volume landing path.
    Schema is inferred and persisted in schemaLocation for consistency.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/training/prep/landing/dlt_schema/bronze_orders",
        )
        .load("/Volumes/training/prep/landing/dlt_source/")
    )

# COMMAND ----------
# MAGIC %md ## Silver — Validated + Cleaned Orders

# COMMAND ----------
@dlt.table(
    name="silver_orders",
    comment="Validated and type-cast orders. Bad rows dropped via expectations.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id",  "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "amount > 0")
@dlt.expect(        "recent_date",     "order_date >= '2020-01-01'")
# ^ expect() only: logs warning metric, row is KEPT (not dropped)
def silver_orders():
    """
    Reads from bronze_orders stream.
    - expect_or_drop:  drops rows violating the constraint
    - expect:          records violation count in pipeline metrics but keeps row
    Cast order_date STRING → DATE and add ingestion timestamp.
    """
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("order_date",   to_date(col("order_date")))
        .withColumn("ingested_at",  current_timestamp())
    )

# COMMAND ----------
# MAGIC %md ## Gold — Daily Revenue by Region

# COMMAND ----------
@dlt.table(
    name="gold_daily_revenue",
    comment="Daily aggregated revenue per region (materialized view over silver)",
    table_properties={"quality": "gold"},
)
def gold_daily_revenue():
    """
    Batch read from silver_orders (materialized view, not streaming).
    Aggregates total revenue and order count per day per region.
    Refreshed on every pipeline run.
    """
    return (
        dlt.read("silver_orders")       # batch read — materialised view
        .groupBy("order_date", "region")
        .agg({"amount": "sum", "order_id": "count"})
        .withColumnRenamed("sum(amount)",    "total_revenue")
        .withColumnRenamed("count(order_id)", "order_count")
        .orderBy("order_date")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Expectation Reference
# MAGIC
# MAGIC | Decorator | SQL Clause | Violation Action |
# MAGIC |-----------|-----------|------------------|
# MAGIC | `@dlt.expect(name, expr)` | `EXPECT expr` | Log warning; **keep row** |
# MAGIC | `@dlt.expect_or_drop(name, expr)` | `EXPECT expr ON VIOLATION DROP ROW` | **Drop bad row** |
# MAGIC | `@dlt.expect_or_fail(name, expr)` | `EXPECT expr ON VIOLATION FAIL UPDATE` | **Stop pipeline** |
# MAGIC
# MAGIC After the pipeline runs, check the **Expectations** panel in the DLT UI to see
# MAGIC pass/fail counts per constraint.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Equivalent SQL Syntax (reference)
# MAGIC
# MAGIC The same pipeline can be written in SQL DLT notebooks:
# MAGIC
# MAGIC ```sql
# MAGIC -- Bronze
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC COMMENT 'Raw orders from landing zone'
# MAGIC AS SELECT *
# MAGIC FROM cloud_files(
# MAGIC   '/Volumes/training/prep/landing/dlt_source/',
# MAGIC   'json',
# MAGIC   map('cloudFiles.inferColumnTypes', 'true')
# MAGIC );
# MAGIC
# MAGIC -- Silver
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver_orders
# MAGIC   CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL)  ON VIOLATION DROP ROW
# MAGIC   CONSTRAINT positive_amount EXPECT (amount > 0)            ON VIOLATION DROP ROW
# MAGIC   CONSTRAINT recent_date     EXPECT (order_date >= '2020-01-01')
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   CAST(order_date AS DATE) AS order_date,
# MAGIC   customer_id,
# MAGIC   amount,
# MAGIC   region,
# MAGIC   current_timestamp() AS ingested_at
# MAGIC FROM LIVE.bronze_orders;
# MAGIC
# MAGIC -- Gold
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
# MAGIC COMMENT 'Daily revenue by region'
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   SUM(amount)     AS total_revenue,
# MAGIC   COUNT(order_id) AS order_count
# MAGIC FROM LIVE.silver_orders
# MAGIC GROUP BY order_date, region;
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verification (run in a SEPARATE notebook after pipeline completes)
# MAGIC
# MAGIC ```python
# MAGIC for tbl in ["bronze_orders", "silver_orders", "gold_daily_revenue"]:
# MAGIC     df = spark.table(f"training.prep.{tbl}")
# MAGIC     print(f"{tbl}: {df.count()} rows")
# MAGIC     df.show(5, truncate=False)
# MAGIC
# MAGIC # Expected:
# MAGIC # bronze_orders:       6 rows (all raw)
# MAGIC # silver_orders:       4 rows (2 bad rows dropped)
# MAGIC # gold_daily_revenue:  3 rows (aggregated by date+region)
# MAGIC ```
