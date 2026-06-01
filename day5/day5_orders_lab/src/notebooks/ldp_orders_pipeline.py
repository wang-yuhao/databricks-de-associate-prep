# Databricks notebook source
# COMMAND ----------
# Cell 1 — Imports

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, current_timestamp

LANDING_PATH = "abfss://practice@dbsdatastorage.dfs.core.windows.net/orders/"
SCHEMA_PATH  = "abfss://practice@dbsdatastorage.dfs.core.windows.net/checkpoints/orders_schema/"

# Cell 2 — BRONZE: Auto Loader streaming ingest
@dlt.table(
    name    = "raw_orders",
    comment = "Bronze: raw orders ingested from ADLS Gen2 landing zone via Auto Loader",
    table_properties = {
        "quality": "bronze",
        "pipelines.reset.allowed": "false"  # prevent accidental full refresh in prod
    }
)
def raw_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",           "json")
        .option("cloudFiles.inferColumnTypes",  "true")
        .option("cloudFiles.schemaLocation",    SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unknown cols go to _rescued_data
        .load(LANDING_PATH)
    )

# Cell 3 — SILVER: Expectations enforce data quality
@dlt.table(
    name    = "clean_orders",
    comment = "Silver: validated and type-cast orders",
    table_properties = {"quality": "silver"}
)
@dlt.expect_or_drop("no_null_order_id",  "order_id IS NOT NULL")      # FAIL  — stops pipeline
@dlt.expect_or_drop("positive_amount",   "amount > 0")                # DROP  — removes bad rows
@dlt.expect("recent_date",               "order_date >= '2020-01-01'") # WARN  — logs, keeps rows
def clean_orders():
    return (
        dlt.read_stream("raw_orders")
        .withColumn("order_date",  to_date(col("order_date")))
        .withColumn("amount",      col("amount").cast("decimal(10,2)"))
        .withColumn("ingested_at", current_timestamp())
        .select("order_id", "order_date", "customer_id", "amount", "status", "ingested_at")
    )


# Cell 4 — GOLD: Materialized View for analytics
@dlt.table(
    name    = "customer_order_summary",
    comment = "Gold: per-customer order summary — refreshed on each pipeline run",
    table_properties = {"quality": "gold"}
)
def customer_order_summary():
    # Use dlt.read() (batch read) for the gold layer — no streaming needed
    return (
        dlt.read("clean_orders")
        .where(col("status") == "completed")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum("amount").alias("total_spend"),
            F.max("order_date").alias("last_order_date")
        )
    )
