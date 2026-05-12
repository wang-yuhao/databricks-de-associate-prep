# Databricks notebook source
# MAGIC %md
# MAGIC # Day 5: Delta Live Tables (DLT) Pipeline
# MAGIC
# MAGIC ## ⚠️ Important: DLT notebooks run differently!
# MAGIC DLT pipeline notebooks **cannot be run cell-by-cell**. They must be:
# MAGIC 1. Configured as a **Delta Live Tables pipeline** in the UI or via REST API
# MAGIC 2. Triggered via the **Workflows > Delta Live Tables** section
# MAGIC
# MAGIC ### Setup in Community Edition:
# MAGIC 1. Go to **Workflows** (left sidebar)
# MAGIC 2. Click **Delta Live Tables** tab → **Create Pipeline**
# MAGIC 3. Set:
# MAGIC    - Pipeline name: `day5_demo_pipeline`
# MAGIC    - Source: this notebook path
# MAGIC    - Target catalog/schema: `default` (or your Unity Catalog schema)
# MAGIC    - Pipeline mode: **Triggered** (not Continuous)
# MAGIC 4. Click **Start** to run
# MAGIC
# MAGIC > In Community Edition, DLT may be limited. Use the code as reference for exam questions.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze Layer — Raw Ingestion

# COMMAND ----------
@dlt.table(
    name="bronze_iot_raw",
    comment="Raw IoT device events — no transformations applied",
    table_properties={"quality": "bronze"}
)
def bronze_iot_raw():
    """
    Bronze table: ingest raw JSON data using Auto Loader.
    In a real pipeline this would point to cloud storage (ADLS, S3, GCS).
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/tmp/dlt/schema/bronze_iot")
            .load("/databricks-datasets/iot/")
            .withColumn("_ingest_timestamp", F.current_timestamp())
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver Layer — Cleaned & Validated

# COMMAND ----------
@dlt.table(
    name="silver_iot_cleaned",
    comment="Cleaned IoT events — nulls removed, types cast, duplicates dropped",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_battery", "battery_level >= 0 AND battery_level <= 100")
@dlt.expect("valid_signal",  "signal >= 0")
@dlt.expect_or_drop("non_null_device", "device_id IS NOT NULL")
def silver_iot_cleaned():
    """
    Silver table: apply quality checks using DLT expectations.
    - @dlt.expect: warns on violations but keeps the row
    - @dlt.expect_or_drop: drops rows that fail the check
    - @dlt.expect_or_fail: fails the pipeline on any violation
    """
    return (
        dlt.read_stream("bronze_iot_raw")
            .dropDuplicates(["device_id", "timestamp"])
            .withColumn("battery_level", F.col("battery_level").cast(IntegerType()))
            .withColumn("signal",        F.col("signal").cast(IntegerType()))
            .withColumn("timestamp",     F.col("timestamp").cast(LongType()))
            .select(
                "device_id", "device_name", "ip",
                "battery_level", "signal", "timestamp",
                "lcd", "_ingest_timestamp"
            )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Gold Layer — Aggregated Business Metrics

# COMMAND ----------
@dlt.table(
    name="gold_device_stats",
    comment="Per-LCD aggregated device health statistics",
    table_properties={"quality": "gold"}
)
def gold_device_stats():
    """
    Gold table: business-level aggregations.
    Uses dlt.read() (batch read from silver).
    """
    return (
        dlt.read("silver_iot_cleaned")
            .groupBy("lcd")
            .agg(
                F.count("device_id").alias("device_count"),
                F.round(F.avg("battery_level"), 2).alias("avg_battery"),
                F.round(F.avg("signal"), 2).alias("avg_signal"),
                F.count_if(F.col("battery_level") < 5).alias("critical_battery_devices")
            )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Live Tables vs. Streaming Tables

# COMMAND ----------
# @dlt.table — creates a LIVE TABLE (recomputed fully each run, like a view)
@dlt.table(
    name="gold_low_battery_alert",
    comment="Devices with battery < 5 — refreshed every pipeline run"
)
def gold_low_battery_alert():
    """
    LIVE TABLE: fully recomputed each time the pipeline runs.
    Best for small aggregations that need to reflect the current state.
    """
    return (
        dlt.read("silver_iot_cleaned")
            .filter(F.col("battery_level") < 5)
            .select("device_id", "device_name", "battery_level", "lcd")
            .orderBy("battery_level")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. DLT Expectations Summary
# MAGIC
# MAGIC | Decorator | Violation Behavior |
# MAGIC |---|---|
# MAGIC | `@dlt.expect(name, expr)` | Log warning metric; **keep** the row |
# MAGIC | `@dlt.expect_or_drop(name, expr)` | **Drop** rows that fail; log metric |
# MAGIC | `@dlt.expect_or_fail(name, expr)` | **Fail the pipeline** on any violation |
# MAGIC | `@dlt.expect_all(rules_dict)` | Apply multiple expectations; keep all |
# MAGIC | `@dlt.expect_all_or_drop(rules_dict)` | Drop rows failing any expectation |
# MAGIC | `@dlt.expect_all_or_fail(rules_dict)` | Fail on any violation across all rules |
# MAGIC
# MAGIC ## 6. DLT Pipeline Modes
# MAGIC
# MAGIC | Mode | Description | Use Case |
# MAGIC |---|---|---|
# MAGIC | **Triggered** | Runs once, processes all available data, stops | Batch ETL, scheduled jobs |
# MAGIC | **Continuous** | Runs continuously, low latency | Near-real-time streaming |
# MAGIC
# MAGIC ## 7. DLT Key Concepts for Exam
# MAGIC
# MAGIC - `dlt.read()` — batch read from another DLT table  
# MAGIC - `dlt.read_stream()` — streaming read from another DLT table  
# MAGIC - `LIVE` keyword in SQL: `CREATE OR REFRESH LIVE TABLE ...`  
# MAGIC - Tables created in the **target schema** specified in pipeline config  
# MAGIC - **Pipeline graph** shows data lineage automatically  
# MAGIC - Expectations appear in the **Pipeline UI** with pass/fail/dropped row counts

# COMMAND ----------
# MAGIC %md
# MAGIC ## ✅ Day 5 Practice Challenges
# MAGIC
# MAGIC 1. Create a DLT pipeline in the UI using this notebook
# MAGIC 2. Add a 4th table `gold_top_signal_devices` showing top 10 devices by signal per LCD group
# MAGIC 3. Change `@dlt.expect_or_drop` to `@dlt.expect_or_fail` on the battery check — observe the pipeline failing
# MAGIC 4. Run in **Triggered** mode vs. **Continuous** mode and explain the difference in pipeline behavior
# MAGIC 5. In a separate notebook, create a DLT pipeline using **SQL syntax** (`CREATE OR REFRESH STREAMING TABLE ...`)
