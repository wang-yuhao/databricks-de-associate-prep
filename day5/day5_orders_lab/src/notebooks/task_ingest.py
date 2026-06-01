# Databricks notebook source
# COMMAND ----------
# Simulates ingestion — passes record count downstream via task values
record_count = spark.table("lab.day5_dlt.raw_orders").count()

dbutils.jobs.taskValues.set(key="record_count", value=int(record_count))
dbutils.jobs.taskValues.set(key="run_date",     value=str(spark.sql("SELECT current_date()").collect()[0][0]))

print(f"[ingest] Records available: {record_count:,}")
