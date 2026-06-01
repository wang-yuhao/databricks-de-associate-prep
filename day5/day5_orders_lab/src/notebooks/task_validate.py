# Databricks notebook source
# COMMAND ----------
# Reads task value from ingest task and validates

count = dbutils.jobs.taskValues.get(
    taskKey    = "task_ingest",
    key        = "record_count",
    default    = 0,
    debugValue = 999    # used only in interactive runs
)

if count == 0:
    raise ValueError("Validation failed: no records found from ingest task")

print(f"[validate] {count:,} records passed validation")

