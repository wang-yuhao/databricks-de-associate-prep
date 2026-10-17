# Databricks notebook source
# MAGIC %md
# MAGIC # Course 3 setup (included with `%run ./_setup`)
# MAGIC Serverless has **no Spark UI**. Every cell that runs Spark shows **See performance** under its output: click it,
# MAGIC then a query → **query profile**. That is where you read *files read / pruned*, *bytes read*, *rows*, shuffle,
# MAGIC spill, and the plan with per-operator metrics. The helpers below print the numbers you can get from code.

# COMMAND ----------

import time

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "perf_course"
FQ = f"{CATALOG}.{SCHEMA}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"USE {FQ}")
# Predictive optimization would silently OPTIMIZE / cluster our badly laid-out tables. Turn it off for this schema.
try:
    spark.sql(f"ALTER SCHEMA {FQ} DISABLE PREDICTIVE OPTIMIZATION")
except Exception as e:
    print("Could not disable predictive optimization:", str(e).splitlines()[0][:150])


def timed(df, label=""):
    """Execute a DataFrame fully (no-op sink, nothing is collected) and return wall-clock seconds."""
    t0 = time.time()
    try:
        df.write.format("noop").mode("overwrite").save()
    except Exception:
        df.count()
    secs = round(time.time() - t0, 2)
    print(f"{label:40s} {secs:7.2f} s")
    return secs


def table_files(table):
    """numFiles, size and average file size from DESCRIBE DETAIL (the 'small files' signal)."""
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    avg_mb = round(d.sizeInBytes / max(d.numFiles, 1) / 1024 / 1024, 2)
    out = {"table": table, "numFiles": d.numFiles, "sizeMB": round(d.sizeInBytes / 1024 / 1024, 1),
           "avgFileMB": avg_mb, "partitionColumns": list(d.partitionColumns),
           "clusteringColumns": list(d.clusteringColumns or [])}
    print(out)
    return out


def files_with_matches(table, predicate):
    """How many files contain at least one matching row = the minimum a perfect data skipper has to read.
    Compare with numFiles, and with 'files read' in the query profile of the same filter."""
    n = spark.table(table).filter(predicate).select(F.col("_metadata.file_path")).distinct().count()
    total = spark.sql(f"DESCRIBE DETAIL {table}").first().numFiles
    print(f"{table}: {n} of {total} files contain rows for [{predicate}]")
    return n


print(f"schema {FQ} ready. Helpers: timed(df, label), table_files(t), files_with_matches(t, predicate)")
