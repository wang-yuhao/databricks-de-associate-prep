# Databricks notebook source
# MAGIC %md
# MAGIC # 3.2 Demo - File Explosion (Databricks Free Edition)
# MAGIC **Lessons:** Introduction to Designing the Foundation · File Explosion
# MAGIC
# MAGIC Same 5M rows, three layouts: well-sized files, thousands of tiny files, and over-partitioning. Then fix and prevent.
# MAGIC Databricks protects you by default (optimized writes, auto compaction, predictive optimization), so this demo
# MAGIC switches those off **per table** to reproduce the problem.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

NO_AUTO = "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')"
SCHEMA_DDL = "event_id BIGINT, device_id INT, event_type STRING, value DOUBLE, event_date DATE"


def events(n=5_000_000):
    return spark.range(n).select(
        F.col("id").alias("event_id"),
        (F.col("id") % 2000).cast("int").alias("device_id"),
        F.element_at(F.array(*[F.lit(x) for x in ["click", "view", "buy", "error"]]),
                     (F.col("id") % 4 + 1).cast("int")).alias("event_type"),
        F.rand(7).alias("value"),
        F.expr("date_add(DATE'2026-09-01', CAST(id % 30 AS INT))").alias("event_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Baseline: a well-sized table

# COMMAND ----------

events().write.mode("overwrite").saveAsTable("events_baseline")
table_files("events_baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explosion #1: many tiny files
# MAGIC Typical causes: frequent small appends (streaming with short triggers, row-by-row ingestion), or a
# MAGIC `repartition(n)` with a large `n`.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE events_tiny ({SCHEMA_DDL}) {NO_AUTO}")
events().repartition(2000).write.mode("append").saveAsTable("events_tiny")
table_files("events_tiny")

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explosion #2: over-partitioning
# MAGIC Partitioning by a high-cardinality column (2000 devices) creates a folder per value and at least one file each.
# MAGIC Rule of thumb: partition only tables > 1 TB, by a low-cardinality column that nearly every query filters on, with
# MAGIC ≥ 1 GB per partition. Otherwise use Liquid Clustering (lesson 3.3).

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE events_overpartitioned ({SCHEMA_DDL}) PARTITIONED BY (device_id) {NO_AUTO}")
events().repartition(8).write.mode("append").saveAsTable("events_overpartitioned")
table_files("events_overpartitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Measure
# MAGIC Run each query and open its query profile: compare **files read**, **bytes read** and the scan time.
# MAGIC Tiny files cost a request + open + footer read each, and many tiny tasks.

# COMMAND ----------

q = lambda t: spark.table(t).filter("event_type = 'buy'").groupBy("event_date").agg(F.avg("value"))  # noqa: E731
results = {t: timed(q(t), t) for t in ("events_baseline", "events_tiny", "events_overpartitioned")}

# COMMAND ----------

# MAGIC %md
# MAGIC Timings vary on shared serverless compute (run twice; the second run benefits from the disk cache). The file
# MAGIC counts are the reliable signal. Record your query-profile numbers:
# MAGIC
# MAGIC | Table | numFiles | files read | time (s) |
# MAGIC |---|---|---|---|
# MAGIC | events_baseline | | | |
# MAGIC | events_tiny | | | |
# MAGIC | events_overpartitioned | | | |

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Fix: compaction with `OPTIMIZE`
# MAGIC `OPTIMIZE` bin-packs small files into ~1 GB files (or `delta.targetFileSize`). It is idempotent and incremental.
# MAGIC Partitioning can't be fixed by OPTIMIZE (it compacts *within* each partition): that needs a rewrite.

# COMMAND ----------

display(spark.sql("OPTIMIZE events_tiny"))
table_files("events_tiny")
timed(q("events_tiny"), "events_tiny after OPTIMIZE")

# COMMAND ----------

display(spark.sql("OPTIMIZE events_overpartitioned"))
table_files("events_overpartitioned")

# COMMAND ----------

# Rewrite without the bad partitioning (CTAS + Liquid Clustering)
spark.sql("CREATE OR REPLACE TABLE events_fixed CLUSTER BY (device_id) AS SELECT * FROM events_overpartitioned")
spark.sql("OPTIMIZE events_fixed")
table_files("events_fixed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Prevent it
# MAGIC | Feature | What it does | How |
# MAGIC |---|---|---|
# MAGIC | Optimized writes | shuffle before the write so each task writes fewer, larger files | `delta.autoOptimize.optimizeWrite = true` (default on for most Databricks writes) |
# MAGIC | Auto compaction | after a write, compacts small files in the touched partitions | `delta.autoOptimize.autoCompact = true` |
# MAGIC | Target file size | file size goal for OPTIMIZE / writes | `delta.targetFileSize = '128mb'` |
# MAGIC | Predictive optimization | Databricks runs OPTIMIZE / VACUUM / ANALYZE on UC managed tables when useful | on by default; `ALTER SCHEMA/CATALOG ... ENABLE PREDICTIVE OPTIMIZATION` |

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE events_protected ({SCHEMA_DDL})
              TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')""")
events().repartition(2000).write.mode("append").saveAsTable("events_protected")   # the same bad write as in B
table_files("events_protected")

# COMMAND ----------

# MAGIC %md
# MAGIC **Small appends:** simulate a micro-batch stream with 30 tiny appends, with and without auto compaction.

# COMMAND ----------

for t, props in (("appends_plain", NO_AUTO), ("appends_autocompact", "TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")):
    spark.sql(f"CREATE OR REPLACE TABLE {t} ({SCHEMA_DDL}) {props}")
    for _ in range(30):
        events(10_000).write.mode("append").saveAsTable(t)
    table_files(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Key takeaways
# MAGIC - Check `numFiles` and the average file size (`DESCRIBE DETAIL`) of every important table.
# MAGIC - Don't partition small or medium tables; never by high-cardinality columns.
# MAGIC - Keep optimized writes + auto compaction on, run `OPTIMIZE` (or let predictive optimization do it).
# MAGIC
# MAGIC Re-enable predictive optimization when you finish the course: `ALTER SCHEMA workspace.perf_course INHERIT PREDICTIVE OPTIMIZATION`.
