# Databricks notebook source
# MAGIC %md
# MAGIC # 3.2 Demo - File Explosion (Databricks Free Edition)
# MAGIC **Lessons:** Introduction to Designing the Foundation · File Explosion
# MAGIC
# MAGIC Same 5M rows, three layouts: well-sized files, thousands of tiny files, and over-partitioning. Then fix and prevent.
# MAGIC Databricks protects you by default (optimized writes, auto compaction, predictive optimization), so this demo
# MAGIC switches those off **per table** to reproduce the problem.
# MAGIC Run the cells in order without long pauses: if `numFiles` of a "bad" table later drops on its own, background
# MAGIC maintenance compacted it; just re-run that section.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

NO_AUTO = "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false')"
SCHEMA_DDL = "event_id BIGINT, device_id INT, event_type STRING, value DOUBLE, event_date DATE"


def events(n=5_000_000):
    return spark.range(n).select(
        F.col("id").alias("event_id"),
        (F.col("id") % 500).cast("int").alias("device_id"),
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
# MAGIC Partitioning by a high-cardinality column (500 devices) creates a folder per value, and **every writing task writes
# MAGIC one file into every partition it holds rows for**: 4 tasks x 500 values = ~2000 files of ~2,500 rows.
# MAGIC Current guidance: don't partition tables under 1 TB; if you do, use a low-cardinality column that nearly every
# MAGIC query filters on and keep ≥ 1 GB per partition. Databricks recommends Liquid Clustering (lesson 3.3) instead for
# MAGIC all managed tables.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE events_overpartitioned ({SCHEMA_DDL}) PARTITIONED BY (device_id) {NO_AUTO}")
events().repartition(4).write.mode("append").saveAsTable("events_overpartitioned")
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
# MAGIC `OPTIMIZE` bin-packs small files toward a target size (`delta.targetFileSize` if set; otherwise autotuned, on UC
# MAGIC managed tables from table size: 256 MB below 2.56 TB up to 1 GB above 10 TB). Bin-packing is idempotent (a second
# MAGIC run on unchanged data does nothing) and only rewrites small files. Readers keep working (snapshot isolation); the
# MAGIC replaced files stay on storage until `VACUUM` removes them after the retention period (default 7 days).
# MAGIC Partitioning can't be fixed by OPTIMIZE (it compacts *within* each partition, so you still get one file per
# MAGIC partition): that needs a rewrite.

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
# MAGIC | Optimized writes | adaptive shuffle before the write so each partition gets fewer, larger files | `delta.autoOptimize.optimizeWrite = true`. Always on for `MERGE` / `UPDATE` / `DELETE` with subqueries; on for `CTAS` / `INSERT` on SQL warehouses and into partitioned UC tables (DBR 13.3+) |
# MAGIC | Auto compaction | synchronously after a successful write, compacts small files in the touched partitions | `delta.autoOptimize.autoCompact = 'auto'` (recommended; `true` = legacy, fixed 128 MB target). UC managed tables also get background auto compaction |
# MAGIC | Target file size | file size goal for layout operations | `delta.targetFileSize = '128mb'` (no default; autotuned when unset; on UC managed tables only `OPTIMIZE` respects it) |
# MAGIC | Predictive optimization | Databricks runs OPTIMIZE / VACUUM / ANALYZE on UC managed tables on serverless compute (never ZORDER) | default on for accounts created since Nov 2024; `ALTER {CATALOG \| SCHEMA \| TABLE} x {ENABLE \| DISABLE \| INHERIT} PREDICTIVE OPTIMIZATION` |

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE events_protected ({SCHEMA_DDL})
              TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')""")
events().repartition(2000).write.mode("append").saveAsTable("events_protected")   # the same bad write as in B
table_files("events_protected")

# COMMAND ----------

# MAGIC %md
# MAGIC **Small appends:** simulate a micro-batch stream with 30 tiny appends (4 files each), with and without auto
# MAGIC compaction. Auto compaction only triggers once enough small files have accumulated (default threshold 50), so the
# MAGIC compacted table ends up with a handful of files instead of ~120. Takes a few minutes (60 small commits).

# COMMAND ----------

for t, props in (("appends_plain", NO_AUTO), ("appends_autocompact", "TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')")):
    spark.sql(f"CREATE OR REPLACE TABLE {t} ({SCHEMA_DDL}) {props}")
    for _ in range(30):
        events(10_000).repartition(4).write.mode("append").saveAsTable(t)
    table_files(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Key takeaways
# MAGIC - Check `numFiles` and the average file size (`DESCRIBE DETAIL`) of every important table.
# MAGIC - Don't partition small or medium tables; never by high-cardinality columns.
# MAGIC - Keep optimized writes + auto compaction on, run `OPTIMIZE` (or let predictive optimization do it).
# MAGIC
# MAGIC Re-enable predictive optimization when you finish the course: `ALTER SCHEMA workspace.perf_course INHERIT PREDICTIVE OPTIMIZATION`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - Small-file problem: diagnose with `DESCRIBE DETAIL` (`numFiles`, `sizeInBytes`) and the query profile (files read). Each file costs an open + footer read + task overhead.
# MAGIC - `OPTIMIZE` bin-packing is idempotent, changes no data (reads before/after return the same rows) and compacts only *within* partitions. `OPTIMIZE ... ZORDER BY` is not idempotent and is incompatible with Liquid Clustering tables.
# MAGIC - `OPTIMIZE` never deletes files: the old small files remain (time travel works) until `VACUUM` removes files no longer referenced and older than the retention (default 7 days).
# MAGIC - Optimized writes = shuffle *before* the write (fewer, larger files per partition); auto compaction = synchronous small-file compaction *after* the write. Optimized writes are always on for `MERGE` / `UPDATE` / `DELETE`.
# MAGIC - Partitioning: don't partition tables < 1 TB, never by high-cardinality columns, aim for ≥ 1 GB per partition. Changing partitioning needs a full rewrite; Liquid Clustering keys can be changed with `ALTER TABLE ... CLUSTER BY`.
# MAGIC - Predictive optimization runs `OPTIMIZE`, `VACUUM` and `ANALYZE` automatically on UC **managed** tables only (not external), set via `ALTER CATALOG | SCHEMA | TABLE ... ENABLE | DISABLE | INHERIT PREDICTIVE OPTIMIZATION`.
# MAGIC - `delta.targetFileSize` overrides autotuned file sizes; on UC managed tables only `OPTIMIZE` respects it.
