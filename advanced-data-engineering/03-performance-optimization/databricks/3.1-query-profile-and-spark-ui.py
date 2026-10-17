# Databricks notebook source
# MAGIC %md
# MAGIC # 3.1 Demo - Spark UI Introduction → Query Profile on serverless (Databricks Free Edition)
# MAGIC **Lesson:** Spark UI Introduction
# MAGIC
# MAGIC The course teaches the Spark UI of a classic cluster: **job → stages → tasks**, the SQL/DataFrame tab, Storage,
# MAGIC Environment and Executors. Free Edition is serverless, where the Spark UI is replaced by the **query profile**
# MAGIC (same concepts, same plan, per-operator metrics). Map between the two:
# MAGIC
# MAGIC | Spark UI (classic) | Serverless / Free Edition |
# MAGIC |---|---|
# MAGIC | Jobs / Stages / Tasks tabs | cell → *See performance* → query → **query profile** (graph of stages/operators, task time, rows) |
# MAGIC | SQL / DataFrame tab (plan + metrics) | query profile graph + `df.explain("formatted")` |
# MAGIC | Storage tab (cached data) | n/a: `cache()` / `persist()` are not supported → write a table or temp view |
# MAGIC | Environment tab | `spark.conf.get(...)`, `SELECT current_version()` |
# MAGIC | Executors tab (GC, spill, cores) | managed for you; spill still appears in the query profile |
# MAGIC
# MAGIC If you have access to a classic cluster elsewhere, run the same cells there and open the Spark UI side by side.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

spark.range(0, 10_000_000).select(
    F.col("id").alias("order_id"),
    (F.col("id") % 100_000).alias("customer_id"),
    (F.rand(1) * 500).cast("decimal(10,2)").alias("amount"),
    F.expr("date_add(DATE'2026-01-01', CAST(id % 365 AS INT))").alias("order_date"),
).write.mode("overwrite").saveAsTable("orders_31")
table_files("orders_31")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. A narrow-only query = 1 stage
# MAGIC `filter`, `select`, `withColumn` work row by row inside a partition: no data moves between tasks.
# MAGIC Run the cell, click **See performance** → the query → look at the profile: scan → filter → project, **no Exchange**.
# MAGIC On serverless the plan runs on Photon, so operators show as `PhotonScan`, `PhotonFilter`, `PhotonProject` (inside
# MAGIC `AdaptiveSparkPlan`); the stage/shuffle logic is identical to the classic Spark operators.

# COMMAND ----------

narrow = spark.table("orders_31").filter("amount > 400").withColumn("amount_eur", F.col("amount") * 0.92)
narrow.explain("formatted")
timed(narrow, "narrow (no shuffle)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. A wide transformation = a shuffle = a stage boundary
# MAGIC `groupBy` needs all rows of a key in one task: an **Exchange (hashpartitioning)** appears. In the classic Spark UI
# MAGIC that is a new stage with shuffle write on one side and shuffle read on the other. Note the **partial** and **final**
# MAGIC `HashAggregate` around the exchange (map-side pre-aggregation reduces what is shuffled). On Photon they appear as
# MAGIC `PhotonGroupingAgg` (partial / final) around `PhotonShuffleExchangeSink` / `PhotonShuffleExchangeSource`.
# MAGIC
# MAGIC Classic Spark UI vocabulary: an **action** (`count`, `write`, `collect`) triggers one or more **jobs**; a job is
# MAGIC split into **stages** at shuffle boundaries (the DAG); each stage runs one **task per partition**.

# COMMAND ----------

wide = spark.table("orders_31").groupBy("customer_id").agg(F.sum("amount").alias("revenue"))
wide.explain("formatted")
timed(wide, "wide (groupBy -> shuffle)")

# COMMAND ----------

# MAGIC %md
# MAGIC In the query profile of this cell, find: **rows read**, **files read**, the **shuffle** operator (bytes written /
# MAGIC read), and the task-time distribution. These are the signals for every later lesson.
# MAGIC
# MAGIC ## C. The plan: logical → optimized → physical
# MAGIC `explain("extended")` shows the parsed, analyzed and optimized logical plans plus the physical plan. Look for
# MAGIC column pruning (only `order_date`, `customer_id` are read, not `amount` / `order_id`) and predicate pushdown
# MAGIC (`order_date = 2026-03-01` in the scan's data filters / `PushedFilters`).

# COMMAND ----------

spark.table("orders_31").filter("order_date = DATE'2026-03-01'").groupBy("customer_id").count().explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Caching on serverless
# MAGIC The Storage tab shows cached data on a classic cluster. On serverless, the DataFrame cache API isn't supported;
# MAGIC Databricks caches remote files on local SSD automatically (disk cache; see `read_io_cache_percent` in section F).
# MAGIC To reuse an expensive result, write a table.

# COMMAND ----------

try:
    wide.cache().count()
    print("cache() worked (classic compute)")
except Exception as e:
    print("cache() on serverless:", str(e).splitlines()[0][:200])
wide.write.mode("overwrite").saveAsTable("customer_revenue_31")   # the serverless way to reuse a result
timed(spark.table("customer_revenue_31"), "reuse the materialized result")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Environment
# MAGIC Only a few Spark settings can be changed on serverless (`spark.sql.shuffle.partitions`,
# MAGIC `spark.sql.files.maxPartitionBytes`, `spark.sql.session.timeZone`, `spark.sql.ansi.enabled`,
# MAGIC `spark.sql.legacy.timeParserPolicy`, `spark.databricks.execution.timeout`). Everything else is tuned for you.

# COMMAND ----------

display(spark.sql("SELECT current_version() AS version"))
for k in ["spark.sql.shuffle.partitions", "spark.sql.files.maxPartitionBytes", "spark.sql.adaptive.enabled",
          "spark.sql.autoBroadcastJoinThreshold"]:
    try:
        print(f"{k:45s} {spark.conf.get(k)}")
    except Exception as e:
        print(f"{k:45s} (not readable: {str(e).splitlines()[0][:60]})")
try:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    print("autoBroadcastJoinThreshold changed")
except Exception as e:
    print("Changing autoBroadcastJoinThreshold on serverless:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Optional: query history as data
# MAGIC `system.query.history` (if enabled in your workspace) stores the same metrics for every SQL warehouse and
# MAGIC serverless notebook/job query: `read_files`, `pruned_files`, `read_bytes`, `read_io_cache_percent`,
# MAGIC `shuffle_read_bytes`, `spilled_local_bytes`. Rows can appear with a few minutes of delay.

# COMMAND ----------

try:
    display(spark.sql("""SELECT start_time, statement_type, total_duration_ms, read_files, pruned_files,
                                read_bytes, read_io_cache_percent, shuffle_read_bytes, spilled_local_bytes, left(statement_text, 80) AS stmt
                         FROM system.query.history WHERE executed_by = current_user()
                         ORDER BY start_time DESC LIMIT 20"""))
except Exception as e:
    print("system.query.history not available:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Checklist for the next lessons
# MAGIC | Look at | Problem it reveals | Lesson |
# MAGIC |---|---|---|
# MAGIC | files read vs. table numFiles, avg file size | small files / missing skipping | 3.2, 3.3 |
# MAGIC | shuffle bytes, number of exchanges | shuffle | 3.4 |
# MAGIC | max task time ≫ median, one huge partition | skew | 3.5 |
# MAGIC | join output rows ≫ input rows | exploding join | 3.5 |
# MAGIC | spill (memory / disk) > 0 | spill | 3.5 |
# MAGIC | `BatchEvalPython` / `ArrowEvalPython` operators | serialization (UDFs) | 3.6 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - Transformations are lazy; an **action** triggers a **job**. Jobs split into **stages** at **shuffle** boundaries
# MAGIC   (wide transformations: `groupBy`, `join` without broadcast, `distinct`, `repartition`); a stage runs **one task per partition**.
# MAGIC - Narrow transformations (`filter`, `select`, `withColumn`) are pipelined inside one stage; `Exchange` in the plan = shuffle.
# MAGIC - **Skew**: in a stage's *Summary Metrics* (min / 25th / median / 75th / max task duration), max ≫ median, plus one
# MAGIC   task with far more shuffle-read bytes or records.
# MAGIC - **Spill** (memory / disk) in stage and task metrics means partitions did not fit in execution memory: fix with more /
# MAGIC   smaller partitions (`spark.sql.shuffle.partitions`, AQE) or by removing skew.
# MAGIC - Spark UI tabs: **Jobs**, **Stages** (DAG, task metrics), **Storage** (cached/persisted data), **Environment**
# MAGIC   (Spark confs, versions), **Executors** (cores, memory, GC time, shuffle), **SQL / DataFrame** (plan with per-operator metrics).
# MAGIC - Serverless / SQL warehouses have no Spark UI: use the **query profile** and `system.query.history`;
# MAGIC   `cache()` / `persist()` / `CACHE TABLE` are not supported there and most Spark confs cannot be set.
# MAGIC - `explain()` modes: `simple` (physical only), `extended` (logical + physical), `formatted` (operator list + details),
# MAGIC   `cost`, `codegen`.
