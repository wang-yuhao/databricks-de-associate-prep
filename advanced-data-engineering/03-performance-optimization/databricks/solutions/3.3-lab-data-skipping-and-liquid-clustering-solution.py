# Databricks notebook source
# MAGIC %md
# MAGIC # 3.3 Lab - Data Skipping and Liquid Clustering (Databricks Free Edition) - SOLUTION
# MAGIC **Lesson:** Data Skipping and Liquid Clustering
# MAGIC
# MAGIC Delta stores **min / max / null count** per column per file (first 32 columns, or `delta.dataSkippingStatsColumns`).
# MAGIC A filter skips every file whose [min, max] range can't contain the value. That only helps if the data is
# MAGIC **laid out** so each file covers a narrow range: Z-ORDER (old) or Liquid Clustering (new default).
# MAGIC
# MAGIC Measure each step two ways:
# MAGIC 1. `files_with_matches(table, predicate)`: files that contain matching rows (the best possible "files read").
# MAGIC 2. The query profile of the `timed(...)` cell: **files read** and **files pruned**. Fill in the table at the end.
# MAGIC
# MAGIC Lab shortcut: `delta.targetFileSize = '8mb'` so ~300 MB of data become ~40 files (real tables: 128 MB - 1 GB files).

# COMMAND ----------

# MAGIC %run ../_setup

# COMMAND ----------

SMALL_FILES = "'delta.targetFileSize' = '8mb', 'delta.autoOptimize.optimizeWrite' = 'false', 'delta.autoOptimize.autoCompact' = 'false'"
CUSTOMER_FILTER = "customer_id = 4242"
DATE_FILTER = "sale_date = DATE'2026-06-15'"

sales = spark.range(20_000_000).select(
    F.col("id").alias("sale_id"),
    (F.abs(F.xxhash64("id")) % 100_000).alias("customer_id"),
    F.expr("date_add(DATE'2026-01-01', CAST(abs(xxhash64(id, 1)) % 365 AS INT))").alias("sale_date"),
    (F.abs(F.xxhash64("id", F.lit(2))) % 1000).cast("int").alias("store_id"),
    (F.rand(3) * 300).cast("decimal(10,2)").alias("amount"))
sales.createOrReplaceTempView("sales_source")


def q(table, predicate):
    return spark.table(table).filter(predicate).agg(F.count("*").alias("rows"), F.sum("amount").alias("revenue"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Baseline: random layout
# MAGIC Create `sales_random` with the small-file properties and insert the source **shuffled into 40 files**.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE sales_random (sale_id BIGINT, customer_id BIGINT, sale_date DATE, store_id INT, amount DECIMAL(10,2)) TBLPROPERTIES ({SMALL_FILES})")
spark.table("sales_source").repartition(40).write.mode("append").saveAsTable("sales_random")
table_files("sales_random")
files_with_matches("sales_random", CUSTOMER_FILTER)
timed(q("sales_random", CUSTOMER_FILTER), "random layout, customer filter")

# COMMAND ----------

# CHECK step 1
assert spark.sql("DESCRIBE DETAIL sales_random").first().numFiles >= 30, "expected about 40 files"
print("Step 1 passed: the customer is spread over (almost) every file -> little can be skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Z-ORDER
# MAGIC Copy the table to `sales_zorder` and run `OPTIMIZE ... ZORDER BY (customer_id)`. Z-ORDER is not incremental (after new
# MAGIC data arrives it re-sorts whole partitions again) and can't be used together with Liquid Clustering or on partition columns.

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE sales_zorder TBLPROPERTIES ({SMALL_FILES}) AS SELECT * FROM sales_random")
display(spark.sql("OPTIMIZE sales_zorder ZORDER BY (customer_id)"))
table_files("sales_zorder")
files_with_matches("sales_zorder", CUSTOMER_FILTER)
timed(q("sales_zorder", CUSTOMER_FILTER), "z-ordered, customer filter")

# COMMAND ----------

# CHECK step 2
assert files_with_matches("sales_zorder", CUSTOMER_FILTER) <= 3, "after Z-ORDER the customer should be in very few files"
print("Step 2 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Liquid Clustering
# MAGIC Create `sales_lc` **clustered by customer_id**, insert the data, then `OPTIMIZE` (clustering happens on OPTIMIZE and,
# MAGIC on Databricks, incrementally on write for supported operations).

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE sales_lc
              (sale_id BIGINT, customer_id BIGINT, sale_date DATE, store_id INT, amount DECIMAL(10,2))
              CLUSTER BY (customer_id) TBLPROPERTIES ({SMALL_FILES})""")
spark.table("sales_source").write.mode("append").saveAsTable("sales_lc")
display(spark.sql("OPTIMIZE sales_lc"))
table_files("sales_lc")
files_with_matches("sales_lc", CUSTOMER_FILTER)
timed(q("sales_lc", CUSTOMER_FILTER), "liquid clustered, customer filter")

# COMMAND ----------

# CHECK step 3
assert spark.sql("DESCRIBE DETAIL sales_lc").first().clusteringColumns == ["customer_id"], "cluster by customer_id"
assert files_with_matches("sales_lc", CUSTOMER_FILTER) <= 3, "run OPTIMIZE to cluster the data"
print("Step 3 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Partitioning is the wrong tool here
# MAGIC 100,000 customers would mean 100,000 folders. Try it on the first 2,000 customers only (`customer_id < 2000`),
# MAGIC partitioned by `customer_id`, and look at the file count and average file size.
# MAGIC (`DISTRIBUTE BY customer_id` sends each customer to one task -> one file per partition; without it every
# MAGIC writer task opens a file in every partition and the count multiplies by the number of tasks.)

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE sales_partitioned PARTITIONED BY (customer_id) TBLPROPERTIES ({SMALL_FILES})
              AS SELECT * FROM sales_source WHERE customer_id < 2000 DISTRIBUTE BY customer_id""")
table_files("sales_partitioned")

# COMMAND ----------

# CHECK step 4
d = spark.sql("DESCRIBE DETAIL sales_partitioned").first()
assert d.partitionColumns == ["customer_id"] and d.numFiles >= 2000, "partition by customer_id"
print(f"Step 4 passed: {d.numFiles} files averaging {d.sizeInBytes / d.numFiles / 1024:.0f} KB - file explosion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Workloads change: switch the clustering key
# MAGIC Analysts now filter by `sale_date`. With Liquid Clustering you change the key **without rewriting the table**
# MAGIC by hand: `ALTER TABLE ... CLUSTER BY (...)`, then `OPTIMIZE FULL` to re-cluster existing data
# MAGIC (plain `OPTIMIZE` only clusters new data).

# COMMAND ----------

files_with_matches("sales_lc", DATE_FILTER)          # before: clustered by customer -> date is everywhere
spark.sql("ALTER TABLE sales_lc CLUSTER BY (sale_date)")
display(spark.sql("OPTIMIZE sales_lc FULL"))
files_with_matches("sales_lc", DATE_FILTER)
timed(q("sales_lc", DATE_FILTER), "re-clustered by date, date filter")

# COMMAND ----------

# CHECK step 5
assert spark.sql("DESCRIBE DETAIL sales_lc").first().clusteringColumns == ["sale_date"]
assert files_with_matches("sales_lc", DATE_FILTER) <= 3
print("Step 5 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC Multi-column keys: `CLUSTER BY (sale_date, store_id)` serves both filters reasonably (up to 4 keys).
# MAGIC `CLUSTER BY AUTO` lets Databricks choose and change keys from the observed query workload (needs predictive
# MAGIC optimization, so it won't act on this schema while PO is disabled).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Which columns have statistics?
# MAGIC Skipping needs stats on the filter column. Restrict `sales_zorder` stats to `sale_date` only, recompute stats,
# MAGIC and see that the customer filter can no longer skip (query profile: files pruned = 0), even though the data is
# MAGIC still Z-ordered by customer.

# COMMAND ----------

spark.sql("ALTER TABLE sales_zorder SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'sale_date')")
try:
    # Rewrites the per-file stats in the Delta log for the configured columns only (DBR 14.3+ / serverless).
    spark.sql("ANALYZE TABLE sales_zorder COMPUTE DELTA STATISTICS")
except Exception as e:
    print("ANALYZE ... COMPUTE DELTA STATISTICS not available here; existing files keep their old customer_id stats:",
          str(e).splitlines()[0][:150])
timed(q("sales_zorder", CUSTOMER_FILTER), "z-ordered but no customer stats")

# COMMAND ----------

# CHECK step 6
props = {r.key: r.value for r in spark.sql("SHOW TBLPROPERTIES sales_zorder").collect()}
assert props.get("delta.dataSkippingStatsColumns") == "sale_date"
print("Step 6 passed - now open the query profile: files pruned should be 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary - fill in from the query profiles
# MAGIC | Table / layout | Filter | files with matches | files read (profile) | files pruned (profile) | time |
# MAGIC |---|---|---|---|---|---|
# MAGIC | sales_random | customer | | | | |
# MAGIC | sales_zorder | customer | | | | |
# MAGIC | sales_lc (customer) | customer | | | | |
# MAGIC | sales_lc (re-clustered) | date | | | | |
# MAGIC | sales_zorder, no stats | customer | | | | |
# MAGIC
# MAGIC Liquid Clustering vs. alternatives: incremental (only unclustered data is rewritten), keys changeable, good for
# MAGIC high-cardinality keys, no small-partition problem. It replaces partitioning and Z-ORDER for new tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - File stats (min, max, null count, row count) are collected for the **first 32 columns** by default. Change with
# MAGIC   `delta.dataSkippingNumIndexedCols` (`-1` = all) or `delta.dataSkippingStatsColumns` (explicit list, takes precedence).
# MAGIC   Changing either does **not** recompute existing files: run `ANALYZE TABLE t COMPUTE DELTA STATISTICS`.
# MAGIC - Put filter / clustering columns where stats are collected; skipping and clustering on a column without stats is useless
# MAGIC   (Liquid Clustering keys **must** have stats).
# MAGIC - `OPTIMIZE t [WHERE <partition predicate>] ZORDER BY (c1, ...)`: not allowed on partition columns, loses
# MAGIC   effectiveness with every extra column, and is not incremental.
# MAGIC - Liquid Clustering: `CLUSTER BY (c1, ...)` (max **4** keys) on `CREATE` / CTAS, or `ALTER TABLE t CLUSTER BY (...)` on an
# MAGIC   existing **unpartitioned** table. Incompatible with `PARTITIONED BY` and `ZORDER BY`. Check with `DESCRIBE DETAIL`
# MAGIC   (`clusteringColumns`).
# MAGIC - Changing keys (`ALTER TABLE ... CLUSTER BY`) rewrites nothing; `OPTIMIZE t` clusters incrementally, `OPTIMIZE t FULL`
# MAGIC   re-clusters all existing data. `ALTER TABLE t CLUSTER BY NONE` stops clustering without rewriting data.
# MAGIC - `CLUSTER BY AUTO` lets predictive optimization pick and evolve keys (UC managed tables, PO enabled);
# MAGIC   `CREATE OR REPLACE` without `CLUSTER BY AUTO` turns it off.
# MAGIC - Partition only on low-cardinality columns with large partitions (~1 GB+); high-cardinality partition keys cause
# MAGIC   the small-file explosion seen in Step 4.
