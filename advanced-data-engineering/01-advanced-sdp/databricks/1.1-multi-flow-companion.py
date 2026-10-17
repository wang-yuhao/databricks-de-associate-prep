# Databricks notebook source
# MAGIC %md
# MAGIC # 1.1 Demo - Multi Flow SDP with Liquid Clustering and Data Quality (Databricks)
# MAGIC Companion notebook for the `multi_flow` pipeline. The pipeline itself runs in *Jobs & Pipelines*; this notebook
# MAGIC inspects what each update did. Follow the steps in `FREE_EDITION_GUIDE.md` → Course 1 → Lesson 1.1.
# MAGIC
# MAGIC | Pipeline setting | Value |
# MAGIC |---|---|
# MAGIC | Source code | `01-advanced-sdp/pipelines/multi_flow/transformations` (later + `pipelines/multi_flow_extras/flow_c.py`) |
# MAGIC | Default catalog / schema | `workspace` / `sdp_1_1` |
# MAGIC | Configuration | `source_flow_a` = `/Volumes/workspace/sdp_course/landing/source_flow_a`, `source_flow_b` = `.../source_flow_b` |
# MAGIC | Event log (Advanced) | Publish to `workspace.sdp_1_1.event_log` |

# COMMAND ----------

# MAGIC %run ./_event_log_helpers

# COMMAND ----------

dbutils.widgets.text("schema", "workspace.sdp_1_1")
dbutils.widgets.text("event_log", "workspace.sdp_1_1.event_log")
SCHEMA, EVENT_LOG = dbutils.widgets.get("schema"), dbutils.widgets.get("event_log")
spark.sql(f"USE {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Explore the raw data (before the first update)
# MAGIC Every file is about 50 rows; roughly 20% are dirty: NULL `event_ts`, invalid `status`, or a negative `amount`.

# COMMAND ----------

LANDING = "/Volumes/workspace/sdp_course/landing"
raw = spark.read.json(f"{LANDING}/source_flow_*")
display(raw.groupBy("status").count().orderBy("status"))
display(raw.selectExpr("count_if(event_ts IS NULL) AS null_ts", "count_if(amount < 0) AS negative_amount",
                       "count_if(event_id IS NULL) AS null_event_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - After the first update: two flows, one table
# MAGIC In the pipeline graph, `bronze_events` has **two incoming flows** (`flow_a`, `flow_b`). Each has its own checkpoint.

# COMMAND ----------

display(spark.sql("SELECT source_flow, count(*) AS rows, count(DISTINCT source_file) AS files FROM bronze_events GROUP BY 1"))
display(flow_rows(EVENT_LOG))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Data quality: expectation metrics and where every row went
# MAGIC Compare with the pipeline UI: click `silver_events` → *Data quality* tab.
# MAGIC - `non_negative_amount` is **warn**: failed rows are counted but kept.
# MAGIC - `valid_timestamp`, `valid_status` are **drop**: failed rows are removed (and kept in `silver_quarantine`).
# MAGIC - `valid_event_id` is **fail**: one bad row stops the update.

# COMMAND ----------

display(expectation_metrics(EVENT_LOG))

# COMMAND ----------

display(spark.sql("""
SELECT (SELECT count(*) FROM bronze_events)                    AS bronze,
       (SELECT count(*) FROM silver_events)                    AS silver,
       (SELECT count(*) FROM silver_quarantine)                AS quarantine,
       (SELECT count(*) FROM silver_events WHERE amount < 0)   AS warned_but_kept"""))
display(spark.sql("SELECT failed_expectations, count(*) AS rows FROM silver_quarantine GROUP BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK:** `bronze = silver + quarantine` (no row lost), and `warned_but_kept > 0`.

# COMMAND ----------

c = spark.sql("""SELECT (SELECT count(*) FROM bronze_events) b, (SELECT count(*) FROM silver_events) s,
                        (SELECT count(*) FROM silver_quarantine) q""").first()
assert c.b == c.s + c.q, f"rows lost: bronze={c.b} silver={c.s} quarantine={c.q}"
print("OK - every Bronze row is either in Silver or in the quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Liquid Clustering
# MAGIC `cluster_by=["status", "ingest_date"]` in the code becomes `clusteringColumns` on the table.
# MAGIC On Databricks, pipeline tables are optimized (and clustered) automatically by predictive optimization / pipeline maintenance.

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL silver_events").select("name", "clusteringColumns", "numFiles", "sizeInBytes"))
display(spark.sql("DESCRIBE DETAIL gold_daily_metrics").select("name", "clusteringColumns", "numFiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Try (optional):** change the code to `cluster_by_auto=True` (instead of `cluster_by=[...]`) on `silver_events`,
# MAGIC run an update and look at `clusteringColumns` again. With automatic Liquid Clustering, Databricks picks keys
# MAGIC from the query workload, so it may stay empty until enough queries have run. Change it back afterwards.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Gold materialized view

# COMMAND ----------

display(spark.sql("SELECT * FROM gold_daily_metrics ORDER BY ingest_date, status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Incremental processing
# MAGIC In `00-land-course-data`, run the cell **"a new file in flow B only"**, then click **Start** on the pipeline.
# MAGIC Expected: `flow_a` 0 new rows, `flow_b` about 50 new rows. The MV is refreshed (incrementally if possible).

# COMMAND ----------

display(flow_rows(EVENT_LOG))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Add a third source without a full refresh
# MAGIC 1. In `00-land-course-data`, run the cell **"add a third source (flow C)"**.
# MAGIC 2. Pipeline → *Settings* → *Source code* → add the file `01-advanced-sdp/pipelines/multi_flow_extras/flow_c.py`.
# MAGIC 3. *Configuration* → add `source_flow_c` = `/Volumes/workspace/sdp_course/landing/source_flow_c`.
# MAGIC 4. **Start** (a normal update, not a full refresh). Only `flow_c` reads data; the other checkpoints are untouched.

# COMMAND ----------

display(spark.sql("SELECT source_flow, count(*) AS rows FROM bronze_events GROUP BY 1 ORDER BY 1"))
display(flow_rows(EVENT_LOG))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - `expect_or_fail` stops the update
# MAGIC 1. In `00-land-course-data`, run **"a file in flow A that contains one row with event_id = NULL"**. Start an update.
# MAGIC 2. The update **fails** on `silver_events`. Open the error in the pipeline UI. Nothing of that micro-batch is committed to Silver.

# COMMAND ----------

display(errors(EVENT_LOG))

# COMMAND ----------

# MAGIC %md
# MAGIC **Recover:** pipeline *Settings* → *Configuration* → add `event_id_policy` = `drop` → **Start**.
# MAGIC The failed micro-batch is reprocessed under the new rule: the NULL row stays in Bronze but not in Silver.

# COMMAND ----------

display(spark.sql("""SELECT (SELECT count(*) FROM bronze_events WHERE event_id IS NULL) AS null_ids_in_bronze,
                            (SELECT count(*) FROM silver_events WHERE event_id IS NULL) AS null_ids_in_silver"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Full refresh
# MAGIC Pipeline → the arrow next to **Start** → **Full refresh all**. Tables are truncated, checkpoints reset, and every
# MAGIC landed file is read again. Bronze ends up with the same row count as before.
# MAGIC Run the cell below before and after, and compare.

# COMMAND ----------

display(spark.sql("SELECT count(*) AS bronze_rows FROM bronze_events"))
display(spark.sql("DESCRIBE HISTORY bronze_events").select("version", "timestamp", "operation").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - SQL syntax (optional)
# MAGIC Create a second pipeline (schema `sdp_1_1_sql`) with source `01-advanced-sdp/databricks/sql/multi_flow.sql`
# MAGIC and the same configuration. Compare `CREATE FLOW ... INSERT INTO` and `CONSTRAINT ... EXPECT ... ON VIOLATION`
# MAGIC with the Python decorators. Free Edition runs **one active pipeline at a time**, so wait until the other one is idle.
