# Databricks notebook source
# MAGIC %md
# MAGIC # 1.4 Demo - Advanced Data Quality Checks and Expectations (Databricks)
# MAGIC Companion notebook for the `advanced_dq` pipeline. Guide: `FREE_EDITION_GUIDE.md` → Course 1 → Lesson 1.4.
# MAGIC
# MAGIC | Pipeline setting | Value |
# MAGIC |---|---|
# MAGIC | Source code | `01-advanced-sdp/pipelines/advanced_dq/transformations` |
# MAGIC | Default catalog / schema | `workspace` / `sdp_1_4` |
# MAGIC | Configuration | `orders_source` = `/Volumes/workspace/sdp_course/landing/ecommerce_dq/orders_us` |
# MAGIC | | `products_source` = `/Volumes/workspace/sdp_course/landing/ecommerce_dq/products` |
# MAGIC | | `rules_table` = `workspace.sdp_course.dq_rules` |
# MAGIC | Event log | Publish to `workspace.sdp_1_4.event_log` |
# MAGIC
# MAGIC Before the first update, in `00-land-course-data`: land e-commerce **batch 0** into `ecommerce_dq`, and run the
# MAGIC **rules table** cell.

# COMMAND ----------

# MAGIC %run ./_event_log_helpers

# COMMAND ----------

dbutils.widgets.text("schema", "workspace.sdp_1_4")
dbutils.widgets.text("event_log", "workspace.sdp_1_4.event_log")
SCHEMA, EVENT_LOG = dbutils.widgets.get("schema"), dbutils.widgets.get("event_log")
RULES = "workspace.sdp_course.dq_rules"
spark.sql(f"USE {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Rules as data
# MAGIC The pipeline loads the rules **by tag** when the update starts (`get_rules("orders")`), so a governed table, not the
# MAGIC code, decides what "valid" means. Every rule is **null-safe** (`quantity IS NOT NULL AND quantity > 0`): an
# MAGIC expectation that evaluates to NULL is ambiguous.

# COMMAND ----------

display(spark.table(RULES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - After the first update: the zero-data-loss quarantine pattern
# MAGIC `orders_dq_flagged` keeps **every** row with `is_quarantined` (partitioned by it), warns on each rule, and splits into
# MAGIC `orders_valid` (temporary view, not published) and `orders_quarantine` (with `failed_rules`).
# MAGIC `orders_silver` then drops unknown products (referential integrity through a stream-static join).

# COMMAND ----------

display(expectation_metrics(EVENT_LOG))
display(spark.sql("SELECT order_id, customer_id, quantity, price, failed_rules FROM orders_quarantine"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Table-level checks
# MAGIC Row rules can't prove "no record was lost". The `dq_reconciliation` MV compares row counts across layers, and its
# MAGIC expectations (`bronze_fully_flagged`, `flagged_split_complete`) turn that into a monitored metric.
# MAGIC `daily_category_sales` uses `expect_or_fail` on an aggregate (`revenue >= 0`).

# COMMAND ----------

display(spark.table("dq_reconciliation"))

# COMMAND ----------

r = spark.table("dq_reconciliation").first()
assert r.bronze_rows == r.flagged_rows == r.valid_rows + r.quarantined_rows, r
print("OK - bronze = valid + quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Schema drift arrives
# MAGIC In `00-land-course-data`: target `ecommerce_dq`, batch **1** → land, then **Start**.
# MAGIC Batch 1 adds an unexpected `coupon` field and a `quantity` of `"two"`. With `schemaHints` + `schemaEvolutionMode =
# MAGIC rescue`, both land in `_rescued_data` instead of failing the stream or being lost. The **warn** expectation
# MAGIC `no_rescued_data` makes the drift visible in the *Data quality* tab.

# COMMAND ----------

display(spark.sql("SELECT orderId, quantity, _rescued_data FROM orders_bronze WHERE _rescued_data IS NOT NULL"))
display(expectation_metrics(EVENT_LOG).filter("expectation IN ('no_rescued_data', 'positive_quantity')"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Discuss:** the other `schemaEvolutionMode` values. `addNewColumns` (default when no schema is given) fails the
# MAGIC stream once, then restarts with the new column; `failOnNewColumns` fails until you change the schema; `none` ignores
# MAGIC new columns and doesn't rescue them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Change a rule without touching code
# MAGIC Add a rule, land batch **2** into `ecommerce_dq`, and **Start**. The new rule shows up in the metrics.

# COMMAND ----------

spark.sql(f"INSERT INTO {RULES} VALUES ('plausible_price', 'price IS NOT NULL AND price <= 2000', 'orders')")
display(spark.table(RULES))

# COMMAND ----------

# Run after the update
display(expectation_metrics(EVENT_LOG).filter("dataset = 'orders_dq_flagged'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Data-quality trend from the event log
# MAGIC The event log keeps every update, so DQ is a time series you can chart (click **+** → *Visualization* → line chart
# MAGIC of `failed` by `update_time`, grouped by `expectation`), or put on a dashboard / SQL alert.

# COMMAND ----------

display(expectation_metrics(EVENT_LOG, all_updates=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Choosing an expectation action
# MAGIC | Action | Row | Update | Use for |
# MAGIC |---|---|---|---|
# MAGIC | `expect` (warn) | kept | continues | monitoring, drift, soft rules |
# MAGIC | `expect_or_drop` | removed | continues | rows that must not reach consumers (pair with a quarantine) |
# MAGIC | `expect_or_fail` | — | **fails**, batch not committed | invariants whose violation means a broken upstream |
# MAGIC
# MAGIC **Try:** in the `dq_rules` table, change a rule so it's NOT null-safe (e.g. `price > 0` without `IS NOT NULL`) and
# MAGIC rerun with full refresh. Rows with a NULL price are then still flagged as quarantined, because the pipeline code
# MAGIC wraps the combined rule in `coalesce(..., true)`: a fail-safe default you should copy into your own pipelines.
# MAGIC Restore the rule afterwards.
