# Databricks notebook source
# MAGIC %md
# MAGIC # 1.5 Lab - Building a Multi-Source E-commerce Pipeline with SDP (Databricks)
# MAGIC You complete the TODOs in `01-advanced-sdp/pipelines/lab_ecommerce/transformations/` (edit them directly in the Git
# MAGIC folder or in the pipeline editor), run the pipeline, and run the **CHECK** cells here.
# MAGIC Solution: `01-advanced-sdp/solutions/lab_ecommerce/transformations/`. Guide: `FREE_EDITION_GUIDE.md` → Lesson 1.5.
# MAGIC
# MAGIC | Pipeline setting | Value |
# MAGIC |---|---|
# MAGIC | Name | `lab_ecommerce` |
# MAGIC | Source code | add the files **one step at a time**: `01_bronze_orders.py`, then `02_dimensions.py`, then `03_silver_gold.py` |
# MAGIC | Default catalog / schema | `workspace` / `sdp_1_5` |
# MAGIC | Configuration | `orders_eu_source` = `/Volumes/workspace/sdp_course/landing/ecommerce/orders_eu` |
# MAGIC | | `orders_us_source` = `.../ecommerce/orders_us`, `products_source` = `.../ecommerce/products`, `customers_source` = `.../ecommerce/customers_cdc` |
# MAGIC | Event log | Publish to `workspace.sdp_1_5.event_log` |
# MAGIC
# MAGIC Why one file at a time: a file whose TODO still raises `NotImplementedError` fails graph initialization of the whole
# MAGIC pipeline. A file you haven't added yet is simply not part of the pipeline.
# MAGIC
# MAGIC Data: in `00-land-course-data`, land target **`ecommerce (lab 1.5)`**, batch **0**.

# COMMAND ----------

# MAGIC %run ./_event_log_helpers

# COMMAND ----------

dbutils.widgets.text("schema", "workspace.sdp_1_5")
dbutils.widgets.text("event_log", "workspace.sdp_1_5.event_log")
SCHEMA, EVENT_LOG = dbutils.widgets.get("schema"), dbutils.widgets.get("event_log")
spark.sql(f"USE {SCHEMA}")
ROOT = "/Volumes/workspace/sdp_course/landing/ecommerce"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the sources first
# MAGIC Note the differences: column names, string-typed EU columns, dirty values (`qty = 0`, empty `customer_id`,
# MAGIC `unit_price = "N/A"`, `quantity = -1`, unknown `sku = p-999`).

# COMMAND ----------

display(spark.read.option("header", True).csv(f"{ROOT}/orders_eu").limit(5))
display(spark.read.json(f"{ROOT}/orders_us").limit(5))
display(spark.read.json(f"{ROOT}/customers_cdc").limit(5))
display(spark.read.option("header", True).csv(f"{ROOT}/products").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Unified Bronze (multi-flow)
# MAGIC Complete TODO 1.1-1.3 in `01_bronze_orders.py`. Pipeline source = that file only. **Start**, then CHECK.

# COMMAND ----------

b = spark.table("orders_bronze")
expected = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts", "source_region", "source_file"]
assert b.columns == expected, f"columns should be {expected}, got {b.columns}"
assert dict(b.groupBy("source_region").count().collect()) == {"EU": 120, "US": 100}, "expected 120 EU + 100 US rows"
types = dict(b.dtypes)
assert types["customer_id"] == "bigint" and types["quantity"] == "int" and types["unit_price"] == "double" \
    and types["order_ts"] == "timestamp", f"cast the columns to the target schema, got {types}"
assert list(spark.sql("DESCRIBE DETAIL orders_bronze").first()["clusteringColumns"]) == ["source_region"], \
    "orders_bronze must be clustered by source_region"
print("Step 1 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Dimensions
# MAGIC Complete TODO 2.1-2.3 in `02_dimensions.py`, add the file to the pipeline source, **Start**, then CHECK.

# COMMAND ----------

assert spark.table("products").count() == 30, f"products: expected 30 rows, got {spark.table('products').count()}"
c = spark.table("customers")
assert {"__START_AT", "__END_AT"} <= set(c.columns), "customers must be SCD type 2 (stored_as_scd_type=2)"
assert not {"operation", "sequence_num"} & set(c.columns), "exclude operation and sequence_num (except_column_list)"
assert c.count() == 40 and c.filter("__END_AT IS NULL").count() == 40, \
    f"expected 40 customers, all with an open version; got {c.count()} rows"
print("Step 2 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Silver, quarantine, and Gold
# MAGIC Complete TODO 3.1-3.5 in `03_silver_gold.py`, add the file, **Start**, then CHECK.

# COMMAND ----------

bronze, silver, quarantine = (spark.table(t).count() for t in ("orders_bronze", "orders_silver", "orders_quarantine"))
assert bronze == silver + quarantine, f"no row may be lost: bronze={bronze} silver={silver} quarantine={quarantine}"
assert quarantine > 0 and "failed_rules" in spark.table("orders_quarantine").columns, \
    "orders_quarantine needs the rejected rows and a failed_rules column"
bad = spark.table("orders_silver").filter(
    "NOT (quantity > 0 AND unit_price > 0 AND customer_id IS NOT NULL AND order_ts IS NOT NULL) "
    "OR quantity IS NULL OR unit_price IS NULL").count()
assert bad == 0, f"{bad} invalid rows reached orders_silver: check the rules of TODO 3.1 (NULLs included)"
assert list(spark.sql("DESCRIBE DETAIL orders_silver").first()["clusteringColumns"]) == ["order_date", "source_region"], \
    "orders_silver must be clustered by (order_date, source_region)"
gold = spark.table("revenue_by_category_daily")
assert set(gold.columns) >= {"order_date", "category", "orders", "revenue"}, f"columns: {gold.columns}"
tiers = {r["tier"] for r in spark.table("revenue_by_customer_tier").collect()}
assert tiers <= {"bronze", "silver", "gold", "unknown"} and len(tiers) >= 3, f"tiers: {tiers}"
print("Step 3 passed")

# COMMAND ----------

display(expectation_metrics(EVENT_LOG))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Day 2: incremental data and history
# MAGIC Land target `ecommerce (lab 1.5)`, batch **1**, and **Start** the same pipeline (no full refresh).

# COMMAND ----------

counts = dict(spark.table("orders_bronze").groupBy("source_region").count().collect())
assert counts == {"EU": 240, "US": 200}, f"each file must be read exactly once: {counts}"
history = spark.sql("SELECT customer_id FROM customers GROUP BY customer_id HAVING count(*) > 1").count()
assert history > 0, "some customers should now have more than one version"
print(f"Step 4 passed - {history} customers have history")

# COMMAND ----------

display(spark.sql("""
SELECT customer_id, name, city, tier, __START_AT, __END_AT
FROM customers
WHERE customer_id IN (SELECT customer_id FROM customers GROUP BY customer_id HAVING count(*) > 1)
ORDER BY customer_id, __START_AT"""))
display(spark.table("revenue_by_customer_tier").orderBy("revenue", ascending=False))
display(expectation_metrics(EVENT_LOG, all_updates=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 (stretch) - Production touches
# MAGIC 1. Switch the pipeline from *Development* to *Production* mode in the pipeline settings (production: retries on
# MAGIC    failure, clusters shut down right after the update). Note the difference in the update details.
# MAGIC 2. Schedule it: pipeline → *Schedule* → add a trigger (then remove it again: Free Edition quota).
# MAGIC 3. Open the lineage: Catalog Explorer → `workspace.sdp_1_5.revenue_by_customer_tier` → *Lineage* → *See lineage graph*.
# MAGIC
# MAGIC ## Reflection questions
# MAGIC 1. Why must each source get its **own append flow** instead of one `unionByName` query? (Checkpoints, adding a third
# MAGIC    channel, backfilling one channel.)
# MAGIC 2. `revenue_by_customer_tier` joins orders to the **current** customer version. How would you attribute revenue to the
# MAGIC    tier a customer had **at order time**? (Join on `order_ts` between `__START_AT` and `__END_AT`, using a timestamp
# MAGIC    `sequence_by`.)
# MAGIC 3. The EU feed sends `unit_price = "N/A"`. Where is that row now, and how would you reprocess it after the source fixes
# MAGIC    the data?
