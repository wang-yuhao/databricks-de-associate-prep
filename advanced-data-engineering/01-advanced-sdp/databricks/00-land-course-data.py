# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Land Course 1 data in a Unity Catalog volume
# MAGIC Run from the Git folder of this repository on **serverless** compute. It reuses `adelab/datagen.py`, the same
# MAGIC deterministic generators as the local labs, and writes the files into `/Volumes/workspace/sdp_course/landing`.
# MAGIC
# MAGIC Run **section 0 once**, then run only the cell a lesson of `FREE_EDITION_GUIDE.md` tells you to. Each cell lands
# MAGIC **one more file**, which is how you simulate new data arriving between pipeline updates.
# MAGIC
# MAGIC | Schema | Used by pipeline |
# MAGIC |---|---|
# MAGIC | `sdp_course` | landing volume + `dq_rules` table (shared) |
# MAGIC | `sdp_1_1` | `multi_flow` |
# MAGIC | `sdp_1_2` | `multiplex` |
# MAGIC | `sdp_1_3` | `auto_cdc` |
# MAGIC | `sdp_1_4` | `advanced_dq` |
# MAGIC | `sdp_1_5` | `lab_ecommerce` |
# MAGIC
# MAGIC One schema per pipeline, because two pipelines can't publish tables with the same name into one schema.

# COMMAND ----------

# MAGIC %md ## 0. Setup (run once, and again after a reset)

# COMMAND ----------

import os
import sys

# The Git folder root is two levels above this notebook: advanced-data-engineering/01-advanced-sdp/databricks
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from adelab import datagen  # noqa: E402

CATALOG, SCHEMA, VOLUME = "workspace", "sdp_course", "landing"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PIPELINE_SCHEMAS = ["sdp_1_1", "sdp_1_2", "sdp_1_3", "sdp_1_4", "sdp_1_5"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
for s in PIPELINE_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")


def ls(folder):
    """Files landed so far in one landing folder."""
    path = f"{LANDING}/{folder}"
    return sorted(os.listdir(path)) if os.path.exists(path) else []


print("Landing root:", LANDING)

# COMMAND ----------

# MAGIC %md ## Demo 1.1 - multi-flow events

# COMMAND ----------

# 1.1 step 1: first file for each flow. Rerun = one more file per flow.
print(datagen.land_events(f"{LANDING}/source_flow_a", "a"))
print(datagen.land_events(f"{LANDING}/source_flow_b", "b"))

# COMMAND ----------

# 1.1 incremental step: a new file in flow B only
print(datagen.land_events(f"{LANDING}/source_flow_b", "b"))

# COMMAND ----------

# 1.1 add a third source (flow C) without a full refresh
print(datagen.land_events(f"{LANDING}/source_flow_c", "c"))

# COMMAND ----------

# 1.1 expect_or_fail: a file in flow A that contains one row with event_id = NULL
print(datagen.land_events(f"{LANDING}/source_flow_a", "a", null_event_id=True))

# COMMAND ----------

# MAGIC %md ## Demo 1.2 - multiplex feed (rerun = the next 6 hours of the stream)

# COMMAND ----------

print(datagen.land_multiplex(f"{LANDING}/multiplex"))
print(ls("multiplex"))

# COMMAND ----------

# MAGIC %md ## Demo 1.3 - CDC batches
# MAGIC Land batch 0, run the pipeline, land batch 1, run, land batch 2, run. Pick the batch in the widget at the top.

# COMMAND ----------

dbutils.widgets.dropdown("cdc_batch", "0", ["0", "1", "2"])
print(datagen.land_customer_cdc(f"{LANDING}/customers_cdc", int(dbutils.widgets.get("cdc_batch"))))
print(ls("customers_cdc"))

# COMMAND ----------

# MAGIC %md ## Demo 1.4 and Lab 1.5 - e-commerce sources
# MAGIC Batch 0 first, then 1 and 2 for incremental runs. Batch 1+ brings schema drift (`coupon` field, `quantity = "two"`).
# MAGIC Demo 1.4 lands into `ecommerce_dq/`, Lab 1.5 into `ecommerce/`, so the lab's row-count checks are not affected
# MAGIC by the demo. Pick both values in the widgets at the top.

# COMMAND ----------

dbutils.widgets.dropdown("ecommerce_target", "ecommerce_dq (demo 1.4)", ["ecommerce_dq (demo 1.4)", "ecommerce (lab 1.5)"])
dbutils.widgets.dropdown("ecommerce_batch", "0", ["0", "1", "2"])
root = dbutils.widgets.get("ecommerce_target").split(" ")[0]
for name, path in datagen.land_ecommerce(f"{LANDING}/{root}", int(dbutils.widgets.get("ecommerce_batch"))).items():
    print(f"{name:14s} {path}")

# COMMAND ----------

# MAGIC %md ## Demo 1.4 - the rules table (rules as data)
# MAGIC Run before the first `advanced_dq` update. The pipeline reads it through the `rules_table` setting.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.dq_rules (name STRING, constraint STRING, tag STRING)
COMMENT 'Data-quality rules loaded by tag in the advanced_dq pipeline.'
""")
spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA}.dq_rules VALUES
  ('valid_order_id',    'order_id IS NOT NULL',                  'orders'),
  ('valid_customer',    'customer_id IS NOT NULL',               'orders'),
  ('positive_quantity', 'quantity IS NOT NULL AND quantity > 0', 'orders'),
  ('valid_price',       'price IS NOT NULL AND price > 0',       'orders')
""")
display(spark.table(f"{CATALOG}.{SCHEMA}.dq_rules"))

# COMMAND ----------

# MAGIC %md ## Inspect / reset

# COMMAND ----------

for folder in ["source_flow_a", "source_flow_b", "source_flow_c", "multiplex", "customers_cdc",
               "ecommerce_dq/orders_us", "ecommerce_dq/products",
               "ecommerce/orders_eu", "ecommerce/orders_us", "ecommerce/customers_cdc", "ecommerce/products"]:
    print(f"{folder:26s} {ls(folder)}")

# COMMAND ----------

# Reset one lesson's landing data. Do a FULL REFRESH of the matching pipeline afterwards,
# otherwise its checkpoints still remember the deleted files.
dbutils.widgets.dropdown("reset_folder", "-", ["-", "source_flow_a", "source_flow_b", "source_flow_c", "multiplex",
                                               "customers_cdc", "ecommerce_dq", "ecommerce", "exports"])
folder = dbutils.widgets.get("reset_folder")
if folder != "-":
    datagen.clear(f"{LANDING}/{folder}")
    print("cleared", folder)

# COMMAND ----------

# MAGIC %md ## Course clean-up (end of Course 1)
# MAGIC Delete the pipelines in *Jobs & Pipelines* first, then uncomment and run.

# COMMAND ----------

# for s in PIPELINE_SCHEMAS + [SCHEMA]:
#     spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{s} CASCADE")
