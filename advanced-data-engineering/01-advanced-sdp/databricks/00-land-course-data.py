# Databricks notebook source
# MAGIC %md
# MAGIC # Land Course 1 data in a Unity Catalog volume
# MAGIC Run from the Git folder of this repository. It reuses `adelab/datagen.py`, the same generators as the local labs.

# COMMAND ----------

import os
import sys

# The Git folder root is three levels above this notebook: advanced-data-engineering/01-advanced-sdp/databricks
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from adelab import datagen  # noqa: E402

CATALOG, SCHEMA, VOLUME = "workspace", "sdp_course", "landing"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

# MAGIC %md ## Demo 1.1: multi-flow events (rerun to land one more file per flow)

# COMMAND ----------

print(datagen.land_events(f"{LANDING}/source_flow_a", "a"))
print(datagen.land_events(f"{LANDING}/source_flow_b", "b"))

# COMMAND ----------

# MAGIC %md ## Demo 1.2: multiplex feed

# COMMAND ----------

print(datagen.land_multiplex(f"{LANDING}/multiplex"))

# COMMAND ----------

# MAGIC %md ## Demo 1.3: CDC batches (land 0, run the pipeline, land 1, run, land 2, run)

# COMMAND ----------

dbutils.widgets.dropdown("cdc_batch", "0", ["0", "1", "2"])
print(datagen.land_customer_cdc(f"{LANDING}/customers_cdc", int(dbutils.widgets.get("cdc_batch"))))

# COMMAND ----------

# MAGIC %md ## Demo 1.4 and Lab 1.5: e-commerce sources (batch 0, then 1, 2 for incremental runs)

# COMMAND ----------

dbutils.widgets.dropdown("ecommerce_batch", "0", ["0", "1", "2"])
print(datagen.land_ecommerce(f"{LANDING}/ecommerce", int(dbutils.widgets.get("ecommerce_batch"))))
