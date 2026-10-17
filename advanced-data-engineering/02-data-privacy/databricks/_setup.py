# Databricks notebook source
# MAGIC %md
# MAGIC # Course 2 setup (included with `%run ./_setup`)
# MAGIC Creates `workspace.privacy_course` with a `landing` volume and imports `adelab.datagen` from the Git folder.

# COMMAND ----------

import os
import sys

_root = os.getcwd()   # the calling notebook's folder; walk up to the folder that contains adelab/
while not os.path.isdir(os.path.join(_root, "adelab")) and os.path.dirname(_root) != _root:
    _root = os.path.dirname(_root)
sys.path.insert(0, _root)
from adelab import datagen  # noqa: E402

CATALOG, SCHEMA = "workspace", "privacy_course"
FQ = f"{CATALOG}.{SCHEMA}"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINTS = f"{LANDING}/_checkpoints"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.landing")
spark.sql(f"USE {FQ}")
ME = spark.sql("SELECT current_user()").first()[0]
print(f"schema {FQ} | landing {LANDING} | you are {ME}")
