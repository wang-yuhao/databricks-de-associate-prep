# Databricks notebook source
# MAGIC %md
# MAGIC # Hello from a bundle
# MAGIC This notebook is deployed and scheduled by `bundles/01_simple_dab`.

# COMMAND ----------

dbutils.widgets.text("greeting", "Hello")
greeting = dbutils.widgets.get("greeting")
print(f"{greeting}! Running as {spark.sql('SELECT current_user()').first()[0]}")

# COMMAND ----------

display(spark.range(5).selectExpr("id", "id * id AS squared"))
