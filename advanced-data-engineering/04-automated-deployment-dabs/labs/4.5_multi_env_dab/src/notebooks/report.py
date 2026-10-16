# Databricks notebook source
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "orders_dev")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalog}.{schema}.daily_revenue ORDER BY order_date DESC"))
