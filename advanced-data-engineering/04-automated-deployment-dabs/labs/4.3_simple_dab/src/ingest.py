# Databricks notebook source
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date") or str(spark.sql("SELECT current_date()").first()[0])
print(f"ingesting data for {run_date}")
spark.range(100).selectExpr("id", f"date'{run_date}' AS run_date").createOrReplaceTempView("ingested")
