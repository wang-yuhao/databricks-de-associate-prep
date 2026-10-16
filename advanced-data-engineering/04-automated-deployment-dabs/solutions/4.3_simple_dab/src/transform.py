# Databricks notebook source
dbutils.widgets.text("run_date", "")
print(f"transforming data for {dbutils.widgets.get('run_date') or 'today'}")
display(spark.range(10).selectExpr("id", "id * 2 AS doubled"))
