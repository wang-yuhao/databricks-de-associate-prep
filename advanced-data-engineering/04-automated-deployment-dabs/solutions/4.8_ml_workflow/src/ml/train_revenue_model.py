"""Train a revenue model: the ML task of the orders workflow (lab 4.8).

Reads the `daily_summary` table built by the engineering task, fits a small regression model, logs it
with MLflow and registers it in Unity Catalog. A plain Python script (`spark_python_task`), because on
serverless jobs only script, wheel and dbt tasks accept a job `environment_key` with extra libraries;
notebook tasks install them with %pip instead.
"""
import argparse

import mlflow
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.linear_model import LinearRegression

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
args = parser.parse_args()

spark = SparkSession.builder.getOrCreate()
pdf = spark.table(f"{args.catalog}.{args.schema}.daily_summary").toPandas()
pdf["day_index"] = (pd.to_datetime(pdf["order_date"]) - pd.to_datetime(pdf["order_date"]).min()).dt.days
X, y = pdf[["day_index", "orders"]], pdf["revenue"]

model_name = f"{args.catalog}.{args.schema}.revenue_model"
mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run(run_name="revenue_model"):
    model = LinearRegression().fit(X, y)
    mlflow.log_metric("r2_train", model.score(X, y))
    mlflow.sklearn.log_model(model, name="model", input_example=X.head(1), registered_model_name=model_name)
print("registered", model_name)
