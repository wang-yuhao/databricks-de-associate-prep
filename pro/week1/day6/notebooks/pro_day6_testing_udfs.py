# Databricks notebook source
# MAGIC %md
# MAGIC # Day 6 — Python Project Structure, UDFs & Testing
# MAGIC **Exam weight: ~22%**

# COMMAND ----------
# MAGIC %md ## Setup

# COMMAND ----------
spark.sql("CREATE CATALOG IF NOT EXISTS training")
spark.sql("CREATE SCHEMA IF NOT EXISTS training.prep")

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField, LongType, DoubleType
import pandas as pd

print("Imports done")

# COMMAND ----------
# MAGIC %md ## 6.1 Sample DataFrame

# COMMAND ----------
orders_df = spark.createDataFrame(
    [
        (1,  "Alice",  99.99),
        (2,  "Bob",   149.00),
        (3,  None,     50.00),  # null customer
        (4,  "Dave",  -10.00),  # negative amount
        (5,  "Eve",   299.99),
    ],
    "order_id BIGINT, customer STRING, amount DOUBLE"
)
orders_df.show()

# COMMAND ----------
# MAGIC %md ## 6.2 Regular Python UDF

# COMMAND ----------
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def classify_amount(amount):
    if amount is None:
        return "unknown"
    elif amount < 0:
        return "invalid"
    elif amount < 50:
        return "small"
    elif amount < 200:
        return "medium"
    else:
        return "large"

# Register for SQL
spark.udf.register("classify_amount", classify_amount)

# DataFrame API
orders_df.withColumn("size_class", classify_amount("amount")).show()

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT order_id, amount, classify_amount(amount) AS size_class
# MAGIC FROM training.prep.orders_raw_view

# Note: create the view first for the SQL cell to work

# COMMAND ----------
orders_df.createOrReplaceTempView("orders_view")
spark.sql("SELECT order_id, amount, classify_amount(amount) AS size_class FROM orders_view").show()

# COMMAND ----------
# MAGIC %md ## 6.3 Pandas UDF (Vectorized)

# COMMAND ----------
from pyspark.sql.functions import pandas_udf

# Scalar Pandas UDF: pd.Series in → pd.Series out
@pandas_udf(FloatType())
def normalize_amount(amounts: pd.Series) -> pd.Series:
    mean = amounts.mean()
    std  = amounts.std()
    if std == 0:
        return amounts * 0.0
    return ((amounts - mean) / std).astype(float)

result = orders_df.withColumn("normalized_amount", normalize_amount("amount"))
result.show()

# COMMAND ----------
# MAGIC %md ## 6.4 DataFrame.transform() for clean chaining

# COMMAND ----------
def add_total_with_tax(df: DataFrame, tax_rate: float = 0.2) -> DataFrame:
    return df.withColumn("total_with_tax", F.col("amount") * (1 + tax_rate))

def filter_valid_orders(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("amount") > 0) &
        (F.col("customer").isNotNull()) &
        (F.col("order_id").isNotNull())
    )

def add_size_class(df: DataFrame) -> DataFrame:
    return df.withColumn("size_class", classify_amount("amount"))

# Chain with .transform()
result = (
    orders_df
    .transform(add_total_with_tax, tax_rate=0.19)
    .transform(filter_valid_orders)
    .transform(add_size_class)
)
result.show()

# COMMAND ----------
# MAGIC %md ## 6.5 Unit Testing with pyspark.testing

# COMMAND ----------
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# --- Test: add_total_with_tax ---
input_df = spark.createDataFrame(
    [(1, "Alice", 100.0), (2, "Bob", 200.0)],
    "order_id BIGINT, customer STRING, amount DOUBLE"
)
result_df  = add_total_with_tax(input_df, tax_rate=0.2)
expected_df = spark.createDataFrame(
    [(1, "Alice", 100.0, 120.0), (2, "Bob", 200.0, 240.0)],
    "order_id BIGINT, customer STRING, amount DOUBLE, total_with_tax DOUBLE"
)
assertDataFrameEqual(result_df, expected_df)
print("✅ add_total_with_tax: PASSED")

# --- Test: filter_valid_orders removes invalid rows ---
test_df = spark.createDataFrame(
    [(1, "Alice", 100.0), (None, "Bob", 50.0), (3, None, 75.0), (4, "Dave", -10.0)],
    "order_id BIGINT, customer STRING, amount DOUBLE"
)
filtered = filter_valid_orders(test_df)
assert filtered.count() == 1, f"Expected 1 row, got {filtered.count()}"
assert filtered.collect()[0]["order_id"] == 1
print("✅ filter_valid_orders: PASSED")

# --- Test: schema preserved after filter ---
assertSchemaEqual(filtered.schema, test_df.schema)
print("✅ Schema preserved after filter: PASSED")

# COMMAND ----------
# MAGIC %md ## 6.6 assertDataFrameEqual Options

# COMMAND ----------
# checkRowOrder=False (default): row order does not matter
df1 = spark.createDataFrame([(1, "a"), (2, "b")], "id INT, val STRING")
df2 = spark.createDataFrame([(2, "b"), (1, "a")], "id INT, val STRING")
assertDataFrameEqual(df1, df2, checkRowOrder=False)
print("✅ Order-insensitive comparison: PASSED")

# rtol for float tolerance
df3 = spark.createDataFrame([(1, 100.0)], "id INT, val DOUBLE")
df4 = spark.createDataFrame([(1, 100.000001)], "id INT, val DOUBLE")
assertDataFrameEqual(df3, df4, rtol=1e-4)
print("✅ Float tolerance comparison: PASSED")

# COMMAND ----------
# MAGIC %md ## 6.7 UDF Performance Summary
# MAGIC
# MAGIC | Type | Serialization | Speed | Use Case |
# MAGIC |------|--------------|-------|----------|
# MAGIC | Python UDF | Row-by-row (pickle) | Slowest | Fallback only |
# MAGIC | Pandas UDF (Scalar) | Columnar (Arrow) | Fast | Vectorizable ops |
# MAGIC | Pandas UDF (Grouped Map) | Group-level (Arrow) | Fast | Per-group transforms |
# MAGIC | Spark built-in | Native JVM | Fastest | Always prefer |

# COMMAND ----------
# MAGIC %md ## ✅ Key Takeaways
# MAGIC - Prefer **built-in Spark functions** over UDFs whenever possible
# MAGIC - **Pandas UDFs** use Apache Arrow (columnar) — much faster than row-by-row Python UDFs
# MAGIC - `spark.udf.register(name, func)` makes a UDF available in SQL
# MAGIC - `DataFrame.transform(func)` enables clean, testable function chaining
# MAGIC - `assertDataFrameEqual` checks rows + schema; use `checkRowOrder=False` and `rtol` for floats
# MAGIC - `assertSchemaEqual` checks only schema structure
# MAGIC - `%pip install` in a notebook restarts the Python interpreter
