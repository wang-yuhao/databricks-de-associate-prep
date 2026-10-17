# Databricks notebook source
# MAGIC %md
# MAGIC # 3.6 Demo - User-Defined Functions (Databricks Free Edition)
# MAGIC **Lessons:** Serialization · User-Defined Functions
# MAGIC
# MAGIC Built-in functions run inside the JVM (and in Photon on serverless). A Python UDF ships every row to a Python worker
# MAGIC and back: serialization + per-row calls. Arrow (vectorized) UDFs move columnar batches, which is much cheaper.
# MAGIC SQL UDFs are inlined into the plan like built-ins.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf, udf

spark.range(5_000_000).select(
    F.col("id").alias("customer_id"),
    F.concat(F.lit("  User."), F.col("id"), F.lit("@Example.COM ")).alias("email"),
    (F.abs(F.xxhash64("id")) % 100_000).alias("amount_cents"),
).write.mode("overwrite").saveAsTable("payments_36")
df = spark.table("payments_36")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. The task: normalize the email and convert cents to an amount with 19% VAT
# MAGIC Five implementations of the same logic.

# COMMAND ----------

# 1. Built-ins
builtin = df.select(F.lower(F.trim("email")).alias("email"),
                    F.round(F.col("amount_cents") / 100 * 1.19, 2).alias("gross"))


# 2. Python UDF (row at a time, pickled)
@udf("string")
def norm_email_py(e):
    return e.strip().lower() if e is not None else None


@udf("double")
def gross_py(c):
    return round(c / 100 * 1.19, 2) if c is not None else None


python_udf = df.select(norm_email_py("email").alias("email"), gross_py("amount_cents").alias("gross"))


# 3. Arrow-optimized Python UDF (same code, Arrow batches instead of pickle)
@udf("string", useArrow=True)
def norm_email_arrow(e):
    return e.strip().lower() if e is not None else None


@udf("double", useArrow=True)
def gross_arrow(c):
    return round(c / 100 * 1.19, 2) if c is not None else None


arrow_udf = df.select(norm_email_arrow("email").alias("email"), gross_arrow("amount_cents").alias("gross"))


# 4. pandas UDF (vectorized: one call per batch of rows)
@pandas_udf("string")
def norm_email_pd(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower()


@pandas_udf("double")
def gross_pd(s: pd.Series) -> pd.Series:
    return (s / 100 * 1.19).round(2)


pandas_udf_df = df.select(norm_email_pd("email").alias("email"), gross_pd("amount_cents").alias("gross"))

# 5. SQL UDF in Unity Catalog (governed, reusable from SQL and Python, inlined by the optimizer)
spark.sql(f"CREATE OR REPLACE FUNCTION {FQ}.norm_email(e STRING) RETURNS STRING RETURN lower(trim(e))")
spark.sql(f"CREATE OR REPLACE FUNCTION {FQ}.gross_amount(c BIGINT) RETURNS DOUBLE RETURN round(c / 100 * 1.19, 2)")
sql_udf = spark.sql(f"SELECT {FQ}.norm_email(email) AS email, {FQ}.gross_amount(amount_cents) AS gross FROM payments_36")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Compare

# COMMAND ----------

results = {name: timed(d, name) for name, d in [("built-in functions", builtin), ("SQL UDF", sql_udf),
                                                 ("pandas UDF", pandas_udf_df), ("Arrow Python UDF", arrow_udf),
                                                 ("Python UDF", python_udf)]}

# COMMAND ----------

assert builtin.exceptAll(pandas_udf_df).limit(1).count() == 0, "same result"
display(spark.createDataFrame(list(results.items()), "implementation STRING, seconds DOUBLE").orderBy("seconds"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Why: the physical plans
# MAGIC - Python UDF → `BatchEvalPython` (rows pickled to a Python worker)
# MAGIC - Arrow / pandas UDF → `ArrowEvalPython` (columnar batches)
# MAGIC - Built-ins and SQL UDFs → just `Project` with expressions (the SQL UDF body is inlined)
# MAGIC
# MAGIC In the query profile, the Python operators show up as separate nodes with their own time.

# COMMAND ----------

for name, d in [("python", python_udf), ("pandas", pandas_udf_df), ("sql udf", sql_udf), ("builtin", builtin)]:
    print(f"===== {name}")
    d.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. UDFs block optimizations
# MAGIC A filter on a UDF result is a black box to the optimizer: it can't be pushed into the scan and can't use file
# MAGIC statistics for skipping. The same filter with built-ins becomes a `PushedFilters` / data-skipping predicate.

# COMMAND ----------

spark.table("payments_36").filter(F.col("customer_id") == 42).explain()                 # pushed down

@udf("bigint")
def same_id(x):
    return x

spark.table("payments_36").filter(same_id("customer_id") == 42).explain()               # not pushed: full scan

# COMMAND ----------

timed(spark.table("payments_36").filter(F.col("customer_id") == 42), "built-in filter")
timed(spark.table("payments_36").filter(same_id("customer_id") == 42), "UDF filter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Guidelines
# MAGIC 1. Use built-in functions (there are hundreds: `regexp_*`, `transform`, `aggregate`, JSON, dates, `try_*`).
# MAGIC 2. Need reuse / governance? **SQL UDF** in Unity Catalog.
# MAGIC 3. Need Python libraries? **pandas UDF** (or `useArrow=True`), never a plain row-at-a-time UDF on big data.
# MAGIC 4. Keep filters and join keys out of UDFs so pushdown and data skipping still work.
