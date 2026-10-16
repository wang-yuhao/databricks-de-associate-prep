# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 — Apache Spark Basics
# MAGIC ### ☁️ Azure Databricks Edition
# MAGIC
# MAGIC **Catalog / Schema:** `training.prep`
# MAGIC **Volume:** `/Volumes/training/prep/landing/`
# MAGIC
# MAGIC Run cells top-to-bottom. Every table write uses Unity Catalog managed tables.

# COMMAND ----------
# Cell 0 — Set default namespace for this notebook session
spark.sql("USE CATALOG training")
spark.sql("USE SCHEMA prep")
print("Catalog:", spark.sql("SELECT current_catalog()").collect()[0][0])
print("Schema: ", spark.sql("SELECT current_schema()").collect()[0][0])
print("User:   ", spark.sql("SELECT current_user()").collect()[0][0])
print("Spark:  ", spark.version)

# COMMAND ----------
# MAGIC %md ## 1. Create Sample Data

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, lit, when, coalesce, to_date
import datetime

schema = StructType([
    StructField("order_id",   IntegerType(), nullable=False),
    StructField("customer",   StringType()),
    StructField("product",    StringType()),
    StructField("region",     StringType()),
    StructField("amount",     DoubleType()),
    StructField("order_date", StringType()),
    StructField("status",     StringType()),
])

data = [
    (1,  "Alice",   "Widget A",  "EU",   150.00, "2024-01-10", "completed"),
    (2,  "Bob",     "Widget B",  "US",   200.00, "2024-01-11", "completed"),
    (3,  "Carol",   "Gadget X",  "EU",   350.00, "2024-01-12", "pending"),
    (4,  "Dave",    "Widget A",  "APAC", 100.00, "2024-01-13", "completed"),
    (5,  "Eve",     "Device Z",  "US",   500.00, "2024-01-14", "cancelled"),
    (6,  "Frank",   "Gadget X",  "EU",   280.00, "2024-01-15", "completed"),
    (7,  "Grace",   "Widget B",  "APAC", 180.00, "2024-01-15", "completed"),
    (8,  "Hank",    "Device Z",  "US",   450.00, "2024-01-16", "completed"),
    (9,  "Iris",    "Widget A",  "EU",   None,   "2024-01-17", "pending"),   # null amount
    (10, "Jack",    None,        "US",   120.00, "2024-01-17", "completed"), # null product
]

df = spark.createDataFrame(data, schema).withColumn("order_date", to_date(col("order_date")))
print("Rows:", df.count())
df.printSchema()
df.show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 2. Transformations vs Actions

# COMMAND ----------
# Transformations are LAZY — they build an execution plan but do not run
filtered   = df.filter(col("amount") > 200)              # lazy
with_vat   = df.withColumn("vat", col("amount") * 0.20) # lazy
selected   = df.select("order_id", "customer", "amount") # lazy
renamed    = df.withColumnRenamed("customer", "buyer")   # lazy

# Actions TRIGGER execution
print("count():   ", df.count())                # action
print("first():   ", df.first()["customer"])    # action
print("take(3):   ", [r["order_id"] for r in df.take(3)])  # action
filtered.show()                                 # action — also shows result

# COMMAND ----------
# MAGIC %md ## 3. Filter, Select, WithColumn, Drop

# COMMAND ----------
# Filter with multiple conditions
df_eu_completed = df.filter((col("region") == "EU") & (col("status") == "completed"))
print("EU completed:", df_eu_completed.count())

# withColumn: add derived column
df_with_discount = df.withColumn(
    "discounted_amount",
    when(col("region") == "EU", col("amount") * 0.9)
    .when(col("region") == "US", col("amount") * 0.95)
    .otherwise(col("amount"))
)
df_with_discount.select("customer", "region", "amount", "discounted_amount").show()

# coalesce: fill nulls
df_no_nulls = df.withColumn("amount",  coalesce(col("amount"),  lit(0.0))) \
                .withColumn("product", coalesce(col("product"), lit("Unknown")))
df_no_nulls.filter(col("order_id").isin(9, 10)).show()

# drop: remove column
df_no_status = df.drop("status")
print("Columns after drop:", df_no_status.columns)

# COMMAND ----------
# MAGIC %md ## 4. GroupBy + Aggregations

# COMMAND ----------
from pyspark.sql.functions import count, sum as spark_sum, avg, min as spark_min, max as spark_max

# Basic aggregation
df_agg = (df.filter(col("amount").isNotNull())
    .groupBy("region")
    .agg(
        count("*").alias("total_orders"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        spark_max("amount").alias("max_order"),
    )
    .orderBy(col("total_revenue").desc())
)
df_agg.show()

# Multi-key groupBy
df_region_product = (df.filter(col("amount").isNotNull())
    .groupBy("region", "product")
    .agg(spark_sum("amount").alias("revenue"))
    .orderBy("region", col("revenue").desc())
)
df_region_product.show()

# COMMAND ----------
# MAGIC %md ## 5. Joins

# COMMAND ----------
# Customer dimension table
customer_data = [
    ("Alice", "alice@example.com",  "premium"),
    ("Bob",   "bob@example.com",    "standard"),
    ("Carol", "carol@example.com",  "premium"),
    ("Dave",  "dave@example.com",   "standard"),
    ("Eve",   "eve@example.com",    "premium"),
]
df_customers = spark.createDataFrame(customer_data, ["customer", "email", "tier"])

# Inner join
df_inner = df.join(df_customers, on="customer", how="inner")
print("Inner join rows:", df_inner.count())  # 9 (Jack has no customer record)
df_inner.select("order_id", "customer", "tier", "amount").show()

# Left join (keeps all orders including Jack)
df_left = df.join(df_customers, on="customer", how="left")
print("Left join rows:", df_left.count())    # 10
df_left.filter(col("tier").isNull()).show()  # Jack's order with null tier

# Anti join (orders with no customer record)
df_anti = df.join(df_customers, on="customer", how="left_anti")
print("Anti join (no customer match):", df_anti.count())
df_anti.show()

# COMMAND ----------
# MAGIC %md ## 6. Window Functions

# COMMAND ----------
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, lag, lead, sum as wsum

# Rank by amount within each region
w_rank = Window.partitionBy("region").orderBy(col("amount").desc())

df_ranked = (df.filter(col("amount").isNotNull())
    .withColumn("rank",        rank().over(w_rank))
    .withColumn("dense_rank",  dense_rank().over(w_rank))
    .withColumn("row_number",  row_number().over(w_rank))
    .select("order_id", "customer", "region", "amount", "rank", "dense_rank", "row_number")
    .orderBy("region", "rank")
)
df_ranked.show()

# Running total within region ordered by date
w_running = Window.partitionBy("region").orderBy("order_date").rowsBetween(Window.unboundedPreceding, 0)
df_running = (df.filter(col("amount").isNotNull())
    .withColumn("running_total", wsum("amount").over(w_running))
    .select("region", "order_date", "amount", "running_total")
    .orderBy("region", "order_date")
)
df_running.show()

# lag/lead: compare to previous/next row
w_lag = Window.partitionBy("region").orderBy("order_date")
df_lag = (df.filter(col("amount").isNotNull())
    .withColumn("prev_amount", lag("amount", 1).over(w_lag))
    .withColumn("next_amount", lead("amount", 1).over(w_lag))
    .withColumn("delta",       col("amount") - col("prev_amount"))
    .select("region", "order_date", "amount", "prev_amount", "next_amount", "delta")
    .orderBy("region", "order_date")
)
df_lag.show()

# COMMAND ----------
# MAGIC %md ## 7. Write to Unity Catalog (Managed Delta Tables)

# COMMAND ----------
# Write as managed Delta table — UC controls storage automatically
df.write.mode("overwrite").saveAsTable("training.prep.day2_orders")

# Verify via SQL
display(spark.sql("""
    SELECT region, COUNT(*) AS orders, ROUND(SUM(amount),2) AS revenue
    FROM training.prep.day2_orders
    WHERE amount IS NOT NULL
    GROUP BY region
    ORDER BY revenue DESC
"""))

# Write partitioned (by region)
(df.filter(col("amount").isNotNull())
    .write
    .mode("overwrite")
    .partitionBy("region")
    .saveAsTable("training.prep.day2_orders_partitioned")
)
print("Partitioned table written")
spark.sql("DESCRIBE DETAIL training.prep.day2_orders_partitioned").select("numFiles", "partitionColumns").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 8. Write JSON Files to Volume (for file-based exercises)

# COMMAND ----------
import json

# Write sample JSON files to the Unity Catalog Volume (landing zone)
for i, row in enumerate(data[:5]):
    record = {
        "order_id":   row[0],
        "customer":   row[1],
        "product":    row[2],
        "region":     row[3],
        "amount":     row[4],
        "order_date": row[5],
        "status":     row[6],
    }
    dbutils.fs.put(
        f"/Volumes/training/prep/landing/day2_json/order_{i:04d}.json",
        json.dumps(record),
        overwrite=True
    )

# Read back from Volume
df_from_vol = (spark.read
    .option("inferSchema", "true")
    .json("/Volumes/training/prep/landing/day2_json/")
)
print("Rows from Volume:", df_from_vol.count())
df_from_vol.show()

# COMMAND ----------
# MAGIC %md ## 9. Spark SQL vs DataFrame API

# COMMAND ----------
# Both produce the same result — Spark SQL is easier for analysts
# DataFrame API is better for programmatic transformations

# DataFrame API
df_api_result = (spark.table("training.prep.day2_orders")
    .filter(col("status") == "completed")
    .groupBy("region")
    .agg(spark_sum("amount").alias("completed_revenue"))
    .orderBy(col("completed_revenue").desc())
)
df_api_result.show()

# Equivalent Spark SQL
df_sql_result = spark.sql("""
    SELECT region,
           ROUND(SUM(amount), 2) AS completed_revenue
    FROM training.prep.day2_orders
    WHERE status = 'completed'
      AND amount IS NOT NULL
    GROUP BY region
    ORDER BY completed_revenue DESC
""")
df_sql_result.show()

# COMMAND ----------
# MAGIC %md ## 10. Higher-Order Functions (SQL)

# COMMAND ----------
spark.sql("""
SELECT
    region,
    COLLECT_LIST(product) AS products,
    TRANSFORM(COLLECT_LIST(amount), x -> ROUND(x * 1.2, 2)) AS inflated_amounts,
    FILTER(COLLECT_LIST(status), s -> s = 'completed')      AS completed_statuses,
    EXISTS(COLLECT_LIST(amount), a -> a > 400)              AS has_big_order
FROM training.prep.day2_orders
WHERE amount IS NOT NULL
GROUP BY region
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md ## 11. Cleanup (Optional)

# COMMAND ----------
# Run only if you want to clean up after this notebook
# spark.sql("DROP TABLE IF EXISTS training.prep.day2_orders")
# spark.sql("DROP TABLE IF EXISTS training.prep.day2_orders_partitioned")
# dbutils.fs.rm("/Volumes/training/prep/landing/day2_json/", recurse=True)
print("Cleanup skipped — tables preserved for Day 3 reference")
