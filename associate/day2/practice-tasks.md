# Day 2 — Practice Tasks

## Setup
Make sure your Community Edition cluster is running. Create a new notebook called `day2-spark-practice`.

Import sample data:
```python
# Cell 1: Create sample DataFrames
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, when, avg, count, sum, broadcast

# Employees data
emp_data = [
    (1, "Alice", "Engineering", 95000, "2020-01-15"),
    (2, "Bob", "Marketing", 72000, "2019-03-01"),
    (3, "Charlie", "Engineering", 88000, "2021-06-01"),
    (4, "Diana", "HR", 65000, "2018-07-20"),
    (5, "Eve", "Marketing", 78000, "2022-01-10"),
    (6, "Frank", None, 92000, "2020-04-15"),  # null department
]
emp_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("department", StringType()),
    StructField("salary", IntegerType()),
    StructField("hire_date", StringType())
])
employees = spark.createDataFrame(emp_data, emp_schema)

# Departments data  
dept_data = [
    ("Engineering", "San Francisco"),
    ("Marketing", "New York"),
    ("HR", "Chicago")
]
departments = spark.createDataFrame(dept_data, ["dept_name", "city"])

print("DataFrames created successfully")
employees.show()
```

---

## Task 1: DataFrame Transformations (40 min)

```python
# Exercise 1a: Select specific columns and add a computed column
# Goal: Select id, name, salary and add a 'senior' flag (True if salary > 85000)
result_1a = employees \
    .select("id", "name", "salary") \
    .withColumn("senior", when(col("salary") > 85000, True).otherwise(False))
result_1a.show()

# Exercise 1b: Filter employees in Engineering with salary > 90000
result_1b = employees.filter(
    (col("department") == "Engineering") & (col("salary") > 90000)
)
result_1b.show()

# Exercise 1c: Handle nulls — fill missing department with 'Unknown'
result_1c = employees.na.fill({"department": "Unknown"})
result_1c.show()

# Exercise 1d: Count employees per department and get avg salary
result_1d = employees \
    .na.fill({"department": "Unknown"}) \
    .groupBy("department") \
    .agg(
        count("*").alias("headcount"),
        avg("salary").alias("avg_salary")
    ) \
    .orderBy(col("avg_salary").desc())
result_1d.show()
```

---

## Task 2: Joins (30 min)

```python
# Exercise 2a: Inner join employees with departments
result_2a = employees \
    .na.fill({"department": "Unknown"}) \
    .join(departments, employees.department == departments.dept_name, "inner")
result_2a.show()

# Exercise 2b: Left join to keep ALL employees (including those with no department match)
result_2b = employees.join(departments, employees.department == departments.dept_name, "left")
result_2b.show()
# Question: How many rows? Why does Frank appear with null city?

# Exercise 2c: Anti join — employees with no matching department
result_2c = employees.join(departments, employees.department == departments.dept_name, "anti")
result_2c.show()
# Expected: Only Frank (null department)

# Exercise 2d: Broadcast join (best practice for small lookup tables)
result_2d = employees.join(broadcast(departments), employees.department == departments.dept_name)
result_2d.show()
```

---

## Task 3: SQL Equivalents (30 min)

```python
# Register DataFrames as temp views
employees.createOrReplaceTempView("employees_view")
departments.createOrReplaceTempView("departments_view")
```

```sql
-- Exercise 3a: Replicate Task 1d using SQL
%sql
SELECT 
    COALESCE(department, 'Unknown') AS department,
    COUNT(*) AS headcount,
    ROUND(AVG(salary), 2) AS avg_salary
FROM employees_view
GROUP BY COALESCE(department, 'Unknown')
ORDER BY avg_salary DESC;
```

```sql
-- Exercise 3b: Window function — rank employees by salary within department
%sql
SELECT
    id,
    name,
    COALESCE(department, 'Unknown') AS department,
    salary,
    RANK() OVER (PARTITION BY COALESCE(department, 'Unknown') ORDER BY salary DESC) AS salary_rank,
    AVG(salary) OVER (PARTITION BY COALESCE(department, 'Unknown')) AS dept_avg
FROM employees_view;
```

```sql
-- Exercise 3c: CTE to find top earner per department
%sql
WITH ranked AS (
    SELECT 
        id, name, department, salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
    FROM employees_view
    WHERE department IS NOT NULL
)
SELECT id, name, department, salary
FROM ranked
WHERE rn = 1;
```

---

## Task 4: Write & Read Delta Table (40 min)

```python
# Exercise 4a: Write employees DataFrame to a Delta table
# (In Community Edition, use /tmp/ path)
employees_path = "/tmp/employees_delta"

employees.write \
    .mode("overwrite") \
    .format("delta") \
    .save(employees_path)

print("Written to Delta successfully")
```

```python
# Exercise 4b: Read back from Delta
df_delta = spark.read.format("delta").load(employees_path)
df_delta.show()
```

```python
# Exercise 4c: Append new data
new_employees = spark.createDataFrame([
    (7, "Grace", "Engineering", 105000, "2023-01-01"),
    (8, "Hank", "Marketing", 68000, "2023-06-15")
], emp_schema)

new_employees.write \
    .mode("append") \
    .format("delta") \
    .save(employees_path)

# Verify: should now have 8 rows
spark.read.format("delta").load(employees_path).count()
```

```sql
-- Exercise 4d: Create a SQL table pointing to our Delta path
%sql
CREATE TABLE IF NOT EXISTS employees_delta
USING DELTA
LOCATION '/tmp/employees_delta';

-- Now query it with SQL
SELECT * FROM employees_delta ORDER BY id;
```

---

## Task 5: Auto Loader Simulation (30 min)

```python
# Exercise 5: Simulate Auto Loader ingestion
# First, write some CSV files to a landing zone
import json

# Create landing directory with initial file
dbutils.fs.mkdirs("/tmp/autoloader_landing")

# Write a CSV file
customer_data = "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com"
dbutils.fs.put("/tmp/autoloader_landing/customers_001.csv", customer_data, True)

print("CSV file written to landing zone")
dbutils.fs.ls("/tmp/autoloader_landing")
```

```python
# Read with Auto Loader
df_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/tmp/autoloader_schema")
    .option("header", "true")
    .load("/tmp/autoloader_landing/")
)

# Write to Delta with trigger AvailableNow (batch-style processing)
query = (df_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/autoloader_checkpoint")
    .trigger(availableNow=True)  # process all existing files, then stop
    .start("/tmp/autoloader_output")
)

query.awaitTermination()
print("Auto Loader run complete")

# Read the output
spark.read.format("delta").load("/tmp/autoloader_output").show()
```

---

## Evening: Practice Questions

Go to [CertSafari](https://www.certsafari.com/databricks/data-engineer-associate) and complete **20 questions** on ELT/PySpark/SQL topics.

**Score:** ___ / 20  
**Weak areas:** ___________________________


---

## Task 6: Partitioning and Shuffle (30 min)

```python
# Task 6a: Check and control partitions
print(f"Default partitions: {employees.rdd.getNumPartitions()}")

# Repartition to 4 (causes a shuffle)
df_repartitioned = employees.repartition(4)
print(f"After repartition(4): {df_repartitioned.rdd.getNumPartitions()}")

# Coalesce to 2 (no shuffle)
df_coalesced = df_repartitioned.coalesce(2)
print(f"After coalesce(2): {df_coalesced.rdd.getNumPartitions()}")

# Repartition by specific column (hash partitioning)
df_by_dept = employees.repartition(4, col("department"))
print(f"After repartition by department: {df_by_dept.rdd.getNumPartitions()}")
```

```python
# Task 6b: Observe shuffle partition setting
print(f"Current shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Trigger a shuffle: groupBy causes a shuffle
result = employees.groupBy("department").count()
result.explain(True)  # View the execution plan — look for Exchange (shuffle)

# Reduce shuffle partitions for small data
spark.conf.set("spark.sql.shuffle.partitions", "4")
result2 = employees.groupBy("department").count()
result2.explain()  # Now should show fewer shuffle partitions
```

```python
# Task 6c: Broadcast join vs regular join
from pyspark.sql.functions import broadcast
import time

# Regular join (may shuffle both sides)
start = time.time()
result_regular = employees.join(departments, employees.department == departments.dept_name)
result_regular.count()
print(f"Regular join: {time.time() - start:.3f}s")

# Broadcast join (small table broadcast, no shuffle)
start = time.time()
result_broadcast = employees.join(broadcast(departments), employees.department == departments.dept_name)
result_broadcast.count()
print(f"Broadcast join: {time.time() - start:.3f}s")

# View the plan: look for BroadcastHashJoin vs SortMergeJoin
result_broadcast.explain()
```

```python
# Task 6d: Write partitioned data and observe partition pruning
import os

# First, create a larger dataset with year/month
from pyspark.sql.functions import lit

orders_data = [
    (1, "Widget A", 100.0, 2024, 1),
    (2, "Widget B", 200.0, 2024, 1),
    (3, "Widget C", 150.0, 2024, 2),
    (4, "Widget D", 300.0, 2023, 12),
    (5, "Widget E", 250.0, 2023, 11),
]
orders = spark.createDataFrame(orders_data, ["id", "product", "amount", "year", "month"])

# Write partitioned by year and month
orders.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .format("delta") \
    .save("/tmp/orders_partitioned")

# Read with partition filter (partition pruning — reads only matching folders)
df_pruned = spark.read.format("delta").load("/tmp/orders_partitioned") \
    .filter("year = 2024 AND month = 1")

df_pruned.explain()  # Look for PartitionFilters in the plan
df_pruned.show()
```

**Questions to answer:**
- After `repartition(4, col("department"))`, how many files will be written per partition?
- Why is `coalesce()` preferred after a filter operation before writing?
- In the explain plan, what operator indicates a shuffle (data movement)?

---

## Task 7: UDFs — Python UDF, Pandas UDF, SQL UDF (30 min)

```python
# Task 7a: Register a Python UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def salary_band(salary):
    """Classify salary into bands"""
    if salary is None:
        return "Unknown"
    elif salary < 70000:
        return "Junior"
    elif salary < 90000:
        return "Mid"
    else:
        return "Senior"

salary_band_udf = udf(salary_band, StringType())

# Apply Python UDF
result_7a = employees.withColumn("band", salary_band_udf(col("salary")))
result_7a.show()
# Note: Python UDFs are slow (row-by-row, pickle serialization)
```

```python
# Task 7b: Pandas UDF (much faster!)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def salary_band_pandas(salaries: pd.Series) -> pd.Series:
    """Vectorized version — processes entire column at once"""
    def classify(s):
        if pd.isna(s): return "Unknown"
        elif s < 70000: return "Junior"
        elif s < 90000: return "Mid"
        else: return "Senior"
    return salaries.apply(classify)

result_7b = employees.withColumn("band", salary_band_pandas(col("salary")))
result_7b.show()
# Compare: Pandas UDF uses Apache Arrow — much faster than Python UDF for large data
```

```sql
-- Task 7c: SQL UDF (persistent, stored in catalog)
-- Note: In Community Edition, this creates a temporary function
%sql
CREATE OR REPLACE TEMPORARY FUNCTION salary_band_sql(salary INT)
RETURNS STRING
RETURN CASE
    WHEN salary IS NULL THEN 'Unknown'
    WHEN salary < 70000 THEN 'Junior'
    WHEN salary < 90000 THEN 'Mid'
    ELSE 'Senior'
END;

-- Use the SQL UDF
SELECT name, salary, salary_band_sql(salary) AS band
FROM employees_view
ORDER BY salary DESC;
```

**Questions to answer:**
- Why should you prefer built-in Spark functions over Python UDFs?
- What is the key serialization advantage of Pandas UDFs over Python UDFs?
- In which access mode can Python UDFs NOT be used with Unity Catalog?

---

## Task 8: dbutils Essentials (20 min)

```python
# Task 8a: File system operations
# Create a directory
dbutils.fs.mkdirs("/tmp/dbutils_practice/")

# Write some data
dbutils.fs.put("/tmp/dbutils_practice/hello.txt", "Hello, Databricks!", overwrite=True)

# List files
files = dbutils.fs.ls("/tmp/dbutils_practice/")
for f in files:
    print(f"Name: {f.name}, Size: {f.size} bytes, Path: {f.path}")

# Read file content
content = dbutils.fs.head("/tmp/dbutils_practice/hello.txt")
print(f"File content: {content}")

# Copy file
dbutils.fs.cp("/tmp/dbutils_practice/hello.txt", "/tmp/dbutils_practice/hello_copy.txt")

# Verify copy
print(dbutils.fs.ls("/tmp/dbutils_practice/"))

# Delete
dbutils.fs.rm("/tmp/dbutils_practice/hello_copy.txt")
```

```python
# Task 8b: Widgets for parameterized notebooks
# Create widgets
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("max_records", "1000", "Max Records")

# Get widget values
start_date = dbutils.widgets.get("start_date")
env = dbutils.widgets.get("environment")
max_records = int(dbutils.widgets.get("max_records"))

print(f"Processing from {start_date} in {env} environment, max {max_records} records")

# Use widget values in query
df_filtered = employees.filter(col("salary") <= max_records * 100)
df_filtered.show()

# Cleanup
dbutils.widgets.removeAll()
```

```python
# Task 8c: Secret access pattern (conceptual — requires secret scope setup)
# In production, you would access credentials like this:
# password = dbutils.secrets.get(scope="my-scope", key="db-password")
# Never hardcode credentials!

# List available scopes (shows what's accessible)
try:
    scopes = dbutils.secrets.listScopes()
    print(f"Available scopes: {[s.name for s in scopes]}")
except Exception as e:
    print(f"No secret scopes configured in this environment: {e}")
```

**Questions to answer:**
- What is the difference between `dbutils.fs.ls()` and `%fs ls`?
- Why should you NEVER hardcode credentials in a notebook? What should you use instead?
- How do you pass parameters to a notebook job using `dbutils.widgets`?
