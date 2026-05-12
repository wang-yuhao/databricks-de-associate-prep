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
