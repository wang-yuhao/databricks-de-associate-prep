# Day 2 Practice Tasks — ELT with Spark SQL & Python

## Task 1 — Database & Table Setup
Using the `training` catalog and `prep` schema, run the following in a notebook:
```sql
USE CATALOG training;
USE SCHEMA prep;
SHOW TABLES;
```
> List all tables you see and describe which are managed vs external.

---

## Task 2 — Data Ingestion (CTAS + Auto Loader)

### Part A — CTAS from JSON
```sql
CREATE OR REPLACE TABLE orders_raw
COMMENT 'Raw orders ingested from JSON'
AS
SELECT * FROM json.`dbfs:/FileStore/orders/*.json`;
```
Verify row count and schema with `DESCRIBE EXTENDED orders_raw`.

### Part B — Auto Loader (Python)
Write a streaming Auto Loader snippet that reads new CSV files from
`dbfs:/FileStore/incoming/` into a Delta table `orders_stream_raw`.
Include `cloudFiles.schemaLocation` and `checkpointLocation`.

```python
# Your Auto Loader code here
```

---

## Task 3 — Transformation Chain
Using `orders_raw`, write SQL to:
1. Filter out rows where `order_status = 'cancelled'`
2. Cast `order_date` (string) to `DATE`
3. Add a derived column `order_year` using `YEAR(order_date)`
4. Write result into a new table `orders_clean` using `CREATE OR REPLACE TABLE … AS SELECT`

```sql
-- Your SQL here
```

---

## Task 4 — Higher-Order Functions
Given a table with column `items ARRAY<STRING>`, write SQL using:
- `FILTER(items, i -> i LIKE 'book%')` to keep only book items
- `TRANSFORM(items, i -> UPPER(i))` to uppercase all items
- `EXPLODE(items)` to flatten the array into rows

---

## Task 5 — Concept Short-Answer
1. What is the difference between `INSERT INTO` and `INSERT OVERWRITE`?
2. When would you use `MERGE INTO` instead of `INSERT OVERWRITE`?
3. What does `GENERATED ALWAYS AS` do in a Delta table definition?
