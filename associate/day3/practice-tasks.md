# Day 3 Practice Tasks — Delta Lake

> **Setup:** Databricks Community Edition — https://community.cloud.databricks.com  
> **Cluster:** Single-node (Community Edition default is fine)  
> **Estimated time:** 3–4 hours

---

## Task 0: Cluster Setup (5 min)

1. Log in to [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Go to **Compute → Create Cluster** (if none running)
3. Name it `day3-delta`; use defaults (DBR 14.x LTS recommended)
4. Create a new notebook: **Workspace → Create → Notebook**, name it `day3_delta_lake`, language **SQL** (you can switch cells)

---

## Task 1: Create a Delta Table and Explore the Transaction Log (20 min)

```sql
-- Cell 1: Create Delta table
CREATE OR REPLACE TABLE workshop.sales (
  sale_id   BIGINT,
  product   STRING,
  region    STRING,
  amount    DOUBLE,
  sale_date DATE
) USING DELTA
LOCATION '/tmp/workshop/sales';

-- Cell 2: Insert rows (creates version 1)
INSERT INTO workshop.sales VALUES
  (1, 'Widget A', 'EU', 99.99,  '2024-01-10'),
  (2, 'Widget B', 'US', 149.50, '2024-01-11'),
  (3, 'Gadget X', 'EU', 299.00, '2024-01-12');

-- Cell 3: View history
DESCRIBE HISTORY workshop.sales;

-- Cell 4: View table details (file count, size, etc.)
DESCRIBE DETAIL workshop.sales;
```

**Expected:** DESCRIBE HISTORY shows 2 versions (0 = CREATE, 1 = INSERT).

---

## Task 2: Practice CRUD + MERGE (30 min)

```sql
-- Cell 5: UPDATE
UPDATE workshop.sales
SET amount = amount * 1.20
WHERE region = 'EU';

-- Cell 6: DELETE
DELETE FROM workshop.sales
WHERE product = 'Gadget X';

-- Cell 7: Create a source table for MERGE
CREATE OR REPLACE TABLE workshop.sales_updates (
  sale_id   BIGINT,
  product   STRING,
  region    STRING,
  amount    DOUBLE,
  sale_date DATE
) USING DELTA;

INSERT INTO workshop.sales_updates VALUES
  (2, 'Widget B', 'US', 199.99, '2024-02-01'),  -- update existing
  (4, 'Gadget Y', 'APAC', 450.00, '2024-02-02'); -- new row

-- Cell 8: MERGE (upsert)
MERGE INTO workshop.sales AS t
USING workshop.sales_updates AS s
ON t.sale_id = s.sale_id
WHEN MATCHED THEN
  UPDATE SET t.amount = s.amount, t.sale_date = s.sale_date
WHEN NOT MATCHED THEN
  INSERT (sale_id, product, region, amount, sale_date)
  VALUES (s.sale_id, s.product, s.region, s.amount, s.sale_date);

-- Cell 9: Verify
SELECT * FROM workshop.sales ORDER BY sale_id;
```

**Expected:** sale_id=2 has amount=199.99; sale_id=4 is present; sale_id=3 is gone.

---

## Task 3: Time Travel (20 min)

```sql
-- Cell 10: View current version
DESCRIBE HISTORY workshop.sales;

-- Cell 11: Query version 1 (after first INSERT)
SELECT * FROM workshop.sales VERSION AS OF 1;

-- Cell 12: Query by timestamp (use a timestamp from history)
-- Replace the timestamp with one from your DESCRIBE HISTORY output
SELECT * FROM workshop.sales
TIMESTAMP AS OF '2024-01-01 00:00:00';  -- use actual timestamp

-- Cell 13: Restore to version 1
RESTORE TABLE workshop.sales TO VERSION AS OF 1;
SELECT * FROM workshop.sales;
```

**Verify:** After RESTORE, you see the original 3 rows.

---

## Task 4: OPTIMIZE and Z-ORDER (20 min)

```python
# Cell 14 (Python): Generate larger dataset to see optimization benefits
from pyspark.sql import Row
from datetime import date, timedelta
import random

products = ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Device Z"]
regions  = ["EU", "US", "APAC", "LATAM"]

rows = [
  Row(
    sale_id=i,
    product=random.choice(products),
    region=random.choice(regions),
    amount=round(random.uniform(10, 1000), 2),
    sale_date=(date(2024, 1, 1) + timedelta(days=i % 365))
  )
  for i in range(1, 100001)
]

df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").saveAsTable("workshop.big_sales")
```

```sql
-- Cell 15: Check file count before optimize
DESCRIBE DETAIL workshop.big_sales;

-- Cell 16: Optimize and Z-Order
OPTIMIZE workshop.big_sales ZORDER BY (product, region);

-- Cell 17: Check file count after optimize
DESCRIBE DETAIL workshop.big_sales;

-- Cell 18: Query with filter — Delta will skip irrelevant files
SELECT COUNT(*), SUM(amount)
FROM workshop.big_sales
WHERE product = 'Widget A' AND region = 'EU';
```

**Observe:** File count drops significantly after OPTIMIZE.

---

## Task 5: Schema Evolution (20 min)

```python
# Cell 19: Add a new column to the DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import lit

# New DF with extra column
new_df = spark.createDataFrame([
  (99999, "Widget A", "EU", 99.99, date(2024, 6, 1), "promo")
], ["sale_id", "product", "region", "amount", "sale_date", "discount_code"])

# This will FAIL without mergeSchema
try:
  new_df.write.format("delta").mode("append").saveAsTable("workshop.sales")
except Exception as e:
  print("Expected error:", str(e)[:200])

# Now with mergeSchema
new_df.write.format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable("workshop.sales")
```

```sql
-- Cell 20: Verify schema was evolved
DESCRIBE TABLE workshop.sales;
SELECT * FROM workshop.sales WHERE discount_code IS NOT NULL;
```

---

## Task 6: Enable and Read Change Data Feed (25 min)

```sql
-- Cell 21: Enable CDF
ALTER TABLE workshop.sales
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Cell 22: Make some changes
INSERT INTO workshop.sales (sale_id, product, region, amount, sale_date)
VALUES (5000, 'New Product', 'EU', 500.00, '2024-06-15');

UPDATE workshop.sales SET amount = 550.00 WHERE sale_id = 5000;

DELETE FROM workshop.sales WHERE sale_id = 5000;
```

```python
# Cell 23: Read CDF in batch mode
changes = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .table("workshop.sales")

changes.select("sale_id", "product", "amount", "_change_type", "_commit_version").show()
```

**Expected output:** Rows with `_change_type` of `insert`, `update_preimage`, `update_postimage`, `delete`.

---

## Task 7: VACUUM (10 min)

```sql
-- Cell 24: View current files
DESCRIBE DETAIL workshop.sales;

-- Cell 25: Vacuum with short retention (for testing only — never do 0 in production)
-- First disable the safety check:
```

```python
# Cell 26
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
```

```sql
-- Cell 27
VACUUM workshop.sales RETAIN 0 HOURS;

-- Cell 28: Now try time travel — should fail
SELECT * FROM workshop.sales VERSION AS OF 1;  -- This will now fail
```

**Observe:** After VACUUM with 0 retention, old versions are inaccessible.

---

## ✅ Checkpoint Quiz

Answer these without looking at notes:

1. What directory stores the Delta transaction log?
2. What happens to data files when you run `DROP TABLE` on a **managed** Delta table?
3. What SQL command compacts small files in a Delta table?
4. What option do you pass to `.write()` to allow adding new columns?
5. What are the three `_change_type` values for an UPDATE in CDF?
6. After running `VACUUM RETAIN 0 HOURS`, can you restore to version 1?
7. What is the default file retention for Delta Lake?

<details>
<summary>Answers</summary>

1. `_delta_log/`
2. Both metadata and data files are deleted
3. `OPTIMIZE`
4. `.option("mergeSchema", "true")`
5. `update_preimage`, `update_postimage` (and the delete of the old record is `update_preimage`)
6. No — VACUUM removes the physical files
7. 30 days for log, 7 days for deleted data files

</details>

---

## Task 8: Medallion Architecture — Bronze / Silver / Gold (30 min)

The Medallion Architecture is a data design pattern used in Databricks Lakehouse to organize data into three layers of quality.

**Concept:**
- **Bronze**: Raw ingested data (as-is from source, no transformations)
- **Silver**: Cleaned, validated, enriched data (deduplication, type casting, null handling)
- **Gold**: Aggregated, business-ready data (KPIs, reports, ML features)

```sql
-- Cell 29: Create Bronze layer (raw ingested data)
CREATE OR REPLACE TABLE workshop.bronze_sales
USING DELTA AS
SELECT
  sale_id,
  product,
  region,
  amount,
  sale_date,
  current_timestamp() AS ingested_at,
  'source_system_A' AS source
FROM workshop.sales;

SELECT * FROM workshop.bronze_sales;
```

```sql
-- Cell 30: Create Silver layer (cleaned + validated data)
CREATE OR REPLACE TABLE workshop.silver_sales
USING DELTA AS
SELECT
  sale_id,
  UPPER(product) AS product,
  UPPER(region) AS region,
  CAST(amount AS DOUBLE) AS amount,
  TO_DATE(sale_date, 'yyyy-MM-dd') AS sale_date,
  ingested_at
FROM workshop.bronze_sales
WHERE sale_id IS NOT NULL
  AND amount > 0;

SELECT * FROM workshop.silver_sales;
```

```sql
-- Cell 31: Create Gold layer (aggregated business metrics)
CREATE OR REPLACE TABLE workshop.gold_sales_summary
USING DELTA AS
SELECT
  region,
  COUNT(*) AS total_transactions,
  SUM(amount) AS total_revenue,
  AVG(amount) AS avg_order_value,
  MAX(sale_date) AS last_sale_date
FROM workshop.silver_sales
GROUP BY region
ORDER BY total_revenue DESC;

SELECT * FROM workshop.gold_sales_summary;
```

**Observe:** Each layer adds more quality and business value. Bronze = immutable raw, Silver = clean, Gold = aggregated.

---

## Task 9: COPY INTO — Idempotent Data Ingestion (20 min)

`COPY INTO` is an idempotent, incremental file ingestion command — it only loads files that haven't been loaded yet.

```python
# Cell 32: Create sample CSV files to simulate source data
import json
dbutils.fs.mkdirs("/tmp/workshop/incoming/")

# Write sample CSV data
dbutils.fs.put("/tmp/workshop/incoming/sales_jan.csv",
"""sale_id,product,region,amount,sale_date
101,Alpha,EU,100.0,2024-01-05
102,Beta,US,200.0,2024-01-06
103,Gamma,EU,150.0,2024-01-07""", overwrite=True)
```

```sql
-- Cell 33: Create target Delta table
CREATE OR REPLACE TABLE workshop.ingested_sales (
  sale_id BIGINT,
  product STRING,
  region STRING,
  amount DOUBLE,
  sale_date DATE
) USING DELTA;
```

```sql
-- Cell 34: Use COPY INTO to load the CSV file
COPY INTO workshop.ingested_sales
FROM '/tmp/workshop/incoming/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM workshop.ingested_sales;
```

```sql
-- Cell 35: Run COPY INTO again (same files) — should load 0 new rows (idempotent)
COPY INTO workshop.ingested_sales
FROM '/tmp/workshop/incoming/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Check row count — should still be 3
SELECT COUNT(*) AS total_rows FROM workshop.ingested_sales;
```

**Observe:** `COPY INTO` tracks which files were already loaded. Re-running it does NOT duplicate rows.

**Key differences vs INSERT INTO:**
- `COPY INTO` = idempotent, file-tracking, incremental
- `INSERT INTO` = always inserts, no deduplication

---

## Task 10: Liquid Clustering (10 min — Conceptual + Demo)

Liquid Clustering replaces traditional `PARTITION BY` with a more flexible, automatic approach.

```sql
-- Cell 36: Create a table with Liquid Clustering
CREATE OR REPLACE TABLE workshop.sales_clustered
USING DELTA
CLUSTER BY (region, sale_date)
AS SELECT * FROM workshop.silver_sales;

-- Inspect the table
DESCRIBE DETAIL workshop.sales_clustered;
```

```sql
-- Cell 37: Trigger clustering (in production, this runs automatically)
OPTIMIZE workshop.sales_clustered;

-- Check after optimize
DESCRIBE HISTORY workshop.sales_clustered;
```

**Key facts for the exam:**
- Liquid Clustering: flexible, no full rewrites needed when clustering keys change
- Traditional partitioning: static, can cause small file problems with high-cardinality columns
- Use `CLUSTER BY` instead of `PARTITION BY` for most new tables

---

## ✅ Checkpoint Quiz — Extended

Answer these without looking at notes:

1. What are the three layers of the Medallion Architecture?
2. What type of data goes into the Bronze layer?
3. What does `COPY INTO` do when you run it a second time with the same files?
4. How is `COPY INTO` different from `INSERT INTO`?
5. What clause replaces `PARTITION BY` in Liquid Clustering?
6. When should you use Silver layer vs Gold layer?

<details>
<summary>Answers</summary>

1. Bronze (raw), Silver (cleaned), Gold (aggregated/business-ready)
2. Raw, unmodified data as-is from the source system
3. It loads 0 new rows — it's idempotent and tracks already-loaded files
4. `COPY INTO` is idempotent and file-tracking; `INSERT INTO` always inserts all rows
5. `CLUSTER BY`
6. Silver = cleaned/validated data for analysts/data scientists; Gold = aggregated KPIs for dashboards/reports

</details>
