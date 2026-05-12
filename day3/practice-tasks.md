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
