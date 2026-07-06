# Day 1 — Practice Tasks: Advanced Delta Lake

## Setup
```python
spark.sql("USE CATALOG training")
spark.sql("USE SCHEMA prep")
```

---

## Task 1: Inspect the Transaction Log

Create a Delta table and inspect its transaction log JSON files.

```sql
-- Step 1: Create a Delta table
CREATE OR REPLACE TABLE training.prep.pro_d1_orders (
  order_id  BIGINT,
  customer  STRING,
  amount    DOUBLE,
  region    STRING,
  order_date DATE
) USING DELTA
COMMENT 'Pro Day 1 practice table';

-- Step 2: Insert some data (version 1)
INSERT INTO training.prep.pro_d1_orders VALUES
  (1, 'Alice',  120.50, 'EU', '2024-01-01'),
  (2, 'Bob',    200.00, 'US', '2024-01-02'),
  (3, 'Carlos', 350.75, 'LATAM', '2024-01-03');

-- Step 3: Update (version 2)
UPDATE training.prep.pro_d1_orders SET amount = 130.00 WHERE order_id = 1;

-- Step 4: Inspect history
DESCRIBE HISTORY training.prep.pro_d1_orders;
```

```python
# Step 5: Read the actual transaction log JSON (Python)
# UC-safe version: read the Delta log from a path-based mirror table,
# not from the Unity Catalog managed storage location.

import json

log_inspect_path = "/tmp/pro_d1_orders_log_inspect"

spark.sql(f"""
  CREATE OR REPLACE TABLE delta.`{log_inspect_path}` AS
  SELECT * FROM training.prep.pro_d1_orders
""")

log_path = log_inspect_path + "/_delta_log/00000000000000000000.json"
log_df = spark.read.text(log_path)

for row in log_df.collect():
    entry = json.loads(row["value"])
    print(json.dumps(entry, indent=2))
```

**Expected:** You should see `commitInfo` (with operation=CREATE TABLE), `metaData` (schema), and `protocol` entries.

---

## Task 2: Shallow vs Deep Clone

```sql
-- Shallow clone
CREATE OR REPLACE TABLE training.prep.pro_d1_orders_shallow
SHALLOW CLONE training.prep.pro_d1_orders;

-- Verify: insert into clone - does source change?
INSERT INTO training.prep.pro_d1_orders_shallow VALUES
  (99, 'Test', 1.00, 'EU', '2024-06-01');

SELECT COUNT(*) as clone_count FROM training.prep.pro_d1_orders_shallow;
SELECT COUNT(*) as source_count FROM training.prep.pro_d1_orders;
-- clone_count = 4, source_count = 3  --> They are independent!

-- Deep clone
CREATE OR REPLACE TABLE training.prep.pro_d1_orders_deep
DEEP CLONE training.prep.pro_d1_orders;

-- Verify deep clone has its own files
DESCRIBE DETAIL training.prep.pro_d1_orders_deep;
-- location will be different from source
```

**Questions to answer:**
1. What happens if you DROP the source table and then query the shallow clone?
2. Can you time-travel a shallow clone independently of the source?

---

## Task 3: Bloom Filters

```sql
-- Create table with bloom filter on high-cardinality column
CREATE OR REPLACE TABLE training.prep.pro_d1_users (
  user_id   STRING,
  email     STRING,
  region    STRING,
  signup_dt DATE
) USING DELTA
TBLPROPERTIES (
  'delta.bloomFilter.user_id.enabled' = 'true',
  'delta.bloomFilter.user_id.fpp'     = '0.01',
  'delta.bloomFilter.user_id.numItems'= '1000000'
);

-- Insert test data
INSERT INTO training.prep.pro_d1_users
SELECT
  uuid()       AS user_id,
  concat(uuid(), '@test.com') AS email,
  element_at(array('EU','US','APAC'), (rand()*3+1)::int) AS region,
  date_add('2020-01-01', (rand()*1000)::int) AS signup_dt
FROM range(10000);

-- Query and check execution plan for bloom filter usage
EXPLAIN SELECT * FROM training.prep.pro_d1_users WHERE user_id = 'some-uuid';
-- Look for BloomFilterAggregate in the plan
```

---

## Task 4: Liquid Clustering

```sql
-- Create a liquid-clustered table
CREATE OR REPLACE TABLE training.prep.pro_d1_events (
  event_id   BIGINT,
  user_id    STRING,
  event_type STRING,
  event_date DATE,
  payload    STRING
) USING DELTA
CLUSTER BY (event_date, user_id)
COMMENT 'Liquid clustered events table';

-- Insert data
INSERT INTO training.prep.pro_d1_events
SELECT
  id AS event_id,
  uuid() AS user_id,
  element_at(array('click','view','purchase'), (rand()*3+1)::int) AS event_type,
  date_add('2024-01-01', (rand()*365)::int) AS event_date,
  'payload_' || id AS payload
FROM range(50000);

-- Trigger clustering
OPTIMIZE training.prep.pro_d1_events;

-- Check clustering info
DESCRIBE DETAIL training.prep.pro_d1_events;

-- Change cluster columns (no full rewrite needed)
ALTER TABLE training.prep.pro_d1_events
CLUSTER BY (user_id, event_type);
```

---

## Task 5: Concurrency Simulation

```python
# Simulate concurrent writes using Python threading
import threading
import time

def writer_a():
    spark.sql("""
        UPDATE training.prep.pro_d1_orders
        SET amount = amount * 1.1
        WHERE region = 'EU'
    """)
    print("Writer A (EU update) done")

def writer_b():
    spark.sql("""
        UPDATE training.prep.pro_d1_orders
        SET amount = amount * 0.9
        WHERE region = 'US'
    """)
    print("Writer B (US update) done")

# These should NOT conflict (different regions = different data)
t1 = threading.Thread(target=writer_a)
t2 = threading.Thread(target=writer_b)
t1.start()
t2.start()
t1.join()
t2.join()

# Verify history shows 2 commits
spark.sql("DESCRIBE HISTORY training.prep.pro_d1_orders").show(5)
```

---

## Reflection Questions

1. What is stored in the `remove` action of a Delta transaction log entry?
2. Why does Delta use a checkpoint every 10 commits?
3. When would you choose `Serializable` vs `WriteSerializable` isolation?
4. What is the key limitation of Shallow Clone?
5. Why should you never partition by UUID or user_id columns?
6. How does Liquid Clustering differ from traditional partitioning + ZORDER?

## Answers

1. The path to the removed (logically deleted) Parquet file, size, stats, and deletion timestamp. File is NOT physically deleted until VACUUM.
2. To speed up snapshot construction — without checkpoints, Delta would need to replay ALL JSON log entries from version 0.
3. Use `Serializable` when correctness is critical (banking, deduplication). Use `WriteSerializable` for streaming appends where blind writes don't conflict.
4. Shallow clone requires the source data files to remain accessible. If source is vacuumed or deleted, shallow clone breaks.
5. High cardinality partitioning creates millions of tiny files (the "small files problem"), degrading performance.
6. Liquid Clustering is incremental (only re-clusters new/changed data), allows changing cluster columns without full rewrites, and avoids partition explosion.
