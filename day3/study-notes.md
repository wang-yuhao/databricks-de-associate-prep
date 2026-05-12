# Day 3 — Delta Lake Deep Dive + Schema Evolution

> **Exam weight:** ~15% of total exam  
> **Estimated read time:** 90 minutes  
> **Goal:** Master Delta Lake internals, time travel, MERGE, OPTIMIZE, and schema handling

---

## 1. Delta Lake Architecture

Delta Lake is an open-source storage layer that brings ACID transactions to data lakes. Every Delta table is a folder of Parquet files plus a `_delta_log/` transaction log directory.

### Transaction Log (`_delta_log/`)
- Ordered sequence of JSON commit files: `000000.json`, `000001.json`, …
- Each commit records **add**, **remove**, and **metadata** actions
- After every 10 commits, Databricks creates a Parquet **checkpoint** file for fast log replay
- Readers use the log to construct a consistent snapshot without locking writers

### ACID Guarantees
| Property | How Delta Achieves It |
|---|---|
| **Atomicity** | Either all files of a write are committed or none are |
| **Consistency** | Schema enforcement prevents corrupt writes |
| **Isolation** | Optimistic concurrency with conflict detection |
| **Durability** | Committed log entries are permanent on cloud storage |

---

## 2. Creating & Managing Delta Tables

```sql
-- Create a managed Delta table
CREATE TABLE sales (
  sale_id   BIGINT,
  product   STRING,
  amount    DOUBLE,
  sale_date DATE
) USING DELTA;

-- Create from existing data
CREATE TABLE sales
USING DELTA
AS SELECT * FROM parquet.`/mnt/raw/sales/`;

-- Convert Parquet to Delta in-place
CONVERT TO DELTA parquet.`/mnt/raw/sales/`;

-- Describe table details
DESCRIBE DETAIL sales;
DESCRIBE HISTORY sales;
```

### Managed vs. External Tables
| | Managed | External |
|---|---|---|
| **Storage** | Databricks-controlled location | User-specified path |
| **DROP TABLE** | Deletes data + metadata | Deletes metadata only |
| **Default in Unity Catalog** | Yes (in schema's location) | Specify `LOCATION` |

---

## 3. CRUD Operations on Delta Tables

### INSERT
```sql
INSERT INTO sales VALUES (1, 'Widget A', 99.99, '2024-01-15');
INSERT OVERWRITE sales SELECT * FROM staging_sales;
```

### UPDATE
```sql
UPDATE sales
SET amount = amount * 1.10
WHERE product = 'Widget A';
```

### DELETE
```sql
DELETE FROM sales
WHERE sale_date < '2023-01-01';
```

### MERGE (Upsert)
MERGE is the most exam-tested Delta operation:

```sql
MERGE INTO target AS t
USING source AS s
ON t.sale_id = s.sale_id
WHEN MATCHED AND s.amount != t.amount THEN
  UPDATE SET t.amount = s.amount, t.sale_date = s.sale_date
WHEN NOT MATCHED THEN
  INSERT (sale_id, product, amount, sale_date)
  VALUES (s.sale_id, s.product, s.amount, s.sale_date)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;
```

> ⚠️ `WHEN NOT MATCHED BY SOURCE` (delete rows in target not in source) requires Delta Lake 2.x+.

---

## 4. Time Travel

Delta Lake keeps historical versions; you can query any past state.

```sql
-- By version number
SELECT * FROM sales VERSION AS OF 3;
SELECT * FROM sales@v3;

-- By timestamp
SELECT * FROM sales TIMESTAMP AS OF '2024-01-01 00:00:00';
SELECT * FROM sales@t'2024-01-01';

-- Restore a table to a previous version
RESTORE TABLE sales TO VERSION AS OF 2;
RESTORE TABLE sales TO TIMESTAMP AS OF '2024-01-01';
```

### Retention
- Default retention: **30 days** (controlled by `delta.logRetentionDuration`)
- `VACUUM` permanently removes files older than the retention threshold

```sql
-- Default: removes files older than 7 days (168 hours)
VACUUM sales;

-- Explicitly set retention (dangerous: disables time travel beyond this point)
VACUUM sales RETAIN 0 HOURS;  -- Requires spark.databricks.delta.retentionDurationCheck.enabled = false
```

> 🚨 **Exam trap:** After `VACUUM`, you cannot time travel past the retention threshold. Plan vacuuming carefully.

---

## 5. OPTIMIZE and Z-ORDER

### OPTIMIZE
Compacts small Parquet files into larger ones (default target: 1 GB per file). Dramatically improves read performance.

```sql
OPTIMIZE sales;

-- Optimize only a partition
OPTIMIZE sales WHERE sale_date = '2024-01-15';
```

### Z-ORDER (Multi-Dimensional Clustering)
Co-locates related data in the same files so that data skipping is more effective.

```sql
OPTIMIZE sales ZORDER BY (product, sale_date);
```

- Z-ORDER works best for high-cardinality columns used in WHERE filters
- Delta stores min/max statistics per column per file → queries skip irrelevant files
- Use 1–4 columns; too many reduces benefit

### Auto Optimize (Databricks-specific)
```sql
-- Enable on existing table
ALTER TABLE sales SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

---

## 6. Schema Evolution and Enforcement

### Schema Enforcement (default: ON)
Delta rejects writes with columns that don't match the table schema.

```python
# This will FAIL if df has extra columns
df.write.format("delta").mode("append").save("/mnt/sales")
```

### Schema Evolution — mergeSchema
Allows adding new columns on the fly:

```python
# Python: allow schema evolution
df.write.format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .save("/mnt/sales")
```

```sql
-- SQL: set at session level
SET spark.databricks.delta.schema.autoMerge.enabled = true;
INSERT INTO sales SELECT * FROM new_sales_with_extra_col;
```

### Column Mapping
Allows renaming/dropping columns without rewriting data files:
```sql
ALTER TABLE sales SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE sales RENAME COLUMN amount TO sale_amount;
ALTER TABLE sales DROP COLUMN old_col;
```

---

## 7. Change Data Feed (CDF)

CDF exposes row-level changes (insert, update, delete) as a readable stream or batch.

```sql
-- Enable CDF on a table
ALTER TABLE sales SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Create table with CDF enabled
CREATE TABLE sales (...) TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

```python
# Read changes as a batch
spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .table("sales")

# Read changes as a stream
spark.readStream.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .table("sales")
```

### CDF Output Columns
| Column | Meaning |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | Delta version of the change |
| `_commit_timestamp` | Timestamp of the commit |

> 💡 CDF is commonly used for **incremental Silver/Gold layer updates** in the Medallion architecture.

---

## 8. Medallion Architecture

```
[Raw Sources]
     ↓
[Bronze Layer]  — Raw, unvalidated data; append-only; full history
     ↓
[Silver Layer]  — Cleaned, deduplicated, conformed; filtered/joined
     ↓
[Gold Layer]    — Business-level aggregates; optimized for BI/ML
```

### Key Principles
- **Bronze:** Preserve source fidelity. Add `_ingest_timestamp`, `_source_file`. Schema-on-read.
- **Silver:** Apply quality rules, deduplicate via MERGE, enforce schema. Row-level data.
- **Gold:** Aggregated, pre-joined, read-optimized for specific use cases.

```sql
-- Example: Silver MERGE from Bronze (using CDF or full load)
MERGE INTO silver.sales AS t
USING (
  SELECT sale_id, product, amount, sale_date
  FROM bronze.raw_sales
  WHERE _ingest_date = current_date()
) AS s
ON t.sale_id = s.sale_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## 9. Key Table Properties (Exam Reference)

```sql
ALTER TABLE sales SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '32'
);
```

---

## 10. Quick-Reference Cheat Sheet

| Operation | SQL / Python |
|---|---|
| Time travel (version) | `SELECT * FROM t VERSION AS OF 5` |
| Time travel (timestamp) | `SELECT * FROM t TIMESTAMP AS OF '2024-01-01'` |
| Restore | `RESTORE TABLE t TO VERSION AS OF 3` |
| Compact files | `OPTIMIZE t` |
| Cluster data | `OPTIMIZE t ZORDER BY (col)` |
| Remove old files | `VACUUM t [RETAIN N HOURS]` |
| View history | `DESCRIBE HISTORY t` |
| View table details | `DESCRIBE DETAIL t` |
| Enable CDF | `ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed=true)` |
| Merge/upsert | `MERGE INTO target USING source ON key ...` |
| Schema evolution | `.option("mergeSchema","true")` or `autoMerge.enabled=true` |

---

## 📺 Recommended Videos

| Video | Duration | Link |
|---|---|---|
| Delta Lake Quickstart — Databricks | 15 min | https://www.youtube.com/watch?v=BMO90DI82Dc |
| Delta Lake Internals Deep Dive | 30 min | https://www.youtube.com/watch?v=LJtShrQqYZY |
| Time Travel & VACUUM Demo | 12 min | https://www.youtube.com/watch?v=5I6IHj9ZQHA |
| Medallion Architecture Explained | 20 min | https://www.youtube.com/watch?v=eUBemRQR_kI |

---

## 📖 Official Docs

- Delta Lake Guide: https://docs.databricks.com/delta/index.html
- Time Travel: https://docs.databricks.com/delta/history.html
- Change Data Feed: https://docs.databricks.com/delta/delta-change-data-feed.html
- OPTIMIZE: https://docs.databricks.com/optimizations/delta-optimize.html
- MERGE syntax: https://docs.databricks.com/sql/language-manual/delta-merge-into.html
