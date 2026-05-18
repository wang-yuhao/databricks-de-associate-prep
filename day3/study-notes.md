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


---

## 11. Medallion Architecture — Deep Dive

The Medallion Architecture (also called Multi-Hop Architecture) is Databricks' recommended data organization pattern. It structures data into progressive layers of quality.

### Architecture Overview

```
[External Sources]
  Raw files, APIs, CDC streams, Kafka
       ↓ (Auto Loader / COPY INTO / Kafka)
[Bronze Layer]  ← Raw, unvalidated, append-only, full history
       ↓ (MERGE / APPLY CHANGES INTO via DLT)
[Silver Layer]  ← Cleaned, deduplicated, conformed, row-level
       ↓ (GROUP BY / aggregations)
[Gold Layer]    ← Business-level aggregates, BI/ML-ready
       ↓
[Serving Layer] ← Dashboards, ML models, APIs
```

### Bronze Layer — Raw Ingestion

**Purpose:** Preserve raw source data exactly as received. Full audit trail.

**Characteristics:**
- Schema-on-read (flexible schema)
- Append-only (never update/delete original raw records)
- Add metadata columns: `_ingest_timestamp`, `_source_file`, `_source_system`
- Store ALL data, even bad/malformed records
- Partition by ingestion date for performance

```python
# Bronze ingestion with Auto Loader
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/bronze_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/landing/orders/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .partitionBy("_ingest_date") \
    .table("catalog.bronze.raw_orders")
```

```sql
-- Bronze table definition (SQL)
CREATE TABLE catalog.bronze.raw_orders (
  order_id      STRING,
  customer_id   STRING,
  amount        STRING,   -- keep as STRING to preserve raw value
  order_date    STRING,   -- parse types in Silver
  _ingest_timestamp TIMESTAMP,
  _source_file  STRING
)
USING DELTA
PARTITIONED BY (_ingest_date DATE)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);
```

### Silver Layer — Cleaned & Conformed

**Purpose:** Apply data quality rules, enforce schema, deduplicate, join reference data.

**Characteristics:**
- Typed columns (cast STRING → INT, DATE, TIMESTAMP)
- Deduplicated via MERGE (use primary key)
- Filtered (remove nulls, bad records)
- May join lookup/reference tables
- Row-level data (not aggregated)
- Enforce schema strictly

```sql
-- Silver MERGE from Bronze (incremental upsert)
MERGE INTO catalog.silver.orders AS t
USING (
  SELECT
    order_id,
    customer_id,
    CAST(amount AS DOUBLE)   AS amount,
    CAST(order_date AS DATE) AS order_date,
    _ingest_timestamp
  FROM catalog.bronze.raw_orders
  WHERE _ingest_date = current_date()
    AND order_id IS NOT NULL
    AND CAST(amount AS DOUBLE) > 0
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s._ingest_timestamp > t._ingest_timestamp THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;
```

```python
# Silver with streaming + foreachBatch for MERGE
def upsert_to_silver(batch_df, batch_id):
    batch_df.createOrReplaceTempView("silver_updates")
    batch_df.sparkSession.sql("""
        MERGE INTO catalog.silver.orders t
        USING silver_updates s ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("catalog.bronze.raw_orders")
    .filter(col("order_id").isNotNull())
    .withColumn("amount", col("amount").cast("double"))
    .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "/checkpoints/silver")
    .start()
)
```

### Gold Layer — Business Aggregates

**Purpose:** Pre-aggregated, pre-joined, domain-specific tables optimized for BI tools and ML models.

**Characteristics:**
- Aggregated data (SUM, COUNT, AVG over time windows)
- Pre-joined fact + dimension tables (star/snowflake schema)
- Optimized for query performance (OPTIMIZE, ZORDER)
- Named for business domains: `daily_revenue`, `customer_360`, `product_performance`
- Often rebuilt fully (INSERT OVERWRITE) or incrementally

```sql
-- Gold: daily revenue aggregation
CREATE OR REPLACE TABLE catalog.gold.daily_revenue
USING DELTA AS
SELECT
  o.order_date,
  c.region,
  p.category,
  COUNT(DISTINCT o.order_id)     AS order_count,
  SUM(o.amount)                  AS total_revenue,
  AVG(o.amount)                  AS avg_order_value,
  COUNT(DISTINCT o.customer_id)  AS unique_customers
FROM catalog.silver.orders o
JOIN catalog.silver.customers c ON o.customer_id = c.customer_id
JOIN catalog.silver.products  p ON o.product_id  = p.product_id
GROUP BY o.order_date, c.region, p.category;

-- Optimize Gold table for BI queries
OPTIMIZE catalog.gold.daily_revenue ZORDER BY (order_date, region);
```

### Medallion Architecture — Layer Comparison

| Aspect | Bronze | Silver | Gold |
|---|---|---|---|
| **Data quality** | Raw, unvalidated | Cleaned, validated | Aggregated, trusted |
| **Schema** | Flexible (schema-on-read) | Enforced (schema-on-write) | Enforced, optimized |
| **Deduplication** | No | Yes (via MERGE) | Yes (result of agg) |
| **Write pattern** | Append-only | MERGE (upsert) | Overwrite or incremental |
| **Retention** | Long (full history) | Business-defined | Often short (current) |
| **Consumers** | Data engineers | Analysts, data scientists | BI tools, dashboards, ML |
| **Typical size** | Largest | Medium | Smallest |
| **Partitioning** | By ingest date | By business date | By report dimension |

### Common Medallion Anti-Patterns (Interview questions)

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Updating Bronze records | Destroys audit trail | Bronze is ALWAYS append-only |
| Skipping Bronze (direct Silver) | No raw history for debugging | Always land raw first |
| Too many Gold tables | Management overhead | Use views for minor variants |
| No MERGE key in Silver | Full re-scan instead of incremental | Define and document primary keys |
| OPTIMIZE never runs on Gold | Slow BI queries | Schedule OPTIMIZE after each Gold refresh |

---

## 12. COPY INTO — Batch File Ingestion

`COPY INTO` is a SQL command that idempotently loads files from cloud storage into a Delta table. It's simpler than Auto Loader but designed for batch (not streaming) use.

### Syntax

```sql
COPY INTO catalog.bronze.raw_events
FROM '/mnt/landing/events/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- CSV with options
COPY INTO my_table
FROM '/mnt/data/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'delimiter' = ',',
  'inferSchema' = 'true'
)
COPY_OPTIONS ('force' = 'false');  -- false = skip already-loaded files (idempotent)

-- With explicit column list
COPY INTO target_table (id, name, amount)
FROM (
  SELECT col1, col2, col3 FROM '/mnt/data/'
)
FILEFORMAT = PARQUET;
```

### COPY INTO vs Auto Loader

| Feature | COPY INTO | Auto Loader |
|---|---|---|
| **Interface** | SQL | Python (readStream) / SQL (DLT) |
| **Processing mode** | Batch (run on demand) | Streaming (continuous or triggered) |
| **File tracking** | Internal metadata store | Checkpoint directory |
| **Scale** | Up to millions of files | Billions of files (queue mode) |
| **Schema evolution** | Manual or `mergeSchema` | Automatic (configurable) |
| **Idempotent** | ✅ Yes (skips already-loaded) | ✅ Yes (checkpoint-based) |
| **Best for** | Simple batch loads, SQL-only | Continuous/incremental pipelines |
| **DLT support** | ❌ No | ✅ Yes |

> ⚠️ **Exam key point:** Both COPY INTO and Auto Loader are **idempotent** — they won't re-load files already processed. Auto Loader is recommended for large-scale or streaming scenarios. COPY INTO is simpler for SQL-based batch loads.

---

## 13. Liquid Clustering — Modern Alternative to ZORDER

Liquid Clustering is a **newer Databricks feature** (GA in 2024) that replaces static `PARTITION BY` + `ZORDER BY` with a flexible, automatically maintained clustering strategy.

### Key Advantages over ZORDER
- **Dynamic:** Can change clustering columns without rewriting the table
- **Incremental:** Only reclusters data that has changed (not full table)
- **Automatic:** Works with `OPTIMIZE` without specifying `ZORDER BY` each time
- **Multi-column:** More effective than ZORDER for multiple clustering dimensions

### Usage

```sql
-- Create table with liquid clustering
CREATE TABLE catalog.silver.orders
USING DELTA
CLUSTER BY (customer_id, order_date);  -- clustering columns

-- Add clustering to existing table
ALTER TABLE catalog.silver.orders
CLUSTER BY (customer_id, order_date);

-- Run OPTIMIZE to apply clustering (no ZORDER needed)
OPTIMIZE catalog.silver.orders;

-- Remove clustering
ALTER TABLE catalog.silver.orders CLUSTER BY NONE;

-- Check clustering info
DESCRIBE DETAIL catalog.silver.orders;
```

### Liquid Clustering vs ZORDER vs Partition

| Feature | Partition BY | ZORDER | Liquid Clustering |
|---|---|---|---|
| **Mechanism** | Folder-based | File-level colocate | Automatic file layout |
| **Change columns** | Table rewrite needed | Just change ZORDER cmd | ALTER TABLE — no rewrite |
| **Incremental** | N/A | No (full OPTIMIZE) | Yes |
| **Cardinality** | Low only | Low-medium | Low to high |
| **Recommended** | Legacy | Legacy | ✅ **Preferred (2024+)** |

> 🔑 **Interview tip:** If asked about performance optimization in modern Databricks, mention Liquid Clustering as the preferred approach over ZORDER, especially for mutable data.

---

## 14. Updated Key Exam-Focus Points

11. ✅ Know **Medallion Architecture** layers: Bronze (raw/append-only) → Silver (cleaned/MERGE) → Gold (aggregated/BI)
12. ✅ Know **Bronze is always append-only** — never update raw records
13. ✅ Know **Silver uses MERGE** to deduplicate and upsert from Bronze
14. ✅ Know **Gold is pre-aggregated** and optimized for BI/ML consumption
15. ✅ Know **COPY INTO** — SQL batch ingestion, idempotent, simpler than Auto Loader
16. ✅ Know **Auto Loader vs COPY INTO** — Auto Loader for streaming/scale; COPY INTO for SQL batch
17. ✅ Know **Liquid Clustering** — modern replacement for ZORDER + partitioning
18. ✅ Know `CLUSTER BY` syntax and that `OPTIMIZE` applies it incrementally
