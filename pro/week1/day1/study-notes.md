# Day 1 — Advanced Delta Lake & Optimistic Concurrency (~22% + 13% of exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day1_advanced_delta.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review 15 practice questions on Delta internals

---

## 1.1 Delta Lake Transaction Log (Deep Dive)

The `_delta_log/` directory is the source of truth for all Delta operations.

```
table_path/
  _delta_log/
    00000000000000000000.json   ← CREATE TABLE
    00000000000000000001.json   ← INSERT
    00000000000000000002.json   ← UPDATE
    00000000000000000010.checkpoint.parquet  ← every 10 commits
  part-00000-....parquet
```

### Transaction Log Entry Structure
Each JSON entry contains **actions**:
- `add` — file added to table
- `remove` — file logically deleted (soft delete)
- `commitInfo` — metadata (operation, timestamp, user)
- `metaData` — schema changes
- `protocol` — reader/writer version requirements

### Checkpointing
- Every **10 commits** Delta writes a Parquet checkpoint
- Checkpoint consolidates all prior log entries → faster reads
- You can force a checkpoint: `DeltaTable.forPath(spark, path).toDF()` then `OPTIMIZE`
- `_last_checkpoint` file tracks the latest checkpoint version

---

## 1.2 Optimistic Concurrency Control (OCC)

Delta Lake uses **Optimistic Concurrency Control** — assumes conflicts are rare.

### Protocol
1. **Read** the current table version (snapshot)
2. **Write** locally (stage changes)
3. **Validate** — check if another writer has committed since your read
4. **Commit** atomically if no conflict, else **retry** or **fail**

### Conflict Detection Rules
| Operation A | Operation B | Conflict? |
|-------------|-------------|----------|
| Append | Append | No |
| Update (different partitions) | Update | No |
| Update (same partition) | Update | Yes |
| Delete | Read | No |
| Schema change | Any write | Yes |
| Overwrite | Any | Yes |

### Isolation Levels in Delta
- **Serializable** (default for UPDATE/DELETE/MERGE) — strongest
- **WriteSerializable** (default for streaming writes) — allows blind appends to proceed concurrently

```sql
-- Check isolation level
DESCRIBE DETAIL training.prep.my_table;

-- Set isolation level
ALTER TABLE training.prep.my_table
SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable');
```

### Concurrent Write Scenarios (Exam Focus)
```
Writer A: UPDATE WHERE region = 'EU'
Writer B: UPDATE WHERE region = 'US'
→ No conflict (different data ranges, Spark can detect via stats)

Writer A: OPTIMIZE (rewrites files)
Writer B: UPDATE
→ Potential conflict — Delta resolves via transaction log re-read
```

---

## 1.3 Delta Clone

### Shallow Clone
- Creates a NEW Delta table that **references** the source data files (no copy)
- Independent transaction log
- Writes to clone do NOT affect source
- Source files must remain accessible

```sql
CREATE TABLE training.prep.orders_clone_shallow
SHALLOW CLONE training.prep.orders;

-- Clone at specific version (time travel)
CREATE TABLE training.prep.orders_v5_clone
SHALLOW CLONE training.prep.orders VERSION AS OF 5;
```

### Deep Clone
- **Copies** all data files + transaction log to a new location
- Fully independent — source can be deleted
- Use for: backups, migrations, environment promotion

```sql
CREATE TABLE training.prep.orders_backup
DEEP CLONE training.prep.orders
LOCATION '/Volumes/training/prep/landing/orders_backup';

-- Incremental deep clone (sync changes)
CREATE OR REPLACE TABLE training.prep.orders_backup
DEEP CLONE training.prep.orders;
```

### Clone Use Cases
| Use Case | Clone Type |
|----------|------------|
| Testing/dev environment | Shallow |
| Disaster recovery backup | Deep |
| Migrating to new catalog | Deep |
| Reproducing a pipeline bug | Shallow (point-in-time) |
| Blue/green deployment | Shallow |

---

## 1.4 Delta Lake Indexing & Performance

### Partitioning
- Partition by **low-cardinality** columns (e.g., `date`, `country`, `status`)
- Rule of thumb: each partition ≥ 1 GB of data
- **Never** partition by high-cardinality columns (user_id, UUID)
- Use `PARTITION BY` at CREATE TABLE time

```sql
CREATE TABLE training.prep.events (
  event_id BIGINT,
  user_id  STRING,
  event_type STRING,
  event_date DATE
) USING DELTA
PARTITION BY (event_date);
```

### ZORDER (Z-Ordering / Multi-Dimensional Clustering)
- Colocates related data in same files
- Works **within** partitions
- Best for high-cardinality columns used in WHERE filters

```sql
OPTIMIZE training.prep.events
ZORDER BY (user_id, event_type);
```

### Bloom Filters
- Probabilistic data structure: fast "not present" checks
- Ideal for equality filters on string/binary columns
- No false negatives; small false positive rate

```sql
CREATE TABLE training.prep.users (
  user_id STRING,
  email   STRING,
  region  STRING
) USING DELTA
TBLPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '32',
  'delta.bloomFilter.user_id.enabled' = 'true',
  'delta.bloomFilter.user_id.fpp' = '0.1',
  'delta.bloomFilter.user_id.numItems' = '10000000'
);
```

### File Size Optimization
```sql
-- Target file size (default 128 MB)
ALTER TABLE training.prep.events
SET TBLPROPERTIES ('delta.targetFileSize' = '134217728');

-- Auto-optimize: auto compaction + optimized writes
ALTER TABLE training.prep.events
SET TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
```

---

## 1.5 Liquid Clustering (New in DBR 13.3+)

Replaces partitioning + ZORDER with a more flexible approach:
- Data is clustered by specified columns automatically
- Incremental clustering (only rewrites what's needed)
- Can change cluster columns without rewriting entire table

```sql
CREATE TABLE training.prep.events_clustered (
  event_id BIGINT,
  user_id  STRING,
  event_date DATE
) USING DELTA
CLUSTER BY (event_date, user_id);

-- Trigger clustering
OPTIMIZE training.prep.events_clustered;

-- Change cluster columns (no full rewrite)
ALTER TABLE training.prep.events_clustered
CLUSTER BY (user_id, event_type);
```

---

## 1.6 Databricks SQL Optimisations

```sql
-- Enable predictive I/O (serverless DBSQL)
SET spark.databricks.delta.preview.enabled = true;

-- Photon engine (automatically used on Photon-enabled clusters)
-- Check if Photon is active:
SELECT version();
-- Look for 'Photon' in the runtime info

-- Stats collection for better query planning
ANALYZE TABLE training.prep.events COMPUTE STATISTICS;
ANALYZE TABLE training.prep.events COMPUTE STATISTICS FOR COLUMNS user_id, event_date;
```

---

## Key Exam Points ✔️

- Delta uses **optimistic concurrency** — writes conflict only when they touch the same data
- **Shallow clone** references source files; **deep clone** copies them
- ZORDER works **within** partitions; partition pruning happens **before** ZORDER
- Bloom filters are for **equality** filters, not range filters
- **Liquid Clustering** replaces partition+ZORDER; supports incremental clustering
- `delta.isolationLevel = 'Serializable'` is strongest; `WriteSerializable` allows concurrent blind appends
- Checkpoint every 10 commits; stores in Parquet format
- `commitInfo` in transaction log records operation, user, timestamp
