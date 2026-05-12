# Day 7 — Full Review & Exam Strategy

> **Goal:** Consolidate everything from Days 1–6, identify weak spots, and simulate real exam conditions.

---

## ⏰ Day 7 Schedule

| Time | Activity |
|---|---|
| 08:00–09:00 | Review cheatsheet.md — highlight anything unsure |
| 09:00–10:30 | Mock Exam 1 (45 min) → Score → Analyze wrong answers |
| 10:30–11:00 | Drill weak topics from Exam 1 |
| 11:00–12:30 | Mock Exam 2 (45 min) → Score → Analyze |
| 12:30–13:00 | Lunch break |
| 13:00–13:30 | Drill weak topics from Exam 2 |
| 13:30–15:00 | Mock Exam 3 (45 min) → Score → Analyze |
| 15:00–16:00 | Final concept review — see Quick Hits below |
| 16:00–17:00 | Read exam-day-checklist.md, relax |

---

## 🎯 Most Heavily Tested Concepts (High Priority Review)

### Domain 1 — Databricks Platform (~20%)

**Lakehouse Architecture:**
- Lakehouse = combines data lake storage flexibility + data warehouse reliability
- Built on **Delta Lake** (open-source storage layer with ACID transactions)
- Key components: **Metastore** (Unity Catalog), **Compute** (clusters), **Storage** (cloud blob + Delta)

**Cluster Types:**
| Type | Use Case | Auto-termination |
|---|---|---|
| All-Purpose | Interactive notebooks, ad hoc | Yes (configurable) |
| Job | Automated workflows/pipelines | Yes (after job) |
| SQL Warehouse | Databricks SQL, BI tools | Yes |

**Databricks Runtime:**
- DBR = Apache Spark + optimizations + pre-installed libraries
- ML Runtime adds MLflow, scikit-learn, TensorFlow
- Photon = native vectorized query engine (C++) — faster SQL, scan-heavy workloads

**Access Modes (Unity Catalog):**
- **Single User:** Full cluster access, one user only
- **Shared:** Multiple users, Python/SQL only (no Scala/R), enforces Unity Catalog governance
- **No Isolation Shared:** Legacy mode, no UC enforcement — avoid in new deployments

---

### Domain 2 — Development & Ingestion (~25%)

**Auto Loader (cloudFiles):**
```python
# The exam LOVES Auto Loader syntax
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/path/schema") \
    .load("/path/source")

df.writeStream.format("delta") \
    .option("checkpointLocation", "/path/checkpoint") \
    .start("/path/target")
```

**Key Auto Loader facts:**
- Uses **file notification** (recommended, event-driven) or **directory listing** mode
- **Schema inference**: infers on first batch, stores at `schemaLocation`
- **Schema evolution**: use `cloudFiles.schemaEvolutionMode` = `"addNewColumns"` (default), `"rescue"`, `"failOnNewColumns"`, `"none"`
- Exactly-once semantics guaranteed with checkpointing

**DataFrame Operations to Know:**
```python
# Read formats
spark.read.format("delta").load("/path")
spark.read.format("parquet").load("/path")
spark.read.csv("/path", header=True, inferSchema=True)

# Key transformations
df.select("col1", "col2")
df.filter(col("age") > 18)                      # or df.where()
df.groupBy("dept").agg(count("*"), avg("salary"))
df.join(df2, "id", "inner")                      # inner/left/right/full/semi/anti
df.withColumn("new_col", col("a") + col("b"))
df.drop("col_to_remove")
df.dropDuplicates(["id", "date"])
df.orderBy("col", ascending=False)               # or .sort()

# Window functions
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag
w = Window.partitionBy("dept").orderBy(col("salary").desc())
df.withColumn("rank", rank().over(w))
```

**SQL in Databricks:**
```sql
-- Exam heavily tests SQL on Delta tables
SELECT dept, AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date) AS avg_sal
FROM employees;

-- MERGE (key exam topic)
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.value = s.value
WHEN NOT MATCHED THEN INSERT *;
```

---

### Domain 3 — Delta Lake Deep Dive (~20%)

**ACID Guarantee mechanisms:**
- **Atomicity**: Transaction log (JSON) records all changes as atomic commits
- **Consistency**: Schema enforcement prevents bad writes
- **Isolation**: Optimistic concurrency control (snapshot isolation)
- **Durability**: Data written to cloud storage before transaction log update

**Delta Transaction Log:**
- `_delta_log/` folder contains numbered JSON files (00000000000000000000.json, etc.)
- Every write = new commit in the log
- Checkpoint files (Parquet) created every 10 commits to speed up state reconstruction
- `DESCRIBE HISTORY table_name` shows all operations

**Time Travel:**
```sql
-- By version
SELECT * FROM orders VERSION AS OF 5;

-- By timestamp
SELECT * FROM orders TIMESTAMP AS OF '2024-01-15';

-- Python
df = spark.read.format("delta").option("versionAsOf", 5).load("/path")
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load("/path")

-- Restore
RESTORE TABLE orders TO VERSION AS OF 5;
```

**OPTIMIZE and ZORDER:**
```sql
OPTIMIZE orders;                        -- Compacts small files → 1GB target
OPTIMIZE orders ZORDER BY (customer_id, order_date);  -- Colocates related data
```
- OPTIMIZE: solves the "small files problem"
- ZORDER: multi-dimensional clustering for filter skip optimization (up to 4 cols)
- **NOT** the same as partitioning — ZORDER is within partitions

**VACUUM:**
```sql
VACUUM orders;                          -- Default: removes files older than 7 days
VACUUM orders RETAIN 0 HOURS;          -- ⚠ Removes ALL old files — breaks time travel!
SET spark.databricks.delta.retentionDurationCheck.enabled = false; -- Required for 0h
```

**Schema Operations:**
```sql
-- Schema enforcement: default, rejects incompatible schema writes
-- Schema evolution: must opt in
df.write.option("mergeSchema", "true").mode("append").saveAsTable("t")  -- Add new cols
ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')  -- For col renames
ALTER TABLE t DROP COLUMN old_col                                        -- After column mapping
```

**Change Data Feed (CDF):**
```sql
ALTER TABLE orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

-- Read changes
SELECT * FROM table_changes('orders', 5)           -- from version 5
SELECT * FROM table_changes('orders', '2024-01-01')  -- from timestamp
-- _change_type values: 'insert', 'update_preimage', 'update_postimage', 'delete'
```

---

### Domain 4 — Structured Streaming (~20%)

**Trigger Types (MEMORIZE):**
| Trigger | Syntax | Behavior |
|---|---|---|
| Default (micro-batch) | no trigger | Processes new data ASAP, then waits |
| Fixed interval | `Trigger.ProcessingTime("5 minutes")` | Runs every N seconds/minutes |
| Once | `Trigger.Once()` | Processes all available data, then stops. **DEPRECATED** |
| Available Now | `Trigger.AvailableNow()` | Processes all available data in multiple batches, then stops |
| Continuous | `Trigger.Continuous("1 second")` | ~ms latency, limited operations |

⚠ **Exam trap:** `Trigger.Once()` is deprecated — prefer `Trigger.AvailableNow()`.

**Streaming Write Modes:**
| Mode | Use Case |
|---|---|
| `append` | New rows only (no updates/deletes). Default for streams. |
| `complete` | Entire result table rewritten each trigger. Only with aggregations. |
| `update` | Only changed rows. With aggregations or without. |

**Watermarking:**
```python
# Handle late-arriving data
df.withWatermark("event_time", "10 minutes") \
  .groupBy(window(col("event_time"), "5 minutes")) \
  .count()
```
- Watermark = max event_time seen - threshold
- Data arriving later than watermark is dropped

**Checkpointing:**
```python
df.writeStream \
  .option("checkpointLocation", "/checkpoint/path") \
  .start()
```
- Required for fault tolerance and exactly-once semantics
- Stores progress (offsets) in checkpoint location
- Must use **different** checkpoint location per stream

**foreachBatch:**
```python
def process_batch(df, epoch_id):
    df.write.format("delta").mode("overwrite").saveAsTable("target")

df.writeStream.foreachBatch(process_batch).start()
```
- Allows arbitrary batch operations on each micro-batch
- Enables writing to multiple sinks

---

### Domain 5 — DLT + Workflows + CI/CD (~20%)

**Delta Live Tables (DLT) Syntax:**
```python
import dlt
from pyspark.sql.functions import *

# Streaming table (append-only source)
@dlt.table
def bronze_orders():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/landing/orders")

# Materialized view (batch, can include aggregations)
@dlt.table
def silver_orders():
    return dlt.read("bronze_orders").filter(col("status") != "cancelled")

# With expectations (data quality)
@dlt.table
@dlt.expect("valid_id", "order_id IS NOT NULL")          # warn, keep
@dlt.expect_or_drop("valid_amount", "amount > 0")         # warn, drop row
@dlt.expect_or_fail("no_dupes", "COUNT(DISTINCT id) = COUNT(id)")  # fail pipeline
def gold_orders():
    return dlt.read("silver_orders")
```

**DLT Pipeline Modes:**
| Mode | Behavior |
|---|---|
| Triggered | Runs once, updates tables, then stops |
| Continuous | Keeps running, low latency (seconds) |

**DLT Development vs Production:**
- Development: reuses cluster, faster iteration
- Production: dedicated cluster, full restart on failure

**Databricks Workflows (Jobs):**
- **Task types:** Notebook, Python script, JAR, DLT pipeline, dbt, SQL
- **Dependencies:** task → task with `depends_on`
- **Trigger types:** Scheduled (cron), File arrival, Continuous
- **Retry policies:** per-task retries, timeouts
- **Repair runs:** re-run only failed/skipped tasks — preserves successful task results

**DABs (Databricks Asset Bundles):**
```yaml
# databricks.yml
bundle:
  name: my_pipeline

resources:
  jobs:
    my_job:
      name: "My Job"
      tasks:
        - task_key: notebook_task
          notebook_task:
            notebook_path: ./notebooks/my_notebook
```
- `databricks bundle deploy` — deploys to workspace
- `databricks bundle run my_job` — runs deployed job
- Enables GitOps / CI/CD for Databricks resources

---

### Domain 6 — Unity Catalog (~15%)

**Three-Level Namespace:**
```sql
catalog.schema.table
-- e.g.:
SELECT * FROM main.sales.orders;
```

**Object Hierarchy:**
```
Metastore (one per region/account)
  └── Catalog
        └── Schema (= Database)
              ├── Table (managed or external)
              ├── View
              ├── Volume (files)
              └── Function
```

**Managed vs External Tables:**
| | Managed | External |
|---|---|---|
| Data location | UC-managed storage | Your cloud storage |
| DROP TABLE | Deletes data + metadata | Deletes metadata only |
| UNDROP | ✅ Yes | ❌ No |
| Use case | Normal tables | When data must live elsewhere |

**Privilege Grants:**
```sql
-- Grant
GRANT SELECT ON TABLE main.sales.orders TO `user@company.com`;
GRANT USE CATALOG ON CATALOG main TO `analyst_group`;
GRANT CREATE TABLE ON SCHEMA main.sales TO `data_engineers`;

-- Revoke
REVOKE SELECT ON TABLE main.sales.orders FROM `user@company.com`;

-- Show grants
SHOW GRANTS ON TABLE main.sales.orders;
```

**Row and Column Filters:**
```sql
-- Row filter (dynamic filter based on current user)
CREATE FUNCTION region_filter(region_col STRING)
  RETURN region_col = current_user_region(); -- custom function

ALTER TABLE sales.orders SET ROW FILTER region_filter ON (region);

-- Column mask (hide sensitive data)
CREATE FUNCTION mask_email(email STRING)
  RETURN CASE WHEN is_account_group_member('admin') THEN email ELSE '***' END;

ALTER TABLE users ALTER COLUMN email SET MASK mask_email;
```

**Delta Sharing:**
- Share data **across organizations** without copying
- Recipient gets a token/URL, reads via Delta Sharing protocol
- Provider keeps full control, can revoke access
- Works cross-cloud and cross-platform (Pandas, Spark, etc.)

---

## ⚡ Quick Hits — Common Exam Traps

1. **`VACUUM` removes time travel history** — default retention 7 days. Set before vacuuming if you need longer history.
2. **`Trigger.Once()` is deprecated** — use `Trigger.AvailableNow()` instead.
3. **DLT `@dlt.table` = streaming by default for streaming sources** — use `@dlt.view` for temporary, non-materialized.
4. **`mergeSchema=True` adds columns; `overwriteSchema=True` replaces the schema entirely.**
5. **Shared access mode = required for Unity Catalog row/column filters** in multi-user scenarios.
6. **OPTIMIZE doesn't run automatically** — must be scheduled or triggered manually (or via Auto Optimize table property).
7. **`DESCRIBE HISTORY`** shows version, timestamp, operation, operationParameters for all commits.
8. **`DESCRIBE DETAIL`** shows file stats, location, format, number of files, size.
9. **Auto Loader vs COPY INTO:** Auto Loader = streaming, scalable, idempotent. COPY INTO = SQL-based, batch, also idempotent.
10. **CDF `_change_type`:** updates produce TWO rows: `update_preimage` (old) + `update_postimage` (new).
11. **DLT expectations:** `expect` = warn only. `expect_or_drop` = drop bad rows. `expect_or_fail` = stop pipeline.
12. **`RESTORE` is a privileged operation** — needs RESTORE privilege or table ownership.
13. **Unity Catalog `DROP TABLE` on managed table = data deleted permanently** (no recycle bin after metastore drop).
14. **Photon is only available on clusters using Databricks Runtime (not open-source Spark).**
15. **Z-Ordering is NOT partitioning** — doesn't change folder structure, works within each partition.

---

## 📊 Scoring Strategy

- **Passing score:** ~70% (about 32/45 questions) — exact threshold not published
- **Flag uncertain questions** and return to them at the end
- **Time math:** 90 min / 45 questions = 2 min per question. If stuck > 2 min, mark and move on.
- **Elimination strategy:** on 4-option MCQ, eliminate 2 wrong answers first, then choose between remaining
- **SQL > Python:** When both SQL and PySpark options look correct, prefer the SQL answer for Databricks context
- **"Best practice" questions:** Databricks recommends Unity Catalog, Workflows, DLT, Auto Loader — favor these over legacy alternatives
