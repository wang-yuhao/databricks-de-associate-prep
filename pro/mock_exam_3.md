# Databricks Certified Data Engineer Professional — Mock Exam 3
## Focus: Data Processing (30%) + Databricks Tooling (20%)
> **Exam Format**: 60 questions · 120 minutes · 70% passing score · Online proctored  
> **This file**: 20 questions covering the two highest-weight domains  
> Updated: July 2026

---

## Domain 1 — Databricks Tooling (20%)

### Q1
A data engineer needs to trigger a Databricks Job via an external CI/CD system after a code merge. Which approach is most appropriate?

A) Use the Databricks UI to manually run the job  
B) Use the Databricks REST API `POST /api/2.1/jobs/run-now` with a JSON payload containing the job ID  
C) Schedule the job with a cron expression inside a notebook  
D) Use `dbutils.notebook.run()` from a separate always-running cluster  

**✅ Answer: B**

**Explanation**: The Databricks REST API (`/api/2.1/jobs/run-now`) is the standard programmatic trigger for external CI/CD systems (Jenkins, GitHub Actions, Azure DevOps). It accepts a `job_id` and optional `notebook_params`. The Databricks CLI (`databricks jobs run-now --job-id <id>`) is a thin wrapper around the same API and is equally valid. `dbutils.notebook.run()` is for notebook-to-notebook orchestration, not external triggers. Hardcoded cron schedules cannot respond to external events.

**Key Knowledge**: Databricks Jobs API 2.1 supports multi-task jobs, task dependencies, retry policies, and timeout settings. Always prefer 2.1 over the deprecated 2.0 API.

---

### Q2
A team wants to version-control Databricks notebooks alongside application code. Which Git integration method best supports branch-based development with pull request workflows?

A) Export notebooks as `.dbc` archives to a shared drive  
B) Use Databricks Repos to connect directly to a remote Git repository (GitHub/Azure DevOps)  
C) Copy notebook source code manually into a Git repo after each change  
D) Use DBFS to store notebook JSON exports  

**✅ Answer: B**

**Explanation**: Databricks Repos provides native Git integration: you can commit, push, pull, branch, and merge from within the Databricks UI or via API. It supports GitHub, GitLab, Azure DevOps, and Bitbucket. Notebooks are stored as source files (`.py`, `.sql`, `.scala`, `.r`) enabling full diff/PR workflows. `.dbc` archives are binary-like and not diff-friendly. DBFS is object storage, not version control.

**Key Knowledge**: Databricks Repos supports sparse checkout. You can use `%run` to execute files from a Repos path. The Repos API (`/api/2.0/repos`) allows CI/CD automation (e.g., `git pull` on deploy).

---

### Q3
A data engineer uses the Databricks CLI to deploy a job definition from a JSON file. Which command correctly creates a new job?

A) `databricks jobs create --json-file job_config.json`  
B) `databricks jobs submit --json-file job_config.json`  
C) `databricks jobs run --json-file job_config.json`  
D) `databricks jobs deploy --json-file job_config.json`  

**✅ Answer: A**

**Explanation**: `databricks jobs create` creates a persistent job definition. `databricks jobs submit` creates a one-time run without saving a job definition. `databricks jobs run` is not a valid command. Understanding the distinction between `create` (persistent) and `submit` (ephemeral) is critical for production deployments where you need repeatable, versioned job definitions.

**Key Knowledge**: Use `databricks jobs reset` to update an existing job definition by replacing its full configuration. Use `databricks jobs update` (API 2.1) for partial updates. Always pin cluster runtime versions in production job configs.

---

### Q4
When configuring a Databricks cluster for a production ETL job, which cluster mode minimizes cost while supporting parallel task execution across multiple workers?

A) Single-node cluster  
B) Standard (multi-node) cluster with autoscaling disabled  
C) Standard (multi-node) cluster with autoscaling enabled (min=2, max=8 workers)  
D) High-concurrency cluster with photon disabled  

**✅ Answer: C**

**Explanation**: Autoscaling standard clusters dynamically adjust worker count based on workload, reducing cost during low-activity periods while scaling out during peak loads. Single-node clusters have no worker nodes and cannot parallelize across multiple executors. Fixed-size clusters waste resources when workload is low. For ETL, enabling **Photon** on supported runtimes further accelerates vectorized SQL processing.

**Key Knowledge**: Autoscaling works by monitoring pending tasks and active executors. The Spark autoscaler observes shuffle file availability before decommissioning nodes. Set `spark.databricks.aggressiveWindowDownS` to control scale-down aggressiveness.

---

### Q5
A team uses Databricks Asset Bundles (DABs) to deploy resources. Which file format defines the bundle configuration?

A) `bundle.json`  
B) `databricks.yml`  
C) `bundle_config.yaml`  
D) `deployment.tf`  

**✅ Answer: B**

**Explanation**: Databricks Asset Bundles use `databricks.yml` as the root configuration file. DABs enable Infrastructure-as-Code deployment of jobs, pipelines, notebooks, and clusters. The `databricks.yml` supports environment overrides (dev/staging/prod), variable substitution, and resource references. This is the modern replacement for ad-hoc Terraform or manual deployments.

**Key Knowledge**: DABs CLI commands: `databricks bundle validate`, `databricks bundle deploy`, `databricks bundle run <resource>`, `databricks bundle destroy`. Use `targets` block to define per-environment configs.

---

## Domain 2 — Data Processing (30%)

### Q6
A streaming job reads from a Kafka topic and writes to a Delta table. After a cluster restart, the job should resume from where it left off. Which checkpoint configuration ensures exactly-once semantics?

A) Set `checkpointLocation` to a temporary DBFS path like `/tmp/checkpoint`  
B) Set `checkpointLocation` to a persistent, dedicated path such as `abfss://container@storage.dfs.core.windows.net/checkpoints/job1/`  
C) Use `trigger(once=True)` without a checkpoint  
D) Use `outputMode("complete")` to reprocess all data each run  

**✅ Answer: B**

**Explanation**: A persistent, dedicated checkpoint path (typically cloud object storage like ADLS Gen2 or S3) stores Kafka offsets, write-ahead logs, and state information. After restart, Structured Streaming reads the checkpoint to resume from the exact last committed offset, providing exactly-once end-to-end guarantees with Delta Lake sinks. Using `/tmp/` risks checkpoint loss on cluster restart. `trigger(once=True)` is a micro-batch pattern but still requires a checkpoint for correctness. `complete` mode rewrites the entire output each trigger, which is inappropriate for append-only event streams.

**Key Knowledge**: Checkpoint directories must NOT be shared between streaming queries. Each query needs its own checkpoint path. The checkpoint stores: current offsets (`offsets/`), committed offsets (`commits/`), and state data (`state/`).

---

### Q7
Given the following PySpark code, what is the output behavior?

```python
from pyspark.sql import functions as F

df = spark.readStream.format("delta").table("bronze.events")

result = (df
  .withWatermark("event_time", "10 minutes")
  .groupBy(F.window("event_time", "5 minutes"), "user_id")
  .agg(F.count("*").alias("event_count"))
)

result.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/chk/user_events") \
  .table("silver.user_events_agg")
```

A) Aggregates all historical data and overwrites the silver table on each trigger  
B) Performs windowed aggregation with late-data handling; emits final window results after the watermark passes, appending to the silver table  
C) Fails because `append` mode is incompatible with aggregations  
D) Produces results only for the current 5-minute window and discards all others  

**✅ Answer: B**

**Explanation**: `withWatermark("event_time", "10 minutes")` tells Structured Streaming to hold window state until event time advances 10 minutes past the window boundary. Once the watermark passes, the window is finalized and the result is appended. `append` output mode is valid with watermarked aggregations — it emits rows only when the watermark guarantees no more late data will update them. Without a watermark, `append` mode is incompatible with aggregations (you'd need `update` or `complete`).

**Key Knowledge**: Watermark = max event_time seen − threshold. State is cleaned up after watermark passes. For windowed aggregations without watermark, use `complete` or `update` mode. `complete` rewrites all aggregated results every trigger.

---

### Q8
A data engineer needs to process CDC (Change Data Capture) data from a relational database. The source delivers records with an `operation` column containing `INSERT`, `UPDATE`, or `DELETE`. Which Delta Lake feature best supports applying these changes to a target table?

A) `COPY INTO` command  
B) `MERGE INTO` statement  
C) `INSERT OVERWRITE` with full refresh  
D) `CREATE OR REPLACE TABLE AS SELECT`  

**✅ Answer: B**

**Explanation**: `MERGE INTO` (also called UPSERT) handles all three CDC operations in a single atomic statement. You can match on a primary key and branch on `operation` type: `WHEN MATCHED AND operation = 'DELETE' THEN DELETE`, `WHEN MATCHED AND operation = 'UPDATE' THEN UPDATE SET *`, `WHEN NOT MATCHED AND operation = 'INSERT' THEN INSERT *`. This is far more efficient than full table rewrites. `COPY INTO` is for batch file ingestion, not CDC. `INSERT OVERWRITE` would lose existing records.

**Key Knowledge**:
```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET *
WHEN NOT MATCHED AND s.op = 'I' THEN INSERT *
```
Delta's MERGE uses optimistic concurrency — it reads once and applies changes atomically.

---

### Q9
A silver-layer transformation job fails intermittently with `AnalysisException: Detected a data update without a corresponding schema update`. What is the most likely cause?

A) The bronze table has too many small files  
B) A column was added or type-changed in the source bronze table, but schema evolution is not enabled in the streaming reader  
C) The checkpoint directory is full  
D) The silver table has row-level security enabled  

**✅ Answer: B**

**Explanation**: When using `readStream` on a Delta table, schema changes (new columns, type casts) in the source trigger this error if `mergeSchema` or automatic schema evolution is not configured. Fix: add `.option("mergeSchema", "true")` to the reader, or enable `spark.databricks.delta.schema.autoMerge.enabled = true`. For streaming jobs, schema changes require restarting the stream after enabling schema evolution. For critical pipelines, schema change detection should be part of a data contract monitoring system.

**Key Knowledge**: Delta Lake supports schema evolution via `mergeSchema` (write option) and `overwriteSchema` (to fully replace). For streaming, enabling `cloudFiles.schemaEvolutionMode = "addNewColumns"` with Auto Loader handles upstream schema drift automatically.

---

### Q10
Which of the following correctly demonstrates reading an Auto Loader stream with schema inference and schema evolution?

A)
```python
spark.readStream.format("delta").load("/bronze/events")
```
B)
```python
spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", "/schema/events") \
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
  .load("abfss://raw@storage.dfs.core.windows.net/events/")
```
C)
```python
spark.read.format("json") \
  .option("inferSchema", "true") \
  .load("/raw/events")
```
D)
```python
spark.readStream.format("json").load("/raw/events")
```

**✅ Answer: B**

**Explanation**: Auto Loader (`cloudFiles` format) is Databricks' optimized incremental file ingestion engine. Key options: `cloudFiles.format` specifies the underlying file format; `cloudFiles.schemaLocation` persists the inferred schema to avoid full re-inference on restart; `cloudFiles.schemaEvolutionMode = "addNewColumns"` allows new columns from upstream to flow through without failing. Auto Loader uses file notification (event-based) or directory listing mode, making it highly scalable for millions of files. Option A reads from Delta (not raw files). Options C/D use standard Spark readers without Auto Loader benefits.

**Key Knowledge**: Auto Loader maintains a RocksDB-backed file tracking state. Use `cloudFiles.maxFilesPerTrigger` to control throughput. `cloudFiles.useNotifications = true` switches to cloud event-based discovery (requires IAM setup).

---

### Q11
A data engineer must implement a slowly changing dimension Type 2 (SCD2) in Delta Lake. Which approach correctly maintains historical records?

A) Use `INSERT OVERWRITE` to replace rows when attributes change  
B) Use `MERGE INTO` with logic to close existing rows (`is_current = false`, `end_date = current_date`) and insert new rows with `is_current = true`  
C) Use `UPDATE` to modify existing rows in place  
D) Use `COPY INTO` to reload the full dimension table daily  

**✅ Answer: B**

**Explanation**: SCD Type 2 preserves full history by never updating the original row's attributes. Instead, when a dimension attribute changes: (1) mark the current row as inactive by setting `is_current = FALSE` and `valid_to = current_date()`, (2) insert a new row with the new attributes, `is_current = TRUE`, `valid_from = current_date()`, `valid_to = NULL`. Delta Lake's `MERGE INTO` supports this with `WHEN MATCHED AND <condition> THEN UPDATE` + `WHEN NOT MATCHED THEN INSERT` clauses. For large dimensions, use `replaceWhere` to partition-prune the affected segments.

**Key Knowledge**:
```sql
MERGE INTO dim_customer t
USING (SELECT * FROM staging_customer) s ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.email != s.email OR t.name != s.name) THEN 
  UPDATE SET t.is_current = false, t.valid_to = current_date()
WHEN NOT MATCHED THEN INSERT (customer_id, email, name, is_current, valid_from, valid_to)
  VALUES (s.customer_id, s.email, s.name, true, current_date(), null)
```

---

### Q12
A PySpark job processes a 500 GB dataset joined to a 2 MB lookup table. The job is slow due to shuffle. Which optimization should the engineer apply?

A) Increase the number of shuffle partitions to 2000  
B) Broadcast the 2 MB lookup table using `F.broadcast()` hint  
C) Repartition the large dataset to 10 partitions before the join  
D) Cache the large dataset before the join  

**✅ Answer: B**

**Explanation**: Broadcast join eliminates shuffle entirely by sending a copy of the small table to every executor. Spark automatically broadcasts tables under `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), but explicitly using `F.broadcast(small_df)` or `/*+ BROADCAST(table) */` SQL hint makes intent clear and overrides threshold limits. Broadcasting a 2 MB table adds negligible network overhead compared to the shuffle cost of a sort-merge join on 500 GB. Increasing shuffle partitions reduces skew but does not eliminate the shuffle itself.

**Key Knowledge**: `spark.sql.autoBroadcastJoinThreshold = -1` disables auto-broadcast. Use `EXPLAIN` to confirm the join strategy. For tables 10–200 MB, consider shuffle-hash join (`spark.sql.join.preferSortMergeJoin = false`).

---

### Q13
What does the following Delta Lake operation accomplish?

```sql
OPTIMIZE events_table ZORDER BY (user_id, event_date);
```

A) Partitions the table by `user_id` and `event_date`  
B) Rewrites data files to co-locate rows with similar `user_id` and `event_date` values, improving data skipping for queries filtering on those columns  
C) Compresses all data files using ZSTD codec  
D) Creates a secondary index on `user_id` and `event_date`  

**✅ Answer: B**

**Explanation**: `OPTIMIZE ... ZORDER BY` performs two tasks: (1) **compaction** — merges small files into larger ones (default target: 1 GB per file), and (2) **Z-ordering** — a space-filling curve technique that co-locates rows with similar values for the specified columns within each file. This allows Delta Lake's **data skipping** mechanism (powered by min/max statistics in the transaction log) to skip entire files when queries filter on those columns. Z-ordering works best with high-cardinality columns used in `WHERE` clauses. It does NOT create partitions.

**Key Knowledge**: Z-ordering is most beneficial when combined with Bloom filters for equality predicates. Use `SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '32')` to index more columns. Run `OPTIMIZE` incrementally — Delta tracks which files have already been optimized.

---

### Q14
A streaming job uses `trigger(processingTime="5 minutes")`. The upstream data arrives in bursts every hour. Which trigger type would be more cost-efficient while ensuring all data is processed?

A) Keep `processingTime="5 minutes"` — it will naturally idle when no data is available  
B) Switch to `trigger(availableNow=True)` for a scheduled batch-like run that processes all available data and terminates  
C) Switch to `trigger(once=True)` — it processes one micro-batch and stops  
D) Use `trigger(continuous="1 second")` for lower latency  

**✅ Answer: B**

**Explanation**: `trigger(availableNow=True)` (introduced in Spark 3.3 / DBR 10.4+) processes all available data across multiple micro-batches (unlike `once=True` which only processes one), then terminates. When scheduled via Databricks Jobs (e.g., hourly), it provides exactly-once semantics with cost efficiency — the cluster only runs when there is data to process. `processingTime` keeps the cluster alive continuously, wasting resources during idle periods. `continuous` mode (millisecond latency) requires a stationary running cluster and is unsuitable for hourly batch patterns.

**Key Knowledge**: `trigger(once=True)` vs `trigger(availableNow=True)`: `once` processes exactly one micro-batch; `availableNow` processes all pending data in multiple micro-batches. `availableNow` respects `maxFilesPerTrigger` and `maxBytesPerTrigger` limits.

---

### Q15
A data engineer wants to enable Change Data Feed (CDF) on an existing Delta table to track row-level changes. Which command is correct?

A) `CREATE TABLE events USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`  
B) `ALTER TABLE events SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`  
C) `OPTIMIZE events SET CHANGE_DATA_FEED = TRUE`  
D) `DESCRIBE HISTORY events` then enable CDF in the output  

**✅ Answer: B**

**Explanation**: CDF (Change Data Feed) is enabled per-table via `ALTER TABLE ... SET TBLPROPERTIES`. Once enabled, Delta tracks `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`), `_commit_version`, and `_commit_timestamp` for every DML operation. To read CDF: `spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 10).table("events")`. CDF is extremely valuable for incremental ETL pipelines that need to propagate only changed records downstream. Option A is only valid at table creation time.

**Key Knowledge**: CDF adds a `_change_data` folder alongside `_delta_log`. CDF data is retained for the same period as table history (default: 30 days). Use `DESCRIBE HISTORY` to find the version number to start reading CDF from.

---

## Domain 2 Extended — Complex Scenarios

### Q16
A gold-layer aggregation job reads from a silver Delta table with 10,000 small files (avg 1 MB each). Performance is poor. Without modifying downstream consumers, which combination of fixes best addresses this?

A) Repartition the silver table to 100 partitions and use `coalesce()` before writing  
B) Run `OPTIMIZE silver_table` to compact files, and configure `spark.sql.shuffle.partitions = 200` for the aggregation job  
C) Add more workers to the cluster  
D) Convert the silver table to Parquet format  

**✅ Answer: B**

**Explanation**: Small file problem has two layers: (1) **storage layer** — `OPTIMIZE` compacts the 10,000 × 1 MB files into larger ~1 GB files, dramatically reducing file listing overhead and metadata reads; (2) **compute layer** — `spark.sql.shuffle.partitions` controls the number of shuffle partitions in the aggregation, which should be tuned to ~2–3× the number of cores. Adding workers helps but doesn't fix the root cause. Delta format is superior to Parquet for transactional workloads.

**Key Knowledge**: Enable Auto Optimize (`delta.autoOptimize.optimizeWrite = true`, `delta.autoOptimize.autoCompact = true`) to proactively manage file sizes on write. `VACUUM` removes old files but does not compact — do not confuse with `OPTIMIZE`.

---

### Q17
In a DLT (Delta Live Tables) pipeline, what is the difference between `@dlt.table` and `@dlt.view`?

A) `@dlt.table` writes results to storage; `@dlt.view` is a temporary, non-materialized virtual table within the pipeline  
B) `@dlt.view` writes to Delta; `@dlt.table` creates a SQL view  
C) There is no functional difference; both materialize data to Delta Lake  
D) `@dlt.table` only supports batch; `@dlt.view` only supports streaming  

**✅ Answer: A**

**Explanation**: In DLT, `@dlt.table` (or `CREATE OR REFRESH LIVE TABLE`) materializes data as a managed Delta table in the pipeline's storage location. It persists across pipeline runs and is queryable outside the pipeline. `@dlt.view` (or `CREATE TEMPORARY LIVE VIEW`) is an ephemeral logical transformation within the pipeline DAG — it is not written to storage and is not accessible outside the pipeline. Use views for intermediate transformations you don't need to persist. Use tables for data you want to persist, audit, or share downstream.

**Key Knowledge**: DLT supports both Python decorators and SQL syntax. Expectations (`@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`) can be attached to tables (not views). Use `dlt.read()` to reference other DLT datasets within the pipeline.

---

### Q18
A data engineer configures a DLT expectation as follows:

```python
@dlt.table
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_transactions():
    return dlt.read("bronze_transactions")
```

What happens to records where `amount <= 0`?

A) The pipeline fails immediately  
B) Invalid records are quarantined to a separate error table  
C) Records failing the expectation are dropped from the output table; metrics are tracked in the pipeline event log  
D) Records are written to the table with a `_dlt_error` flag column  

**✅ Answer: C**

**Explanation**: `@dlt.expect_or_drop` silently removes rows violating the constraint, while recording violation counts in DLT's event log and pipeline metrics. The table receives only valid records. This contrasts with: `@dlt.expect` (records are kept, violation is logged as a warning), and `@dlt.expect_or_fail` (pipeline fails on any violation). The event log (`event_log()` function) tracks `num_output_rows`, `num_rows_dropped`, and expectation violation details per batch.

**Key Knowledge**: Access DLT event logs via `SELECT * FROM event_log('<pipeline_id>')` or the pipeline UI. Combine multiple expectations with `@dlt.expect_all`, `@dlt.expect_all_or_drop`, `@dlt.expect_all_or_fail` using a dictionary of constraint names → expressions.

---

### Q19
Which PySpark code correctly reads the latest version of a Delta table AND the version from 2 days ago for comparison?

A)
```python
current = spark.read.table("sales")
historical = spark.read.format("delta").option("timestampAsOf", "2 days ago").table("sales")
```
B)
```python
current = spark.read.table("sales")
historical = spark.read.format("delta").option("timestampAsOf", (datetime.now() - timedelta(days=2)).isoformat()).table("sales")
```
C)
```python
current = spark.read.table("sales")
historical = spark.sql("SELECT * FROM sales VERSION AS OF -2")
```
D)
```python
current = spark.read.option("version", "latest").table("sales")
historical = spark.read.option("version", -2).table("sales")
```

**✅ Answer: B**

**Explanation**: Delta Time Travel accepts an ISO 8601 timestamp string for `timestampAsOf`. Using Python's `datetime` + `timedelta` generates the correct ISO format. Option A uses a natural language string which is not supported. Option C uses invalid SQL syntax (`VERSION AS OF` requires an integer version number, not `-2`). Delta Time Travel supports: `VERSION AS OF <n>` (exact version), `TIMESTAMP AS OF '<iso_timestamp>'` (point in time). History is retained for `delta.logRetentionDuration` (default 30 days) or until `VACUUM` removes old files.

**Key Knowledge**: `DESCRIBE HISTORY tablename` lists all versions with timestamps and operation types. Version numbers are monotonically increasing integers starting from 0.

---

### Q20
A streaming job produces duplicate records due to upstream re-deliveries. The `event_id` column uniquely identifies each event. Which approach in Structured Streaming deduplicates events within a bounded time window?

A) Use `.distinct()` on the streaming DataFrame  
B) Use `.dropDuplicates(["event_id"])` without a watermark  
C) Use `.withWatermark("event_time", "1 hour").dropDuplicates(["event_id", "event_time"])` 
D) Filter duplicates using a `MERGE INTO` statement outside the streaming job  

**✅ Answer: C**

**Explanation**: `dropDuplicates` in Structured Streaming maintains state to track seen `event_id` values. Without a watermark, state grows unboundedly (memory leak). With `.withWatermark("event_time", "1 hour")`, Spark only deduplicates within the 1-hour window and purges state for events older than the watermark, bounding memory usage. The deduplication key should include both the unique ID and a time column to work correctly with the watermark. Option B would cause OOM in long-running jobs. Option D with `MERGE INTO` is a valid micro-batch pattern but requires external state management.

**Key Knowledge**: For idempotent writes to Delta Lake, use `MERGE INTO` with `WHEN NOT MATCHED THEN INSERT` on the unique key as an alternative deduplication strategy. Delta's transaction log ensures atomic writes, preventing duplicate commits from concurrent executions.

---

## Answer Key Summary

| Q | Answer | Domain |
|---|--------|--------|
| 1 | B | Tooling |
| 2 | B | Tooling |
| 3 | A | Tooling |
| 4 | C | Tooling |
| 5 | B | Tooling |
| 6 | B | Data Processing |
| 7 | B | Data Processing |
| 8 | B | Data Processing |
| 9 | B | Data Processing |
| 10 | B | Data Processing |
| 11 | B | Data Processing |
| 12 | B | Data Processing |
| 13 | B | Data Processing |
| 14 | B | Data Processing |
| 15 | B | Data Processing |
| 16 | B | Data Processing |
| 17 | A | Data Processing (DLT) |
| 18 | C | Data Processing (DLT) |
| 19 | B | Data Processing |
| 20 | C | Data Processing |

---
*Mock Exam 3 — Created July 2026 | Aligned with Databricks Certified Data Engineer Professional exam blueprint*
