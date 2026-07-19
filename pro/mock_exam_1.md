# Databricks Certified Data Engineer Professional — Mock Exam 1

> **Format:** 59 scored multiple-choice questions | **Time:** 120 minutes | **Pass:** ≥70% (≥42/59) | **Cost:** $200 USD  
> **Exam Version:** Updated November 30, 2025

---

## Domain Distribution
| Domain | Weight | Questions |
|---|---|---|
| 1. Databricks Tooling | 20% | ~12 |
| 2. Data Processing | 30% | ~18 |
| 3. Data Modeling | 20% | ~12 |
| 4. Security & Governance | 10% | ~6 |
| 5. Monitoring & Logging | 10% | ~6 |
| 6. Testing & Deployment | 10% | ~6 |

---

## Section 1 — Databricks Tooling (Questions 1–12)

### Q1
A data engineering team needs to run a notebook-based pipeline that executes three notebooks sequentially. Notebook B depends on the output of Notebook A, and Notebook C depends on Notebook B. Which Databricks feature is **most appropriate** for orchestrating this workflow?

- A) Delta Live Tables with a continuous pipeline
- B) Databricks Jobs with task dependencies
- C) Databricks Repos with CI/CD triggers
- D) MLflow with experiment tracking

**✅ Answer: B**  
**Explanation:** Databricks Jobs support multi-task workflows with explicit task dependencies (sequential, parallel, or branching). You define tasks and set upstream/downstream dependencies. Delta Live Tables (A) is for declarative streaming/batch ETL pipelines, not general notebook orchestration. Repos (C) handles version control. MLflow (D) is for ML experiment tracking, not pipeline orchestration.  
**Key Concepts:** Databricks Jobs, task dependencies, `depends_on`, DAG-based workflows.

---

### Q2
You are using Databricks Repos to manage your data pipeline code. After merging a feature branch into `main`, you want to ensure the production job always uses the latest `main` branch commit. What is the **best practice**?

- A) Hard-code the commit SHA in the job definition
- B) Set the job's notebook path to track the `main` branch in Repos
- C) Manually copy notebooks after each merge
- D) Use MLflow model registry to version notebooks

**✅ Answer: B**  
**Explanation:** Databricks Repos integrates with Git providers (GitHub, GitLab, Azure DevOps). When a job references a notebook in a Repo folder pinned to a branch (e.g., `main`), it automatically uses the latest commit of that branch. Hard-coding a SHA (A) defeats the purpose of branch tracking. Manual copying (C) is error-prone and not scalable. MLflow (D) is for ML model versioning, not notebooks.  
**Key Concepts:** Databricks Repos, Git integration, branch-based deployment, CI/CD.

---

### Q3
A Databricks cluster is configured with autoscaling between 2 and 8 worker nodes. A job runs for 30 minutes. For the first 10 minutes, the cluster uses 2 workers; for the next 20 minutes, it scales up to 8 workers. How is the **DBU consumption** calculated for this job?

- A) 8 workers × 30 minutes of DBUs
- B) (2 workers × 10 min + 8 workers × 20 min) / 60 × DBU rate per node-hour
- C) Average workers (5) × 30 minutes of DBUs
- D) Only the driver node DBUs are counted for autoscaling clusters

**✅ Answer: B**  
**Explanation:** DBUs are billed based on actual compute consumed. With autoscaling, billing is proportional to the time each number of workers was active. Formula: (2 × 10/60) + (8 × 20/60) = 0.33 + 2.67 = 3 node-hours × DBU rate. Autoscaling does not average or bill for maximum capacity (A, C). The driver is billed separately but not the only node counted (D).  
**Key Concepts:** DBU billing, autoscaling, compute cost optimization, node-hours.

---

### Q4
Which cluster mode in Databricks is **most suitable** for running multiple concurrent, isolated jobs without sharing compute resources between users?

- A) Single Node cluster
- B) Standard (Shared) cluster in multi-user mode
- C) Job cluster (one per job run)
- D) High Concurrency cluster with Table ACLs

**✅ Answer: C**  
**Explanation:** Job clusters are created at the start of a job run and terminated when the job completes. Each job gets its own isolated environment. This is the recommended approach for production workloads to ensure isolation and reproducibility. Single Node (A) is for single-machine workloads. Shared clusters (B, D) allow multiple users but share compute. For isolation per job, job clusters are best practice.  
**Key Concepts:** Job cluster vs. all-purpose cluster, cluster isolation, compute lifecycle.

---

### Q5
A data engineer wants to share a Databricks dashboard with a business stakeholder who does not have a Databricks account. What is the **correct approach**?

- A) Export the dashboard as a PDF and email it
- B) Use Databricks SQL to publish the dashboard and share a public link
- C) Create a Databricks token and share it with the stakeholder
- D) Schedule a job that emails query results via SMTP

**✅ Answer: B**  
**Explanation:** Databricks SQL dashboards support published (public) links that allow viewing without a Databricks login, enabling sharing with external stakeholders. Exporting as PDF (A) is static and not interactive. Sharing tokens (C) gives full API access — a major security risk. SMTP jobs (D) require custom code and lack interactivity.  
**Key Concepts:** Databricks SQL dashboards, published dashboards, stakeholder sharing, access without credentials.

---

### Q6
A team uses Databricks Workflows to run an ETL job nightly. The job fails at the data transformation task. What is the **most efficient** way to re-run **only the failed task** without re-running the ingestion task?

- A) Re-trigger the entire job run from the beginning
- B) Use the "Repair Run" feature to re-run only failed tasks
- C) Manually re-execute the transformation notebook
- D) Delete the failed run and create a new job

**✅ Answer: B**  
**Explanation:** Databricks Workflows "Repair Run" allows you to re-run only the failed (or skipped) tasks in a workflow, preserving the outputs of successful tasks. This saves time and cost by avoiding redundant re-execution of passed tasks. Re-triggering the whole job (A) wastes resources. Manual notebook execution (C) bypasses job orchestration and logging. Deleting runs (D) loses audit history.  
**Key Concepts:** Repair Run, task retry, Databricks Workflows, partial re-runs.

---

### Q7
When configuring a Databricks Job cluster, which setting ensures the cluster is **automatically terminated** if it is idle for more than 20 minutes?

- A) Set `max_inactivity_min = 20` in the cluster policy
- B) Enable auto-termination with a timeout of 20 minutes on the cluster config
- C) Job clusters always auto-terminate after completion; no config is needed
- D) Configure a separate job to call the Clusters API to terminate idle clusters

**✅ Answer: C**  
**Explanation:** Job clusters automatically terminate when the job run completes — they are ephemeral by design. The auto-termination idle timeout is relevant for **all-purpose clusters**, not job clusters. Job clusters do not persist beyond the job run. This is a key distinction in the exam. For all-purpose clusters, auto-termination timeout is set in cluster settings (B applies to all-purpose clusters, not job clusters).  
**Key Concepts:** Job cluster lifecycle, auto-termination, all-purpose vs. job clusters.

---

### Q8
A data engineer needs to pass a secret (API key) to a Databricks notebook without exposing it in plaintext. What is the **recommended approach**?

- A) Store the key in a Delta table with restricted access
- B) Hardcode the key in the notebook and restrict notebook access
- C) Use Databricks Secrets with `dbutils.secrets.get(scope, key)`
- D) Store the key as a cluster environment variable visible in the UI

**✅ Answer: C**  
**Explanation:** Databricks Secrets is the purpose-built solution for managing sensitive credentials. Secrets are stored in a scope (backed by Databricks or Azure Key Vault), never shown in plaintext in logs or the UI, and accessed via `dbutils.secrets.get(scope="my-scope", key="api-key")`. Storing in Delta (A) doesn't prevent plaintext exposure. Hardcoding (B) is a security anti-pattern. Environment variables (D) can be visible in cluster configuration views.  
**Key Concepts:** Databricks Secrets, `dbutils.secrets`, secret scopes, Azure Key Vault integration, security best practices.

---

### Q9
What is the **primary purpose** of a Databricks Cluster Policy?

- A) To restrict which users can view Delta table data
- B) To enforce cost controls and configuration constraints on cluster creation
- C) To define the scheduling frequency of Databricks Jobs
- D) To specify the partitioning strategy for Delta tables

**✅ Answer: B**  
**Explanation:** Cluster Policies allow administrators to define allowed values, defaults, and hidden/fixed attributes for cluster configurations (e.g., max workers, node types, autoscaling bounds, DBU limits). This enforces governance and cost control, preventing users from spinning up oversized clusters. It has nothing to do with table ACLs (A), job scheduling (C), or Delta partitioning (D).  
**Key Concepts:** Cluster Policy, cost governance, cluster configuration constraints, admin control.

---

### Q10
A Databricks notebook uses `%run ./utils` to call a helper notebook. What is the **correct behavior** of `%run`?

- A) It runs the helper notebook in a separate isolated cluster
- B) It imports the helper notebook as a Python module
- C) It executes the helper notebook in the same cell execution context, sharing variables
- D) It schedules the helper notebook as a dependent job task

**✅ Answer: C**  
**Explanation:** `%run` executes another notebook inline within the calling notebook's execution context. All variables, functions, and imports defined in the called notebook become available in the calling notebook after `%run`. This is different from `dbutils.notebook.run()`, which runs a notebook in a separate context and returns a string result. The `%run` command does not create a new cluster (A), create a Python module (B), or schedule a job (D).  
**Key Concepts:** `%run`, notebook execution context, variable sharing, vs. `dbutils.notebook.run()`.

---

### Q11
Which Databricks SQL statement is used to **view metadata** about a Delta table including schema, location, and properties?

- A) `SHOW TABLES LIKE 'my_table'`
- B) `DESCRIBE EXTENDED my_table`
- C) `SELECT * FROM information_schema.tables WHERE table_name = 'my_table'`
- D) `EXPLAIN my_table`

**✅ Answer: B**  
**Explanation:** `DESCRIBE EXTENDED table_name` (or `DESCRIBE DETAIL table_name` for Delta-specific stats) provides comprehensive metadata: column names, data types, nullability, table location, file format, creation time, table properties, partition columns, and Delta-specific statistics. `SHOW TABLES` (A) lists tables in a schema. `information_schema` (C) gives catalog metadata but not Delta properties. `EXPLAIN` (D) shows the query execution plan.  
**Key Concepts:** `DESCRIBE EXTENDED`, `DESCRIBE DETAIL`, Delta metadata, table properties.

---

### Q12
A data engineer wants to programmatically trigger a Databricks Job from an external CI/CD pipeline (e.g., GitHub Actions). What is the **correct method**?

- A) Use `dbutils.jobs.run()` from within a notebook
- B) Call the Databricks REST API endpoint `POST /api/2.1/jobs/run-now`
- C) Use the Databricks CLI command `databricks clusters start`
- D) Trigger via Databricks SQL query `RUN JOB job_id`

**✅ Answer: B**  
**Explanation:** The Databricks REST API (`POST /api/2.1/jobs/run-now`) is the standard programmatic interface for triggering job runs from external systems. GitHub Actions, Jenkins, and other CI/CD tools use this endpoint with a Databricks PAT (Personal Access Token). `dbutils.jobs.run()` does not exist (A). `databricks clusters start` starts a cluster, not a job (C). There is no `RUN JOB` SQL command (D).  
**Key Concepts:** Databricks REST API, Jobs API, CI/CD integration, `run-now`, PAT authentication.

---

## Section 2 — Data Processing (Questions 13–30)

### Q13
A streaming pipeline reads from a Kafka topic into a Delta table using Spark Structured Streaming. After an unexpected cluster restart, the pipeline re-starts. What guarantees that **no data is lost or duplicated**?

- A) Kafka's at-most-once delivery guarantee
- B) The checkpoint location storing stream progress and offsets
- C) Delta table's ACID transactions
- D) Spark's in-memory RDD lineage

**✅ Answer: B**  
**Explanation:** Structured Streaming uses a **checkpoint directory** (specified via `.option("checkpointLocation", path)`) to persist the stream's progress: the last-processed offsets, committed data, and pending transactions. On restart, the stream resumes from the last checkpoint, ensuring exactly-once semantics combined with Delta's atomic commits (C adds durability but alone doesn't track offset progress). Kafka's delivery guarantee (A) is about broker-to-consumer, not end-to-end. RDD lineage (D) is a batch concept.  
**Key Concepts:** Checkpoint location, exactly-once processing, Structured Streaming recovery, offset management.

---

### Q14
In Databricks Delta Live Tables (DLT), what is the difference between a **Streaming Table** and a **Materialized View**?

- A) Streaming Tables process only new data incrementally; Materialized Views recompute all data on each refresh
- B) Materialized Views are faster; Streaming Tables are for small datasets only
- C) Streaming Tables require Kafka; Materialized Views work with Delta tables only
- D) There is no practical difference; both process data identically

**✅ Answer: A**  
**Explanation:** In DLT, **Streaming Tables** (`CREATE OR REFRESH STREAMING TABLE`) process only newly appended data since the last run — ideal for append-only sources (Kafka, Auto Loader, Event Hubs). **Materialized Views** (`CREATE OR REFRESH MATERIALIZED VIEW`) recompute results from source data on each pipeline run, supporting updates and deletes (SCD patterns). Neither requires Kafka specifically (C). Performance depends on workload, not type (B). They are fundamentally different processing models (D).  
**Key Concepts:** DLT Streaming Tables, DLT Materialized Views, incremental vs. full recompute, DLT pipeline modes.

---

### Q15
A data engineer writes the following PySpark code:
```python
df = spark.read.format("delta").load("/mnt/bronze/events")
df_filtered = df.filter(col("event_date") >= "2024-01-01")
df_agg = df_filtered.groupBy("region").agg(sum("revenue").alias("total_revenue"))
df_agg.write.format("delta").mode("overwrite").save("/mnt/silver/revenue")
```
At which point does Spark **actually execute** the computation?

- A) When `df_filtered` is created
- B) When `df_agg` is created
- C) When `.write...save()` is called
- D) When `spark.read` is called

**✅ Answer: C**  
**Explanation:** Spark uses **lazy evaluation**. Transformations (`filter`, `groupBy`, `agg`) build a logical plan but do not trigger computation. Only **actions** (like `.write`, `.collect()`, `.show()`, `.count()`) trigger actual execution. The DAG is optimized by Catalyst and executed by Tungsten only when an action is called. This is a core Spark concept frequently tested.  
**Key Concepts:** Lazy evaluation, transformations vs. actions, Catalyst optimizer, DAG execution.

---

### Q16
What does the **`MERGE INTO`** statement in Delta Lake accomplish that a standard `INSERT` or `UPDATE` cannot?

- A) It inserts rows faster by bypassing ACID constraints
- B) It atomically handles insert, update, and delete operations based on a matching condition in a single statement
- C) It merges two Delta table schemas automatically
- D) It partitions data during the merge operation

**✅ Answer: B**  
**Explanation:** `MERGE INTO` (also called upsert) enables CDC (Change Data Capture) patterns by matching source and target rows on a key, then applying `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`, or `WHEN MATCHED AND condition THEN DELETE` — all atomically. It does not bypass ACID (A — it fully supports ACID). Schema merge is a separate `mergeSchema` option (C). Partitioning is unrelated to MERGE (D).  
**Key Concepts:** MERGE INTO, upsert, CDC, SCD Type 1, atomic DML, Delta ACID transactions.

---

### Q17
A DLT pipeline is defined with `@dlt.expect("valid_revenue", "revenue > 0")`. What happens when a record with `revenue = -5` is processed?

- A) The pipeline fails immediately
- B) The record is dropped silently
- C) The record is allowed through but the constraint violation is recorded in the event log
- D) The record is written to a quarantine table automatically

**✅ Answer: C**  
**Explanation:** `@dlt.expect()` is a **warning-level** constraint — violations are tracked in the DLT event log (pipeline metrics) but data still flows through. To **drop** invalid records, use `@dlt.expect_or_drop()`. To **fail** the pipeline, use `@dlt.expect_or_fail()`. Automatic quarantine tables (D) are not built-in behavior; they require explicit `@dlt.expect_or_drop()` with a separate quarantine table definition.  
**Key Concepts:** DLT expectations, `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`, data quality monitoring.

---

### Q18
A data engineer needs to read a large CSV file stored in ADLS Gen2 into a Delta table, handling schema evolution and new files automatically. Which approach is **most appropriate**?

- A) `spark.read.csv()` with `inferSchema=True` run as a batch job
- B) Auto Loader with `cloudFiles` format and `schemaEvolutionMode = "addNewColumns"`
- C) COPY INTO with `FORCE = TRUE` to re-read all files each time
- D) A manual `INSERT INTO` after reading each file with `pandas.read_csv()`

**✅ Answer: B**  
**Explanation:** **Auto Loader** (`cloudFiles`) is Databricks' optimized incremental file ingestion mechanism. It tracks which files have been processed (via checkpoints), handles schema inference and evolution (`schemaEvolutionMode`), and scales to millions of files. Batch `spark.read.csv()` (A) re-reads all files and doesn't track new arrivals. `COPY INTO` (C) with `FORCE=TRUE` reprocesses all files, losing idempotency. Pandas (D) doesn't scale and bypasses Spark optimization.  
**Key Concepts:** Auto Loader, `cloudFiles`, schema evolution, incremental ingestion, file tracking, ADLS Gen2.

---

### Q19
Which of the following correctly describes the behavior of `spark.readStream.format("delta").load(path)` when used as a **streaming source**?

- A) It reads the entire Delta table on every micro-batch
- B) It reads only new rows appended since the last checkpoint
- C) It reads rows in descending order of the commit timestamp
- D) It requires the Delta table to have a watermark column defined

**✅ Answer: B**  
**Explanation:** When a Delta table is used as a streaming source, Structured Streaming reads new rows appended since the last processed version (tracked via checkpoint). It uses Delta's transaction log to determine which files contain new data, not a full table scan. It does not re-read the entire table (A), does not guarantee ordering by commit time without explicit sorting (C), and does not require a watermark (D — watermarks are needed for event-time windowing with late data).  
**Key Concepts:** Delta as streaming source, incremental reads, transaction log, checkpoint-based tracking.

---

### Q20
A PySpark job processes a 1TB dataset. The job has a shuffle stage that creates 200 shuffle partitions (default). Performance is slow. What is the **most likely cause** and **best fix**?

- A) Too many shuffle partitions; reduce to 10 using `spark.conf.set("spark.sql.shuffle.partitions", 10)`
- B) Too few shuffle partitions for 1TB; increase to 2000 using `spark.conf.set("spark.sql.shuffle.partitions", 2000)`
- C) Shuffle partitions don't affect performance; the issue is the input read format
- D) Use `spark.sql.adaptive.enabled = true` to let AQE automatically determine optimal shuffle partitions

**✅ Answer: D**  
**Explanation:** **Adaptive Query Execution (AQE)** (enabled by default in Databricks Runtime ≥ 7.3) dynamically optimizes shuffle partitions at runtime based on actual data statistics, including coalescing small partitions. For 1TB, 200 partitions may be too few (B is partially correct in principle, but AQE handles this automatically). AQE also handles skew join optimization and dynamic partition pruning. The best practice is to enable AQE and let it decide rather than hardcoding partition counts.  
**Key Concepts:** AQE, `spark.sql.adaptive.enabled`, shuffle partitions, `spark.sql.shuffle.partitions`, performance tuning.

---

### Q21
A pipeline uses `COPY INTO` to load JSON files from cloud storage into a Delta table. The same files are accidentally loaded twice. What happens?

- A) Duplicate records are inserted into the Delta table
- B) The second load is silently ignored because `COPY INTO` is idempotent
- C) An error is thrown because Delta does not allow duplicate keys
- D) The table is overwritten with only the second load's data

**✅ Answer: B**  
**Explanation:** `COPY INTO` is **idempotent** — it tracks which files have already been loaded and skips them on subsequent runs. This is one of its key advantages for incremental ingestion. Delta tables do not enforce primary key uniqueness by default (C), so duplicates *could* be inserted by other methods. `COPY INTO` without `FORCE=TRUE` never reprocesses already-loaded files. This idempotency guarantee makes it safe to retry failed loads.  
**Key Concepts:** COPY INTO idempotency, file tracking, vs. `FORCE=TRUE`, incremental load.

---

### Q22
In Spark Structured Streaming, what is the purpose of **watermarking**?

- A) To limit the number of records per micro-batch
- B) To define a threshold for how late event-time data can arrive and still be aggregated
- C) To filter records where the event timestamp is null
- D) To enforce schema on streaming DataFrames

**✅ Answer: B**  
**Explanation:** Watermarking (`withWatermark("event_time", "10 minutes")`) tells Spark: "I will wait up to 10 minutes for late-arriving events before finalizing a window aggregation." Records arriving later than the watermark threshold are dropped. Without watermarking, Spark must keep all state in memory indefinitely, causing OOM issues. It does not control micro-batch size (A), null filtering (C), or schema enforcement (D).  
**Key Concepts:** Watermarking, late data handling, stateful streaming, window aggregation, state management.

---

### Q23
A data engineer needs to implement a **Type 2 SCD** (Slowly Changing Dimension) on a customer table in Delta Lake. Which approach best supports this?

- A) Use `INSERT OVERWRITE` to replace all customer records daily
- B) Use `MERGE INTO` with logic to close existing records and insert new versions with effective/end dates
- C) Use `UPDATE` to change the customer record in place
- D) Use Delta's `OPTIMIZE` command to manage historical versions

**✅ Answer: B**  
**Explanation:** Type 2 SCD preserves full history by creating new rows for changed records. With `MERGE INTO`: when a matching record is found with changed attributes, mark the old record as inactive (set `end_date`, `is_current = false`) and insert a new record with the updated values and a new `start_date`. `INSERT OVERWRITE` (A) destroys history. `UPDATE` (C) implements Type 1 (no history). `OPTIMIZE` (D) is for file compaction.  
**Key Concepts:** SCD Type 1/2, MERGE INTO, effective/end dates, is_current flag, dimensional modeling.

---

### Q24
What does `OPTIMIZE table_name ZORDER BY (column_a, column_b)` do in Delta Lake?

- A) Sorts the data by column_a and column_b and repartitions the table
- B) Compacts small files into larger ones and co-locates related data based on column values to improve filter performance
- C) Creates a secondary index on column_a and column_b
- D) Partitions the Delta table by column_a and column_b

**✅ Answer: B**  
**Explanation:** `OPTIMIZE` compacts small Delta files into larger ones (reducing the "small files problem"). `ZORDER BY` is a data skipping technique that co-locates rows with similar values in the same files, so when filtering on those columns, Spark can skip entire files based on min/max statistics in the Delta transaction log. It is not traditional sorting (A), not a secondary index (C), and not the same as `PARTITION BY` (D).  
**Key Concepts:** OPTIMIZE, Z-Ordering, data skipping, file compaction, min/max statistics, Delta transaction log.

---

### Q25
A streaming job writes to a Delta table using trigger `Trigger.AvailableNow()`. What is the behavior?

- A) The stream runs continuously processing data as it arrives
- B) The stream processes all available data in one or more micro-batches, then stops
- C) The stream runs one single micro-batch and stops, regardless of remaining data
- D) The stream runs on a fixed schedule (e.g., every hour)

**✅ Answer: B**  
**Explanation:** `Trigger.AvailableNow()` (introduced in Databricks Runtime 10.1) processes **all data currently available** in the source in multiple micro-batches until caught up, then shuts down. This is ideal for "streaming backfills" or scheduled batch-like streaming runs. `Trigger.Once()` (deprecated) does only one micro-batch (C). Continuous trigger (A) runs indefinitely. Scheduled triggers (D) use fixed intervals.  
**Key Concepts:** `Trigger.AvailableNow()`, `Trigger.Once()`, streaming triggers, batch-mode streaming.

---

### Q26
In PySpark, what is the difference between `cache()` and `persist(StorageLevel.DISK_ONLY)`?

- A) `cache()` stores data in memory only; `persist(DISK_ONLY)` stores on disk, not memory
- B) `cache()` and `persist()` are identical — no difference
- C) `cache()` is for DataFrames; `persist()` is for RDDs only
- D) `persist(DISK_ONLY)` is not supported in Databricks

**✅ Answer: A**  
**Explanation:** `cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)` in Spark, which stores partitions in memory and spills to disk if memory is insufficient. `persist(StorageLevel.DISK_ONLY)` stores partitions only on disk (useful when memory is scarce). Both work for DataFrames and RDDs (C is wrong). `DISK_ONLY` is fully supported (D is wrong). Understanding storage levels is critical for optimizing iterative jobs.  
**Key Concepts:** `cache()`, `persist()`, StorageLevel, MEMORY_AND_DISK, DISK_ONLY, memory management.

---

### Q27
A data engineer runs a PySpark join between a large fact table (10 billion rows) and a small dimension table (10,000 rows). The job is extremely slow. What optimization should be applied?

- A) Increase shuffle partitions to 2000
- B) Use a broadcast join for the small dimension table
- C) Partition both tables by the join key
- D) Use Z-Ordering on the join key column

**✅ Answer: B**  
**Explanation:** A **broadcast join** sends the small dimension table to all executors, eliminating the expensive shuffle of the large fact table. Spark automatically broadcasts tables below `spark.sql.autoBroadcastJoinThreshold` (default 10MB). For 10,000 rows, broadcasting is ideal: `df_fact.join(broadcast(df_dim), "key")`. Increasing shuffle partitions (A) won't eliminate the shuffle cost. Partitioning (C) helps range queries but not join elimination. Z-Ordering (D) is for file skipping, not join optimization.  
**Key Concepts:** Broadcast join, shuffle join, `broadcast()`, `spark.sql.autoBroadcastJoinThreshold`, join optimization.

---

### Q28
What is the **correct output** of the following SQL query on a Delta table:
```sql
SELECT COUNT(*) FROM events VERSION AS OF 3;
```

- A) Returns an error because you cannot query historical versions
- B) Returns the row count of the table at Delta version 3
- C) Returns the number of Delta log entries up to version 3
- D) Returns the current row count, ignoring the version clause

**✅ Answer: B**  
**Explanation:** Delta Lake supports **Time Travel** via `VERSION AS OF n` or `TIMESTAMP AS OF 'datetime'`. `SELECT COUNT(*) FROM events VERSION AS OF 3` returns the row count as the table existed at Delta transaction version 3. This relies on the Delta transaction log (`_delta_log`). Time travel is supported by default until the log retention period (default: 30 days / 30 versions). It does not error (A), does not count log entries (C), and does not ignore the clause (D).  
**Key Concepts:** Delta Time Travel, `VERSION AS OF`, `TIMESTAMP AS OF`, transaction log, data auditing.

---

### Q29
A pipeline reads from Auto Loader and writes to a Delta table. The process fails mid-run. When restarted, what prevents duplicate data ingestion?

- A) Spark's built-in deduplication filter
- B) The Auto Loader checkpoint combined with Delta's idempotent write guarantees
- C) Delta's schema enforcement blocking duplicate schemas
- D) HDFS file locking preventing double reads

**✅ Answer: B**  
**Explanation:** Auto Loader maintains a **checkpoint** recording which files have been ingested. On restart, only unprocessed files are read. Combined with Spark Structured Streaming's exactly-once write semantics to Delta (which uses atomic commits), this ensures no duplicates. Spark has no built-in dedup filter (A). Schema enforcement (C) prevents wrong schemas, not duplicates. ADLS/S3 don't use HDFS file locking (D).  
**Key Concepts:** Auto Loader checkpoint, exactly-once semantics, idempotent writes, Delta atomic commits.

---

### Q30
Which statement about Delta Lake **transaction log** (`_delta_log`) is **correct**?

- A) The transaction log is stored in a separate external database
- B) Each Delta table operation (write, update, delete) creates a JSON commit file in `_delta_log`
- C) The transaction log is only maintained for partitioned tables
- D) The transaction log is deleted after `VACUUM` runs

**✅ Answer: B**  
**Explanation:** Delta Lake's transaction log (`_delta_log`) is a series of JSON files (one per transaction commit) stored in the `_delta_log/` subdirectory of the table's storage path. Every operation — writes, updates, deletes, schema changes — creates a new numbered JSON file (e.g., `00000000000000000001.json`). Every 10 commits, a Parquet checkpoint is created. The log is co-located with the table data, not external (A). It exists for all Delta tables regardless of partitioning (C). `VACUUM` removes data files but not log entries beyond retention (D).  
**Key Concepts:** `_delta_log`, JSON commit files, Parquet checkpoints, transaction atomicity, Delta protocol.

---

## Section 3 — Data Modeling (Questions 31–42)

### Q31
A Delta table has accumulated 50,000 small files over time, causing slow query performance. Which command **specifically addresses** the small files problem?

- A) `VACUUM delta_table RETAIN 168 HOURS`
- B) `OPTIMIZE delta_table`
- C) `ANALYZE TABLE delta_table COMPUTE STATISTICS`
- D) `ALTER TABLE delta_table SET TBLPROPERTIES ('delta.logRetentionDuration' = '30 days')`

**✅ Answer: B**  
**Explanation:** `OPTIMIZE` triggers bin-packing compaction: it reads many small Parquet files and rewrites them into fewer, larger files (target size ~1GB). This dramatically reduces the number of files Spark must open per query, cutting planning time and I/O overhead. `VACUUM` (A) removes old/deleted files but doesn't compact. `ANALYZE` (C) collects statistics for the optimizer. `SET TBLPROPERTIES` (D) configures retention, not compaction.  
**Key Concepts:** OPTIMIZE, bin-packing, file compaction, small files problem, Parquet target file size.

---

### Q32
What is the **medallion architecture** and what does each layer represent?

- A) Bronze = raw data; Silver = cleaned/conformed data; Gold = business-aggregated/serving data
- B) Bronze = aggregated data; Silver = raw data; Gold = ML features
- C) Bronze = streaming data; Silver = batch data; Gold = archived data
- D) Bronze = structured data; Silver = semi-structured; Gold = unstructured

**✅ Answer: A**  
**Explanation:** The **Medallion Architecture** (Lakehouse pattern): **Bronze** layer stores raw ingested data as-is (append-only, preserving source fidelity). **Silver** layer applies cleaning, deduplication, schema enforcement, and joins (conformed, validated data). **Gold** layer contains business-level aggregations, KPIs, and serving tables optimized for BI/ML consumption. This is the foundational data modeling pattern in Databricks Lakehouse.  
**Key Concepts:** Medallion architecture, Bronze/Silver/Gold, Lakehouse pattern, data quality layers.

---

### Q33
When should you use **partitioning** vs. **Z-Ordering** in Delta Lake?

- A) Partitioning for low-cardinality columns with frequent filter predicates; Z-Ordering for high-cardinality columns
- B) Z-Ordering always outperforms partitioning; prefer Z-Ordering in all cases
- C) Partitioning for high-cardinality columns; Z-Ordering for low-cardinality columns
- D) Use both together only when the table exceeds 1TB

**✅ Answer: A**  
**Explanation:** **Partitioning** creates subdirectories per unique value — ideal for low-cardinality columns like `date`, `region`, `status` (few distinct values), which enables partition pruning. Over-partitioning with high-cardinality columns (e.g., `user_id`) causes the small files problem. **Z-Ordering** works within files using data skipping on min/max statistics — ideal for high-cardinality columns (e.g., `order_id`, `timestamp`) that can't be practically partitioned.  
**Key Concepts:** Partition pruning, Z-Ordering, cardinality, small files problem, data skipping, partition strategy.

---

### Q34
A Delta table has `delta.deletedFileRetentionDuration = interval 7 days`. A `VACUUM` is run with default settings. What is the **minimum age** of files that will be deleted?

- A) Files older than 7 days
- B) Files older than 168 hours (the default VACUUM threshold is 168 hours = 7 days)
- C) All unreferenced files regardless of age
- D) Files older than 30 days (Delta's default log retention)

**✅ Answer: B**  
**Explanation:** `VACUUM` by default uses a retention threshold of **168 hours (7 days)**. It deletes data files no longer referenced by the transaction log AND older than this threshold. The property `delta.deletedFileRetentionDuration` overrides this default. The 168-hour safety threshold prevents accidental deletion of files needed by long-running queries. Running `VACUUM RETAIN 0 HOURS` would delete all unreferenced files but is strongly discouraged as it breaks time travel.  
**Key Concepts:** VACUUM, 168-hour retention, `delta.deletedFileRetentionDuration`, time travel compatibility, file lifecycle.

---

### Q35
Which statement about **Delta Lake schema enforcement** is correct?

- A) By default, Delta allows any schema — enforcement must be manually enabled
- B) Delta automatically rejects writes that don't match the table schema, unless `mergeSchema` or `overwriteSchema` is set
- C) Schema enforcement only applies to streaming writes, not batch writes
- D) Schema enforcement is only available with Unity Catalog

**✅ Answer: B**  
**Explanation:** Delta Lake **schema enforcement** (schema-on-write) rejects writes that add new columns or have incompatible data types by default, raising an `AnalysisException`. To add new columns, use `.option("mergeSchema", "true")`. To completely replace the schema, use `.option("overwriteSchema", "true")` with `mode("overwrite")`. This applies to both batch and streaming writes (C is wrong). It's a core Delta feature independent of Unity Catalog (D).  
**Key Concepts:** Schema enforcement, schema evolution, `mergeSchema`, `overwriteSchema`, AnalysisException.

---

### Q36
A data model requires a table where records are **never updated** — only new records are appended. Which Delta Lake table feature is most useful for optimizing read performance on this pattern?

- A) Delta Change Data Feed (CDF)
- B) `delta.appendOnly = true` table property
- C) Liquid Clustering
- D) Generated Columns with IDENTITY

**✅ Answer: C**  
**Explanation:** **Liquid Clustering** (available in Databricks Runtime 13.3+) replaces partitioning and Z-Ordering with an automated, incremental clustering approach that continuously optimizes data layout without full rewrites. For append-heavy tables, Liquid Clustering automatically adjusts as data grows, making it the modern best-practice for large append-only tables. `appendOnly = true` (B) just prevents updates/deletes, not an optimization. CDF (A) tracks changes. Generated columns (D) are for derived column values.  
**Key Concepts:** Liquid Clustering, `CLUSTER BY`, DBR 13.3+, vs. partitioning/Z-Ordering, append-heavy tables.

---

### Q37
You execute `RESTORE TABLE events TO VERSION AS OF 5`. What is the **effect** on the Delta table?

- A) The table's data files are rolled back to version 5, and subsequent versions are deleted
- B) A new commit is created that makes the table's current state match version 5, without deleting history
- C) Only the schema is restored to version 5's schema
- D) A read-only snapshot of version 5 is created as a new table

**✅ Answer: B**  
**Explanation:** `RESTORE TABLE` creates a **new Delta commit** that makes the current state equivalent to the specified historical version. Crucially, it does **not** delete the transaction log history between version 5 and the current version — the full history is preserved. This allows auditing and re-restore if needed. It's not a destructive rollback (A). It restores data, not just schema (C). It modifies the existing table in place, not creating a new one (D).  
**Key Concepts:** RESTORE TABLE, non-destructive restore, Delta history preservation, rollback patterns.

---

### Q38
What is **Change Data Feed (CDF)** in Delta Lake and when should it be enabled?

- A) A feature that automatically deduplicates records on write
- B) A feature that records row-level changes (inserts, updates, deletes) for consumption by downstream pipelines
- C) A Kafka-based CDC connector for streaming ingestion
- D) A method for tracking schema changes over time

**✅ Answer: B**  
**Explanation:** **Delta Change Data Feed** tracks row-level changes: `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`), `_commit_version`, and `_commit_timestamp`. Enabled with `delta.enableChangeDataFeed = true`. Downstream consumers read changes with `spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", n)`. Ideal for downstream Silver/Gold table refresh without full reprocessing. Not a Kafka connector (C) or schema tracker (D).  
**Key Concepts:** CDF, `_change_type`, `readChangeFeed`, incremental propagation, downstream pipeline patterns.

---

### Q39
A Delta table is partitioned by `year` and `month`. A query filters only on `month = 6`. Which statement is **correct**?

- A) Partition pruning eliminates all partitions except those where month = 6
- B) Databricks scans all partitions because the leading partition key `year` is not specified
- C) Z-Ordering on `year` compensates for the missing `year` filter
- D) The query will fail because multi-column partitions require all columns in the filter

**✅ Answer: A**  
**Explanation:** Delta Lake's partition pruning works on any subset of partition columns specified in the filter. A filter on `month = 6` will prune all partitions where `month ≠ 6`, regardless of whether `year` is specified. Unlike some traditional systems, Delta doesn't require hierarchical partition key ordering for pruning. The query won't fail (D), and Z-Ordering doesn't affect partition pruning (C).  
**Key Concepts:** Partition pruning, multi-column partitions, filter predicate pushdown, partition directory structure.

---

### Q40
In Unity Catalog, what does a **three-level namespace** (`catalog.schema.table`) enable?

- A) Partitioning tables by three levels of granularity
- B) Cross-workspace data access and governance with centralized access control
- C) Multi-cloud table replication across three cloud providers
- D) Three-layer caching for improved query performance

**✅ Answer: B**  
**Explanation:** Unity Catalog introduces the **three-level namespace**: `catalog` (top-level container, typically mapped to a business domain or environment), `schema` (database/namespace), `table/view/function`. This enables: (1) centralized access control with fine-grained ACLs, (2) cross-workspace data sharing, (3) data lineage tracking, (4) consistent governance across all Databricks workspaces in an account. Not related to storage partitioning (A), cloud replication (C), or caching (D).  
**Key Concepts:** Unity Catalog, three-level namespace, catalog/schema/table, centralized governance, cross-workspace sharing.

---

### Q41
A data engineer creates a Delta table with `GENERATED ALWAYS AS (CAST(event_timestamp AS DATE))` for a `event_date` column. What does this accomplish?

- A) Automatically fills `event_date` with today's date on each write
- B) Derives `event_date` from `event_timestamp` automatically, enabling partition pruning on `event_date` even when filtered on `event_timestamp`
- C) Creates a computed column that can be manually overwritten
- D) Encrypts the `event_timestamp` column and stores the hash in `event_date`

**✅ Answer: B**  
**Explanation:** **Generated Columns** derive values from other columns using a deterministic expression. When `event_date` is a generated column and also a partition column, Delta can apply **partition pruning** automatically even when queries filter on `event_timestamp` — Databricks recognizes the relationship. The column cannot be manually written (C — `GENERATED ALWAYS AS` is read-only). Not today's date (A). Not encryption (D).  
**Key Concepts:** Generated columns, `GENERATED ALWAYS AS`, partition pruning with derived expressions, automatic derivation.

---

### Q42
What happens when you run `ALTER TABLE my_table ADD COLUMN new_col STRING` on a Delta table?

- A) All existing rows get `null` for `new_col`; the operation is recorded in the transaction log
- B) An error is thrown because schema changes require recreating the table
- C) All existing rows are rewritten to include `new_col` with empty string values
- D) The column is added only to new partitions going forward

**✅ Answer: A**  
**Explanation:** Delta Lake supports **schema evolution** via `ALTER TABLE ADD COLUMN`. The operation is a **metadata-only change** recorded in the transaction log — no data files are rewritten. Existing rows will return `null` for the new column (default for added nullable columns). This is O(1) complexity, not O(n). The table is not recreated (B). Files are not rewritten (C). The change applies to the full table, not just new partitions (D).  
**Key Concepts:** Schema evolution, `ALTER TABLE ADD COLUMN`, metadata-only operation, null defaults, transaction log.

---

## Section 4 — Security & Governance (Questions 43–48)

### Q43
In Unity Catalog, a data engineer needs to grant a user read access to a specific table but not the entire schema. Which SQL statement is **correct**?

- A) `GRANT SELECT ON SCHEMA catalog.schema TO user@company.com`
- B) `GRANT SELECT ON TABLE catalog.schema.table TO user@company.com`
- C) `GRANT READ ON TABLE catalog.schema.table TO user@company.com`
- D) `ALTER TABLE catalog.schema.table SET ACCESS user@company.com = SELECT`

**✅ Answer: B**  
**Explanation:** Unity Catalog uses standard SQL GRANT syntax: `GRANT privilege ON securable_object TO principal`. For table-level read access, `SELECT` is the privilege on the `TABLE` object. The full three-level name is required. Granting on `SCHEMA` (A) gives access to all tables in the schema. `READ` is not a valid Unity Catalog privilege (C). `ALTER TABLE SET ACCESS` is not valid SQL (D).  
**Key Concepts:** Unity Catalog GRANT, `SELECT` privilege, securable objects, principal (user/group/service principal).

---

### Q44
What is a **Unity Catalog Metastore** and how does it differ from a Hive Metastore?

- A) Both are identical; Unity Catalog is just a rebranded Hive Metastore
- B) Unity Catalog Metastore is account-level, supports centralized governance and fine-grained ACLs; Hive Metastore is workspace-level with limited governance
- C) Unity Catalog Metastore stores data in Azure SQL Database; Hive Metastore stores in Delta Lake
- D) Unity Catalog Metastore only supports Parquet; Hive Metastore supports all formats

**✅ Answer: B**  
**Explanation:** The **Unity Catalog Metastore** is an account-level construct (one per Databricks account per region), providing: centralized governance, fine-grained access controls (table, row, column level), built-in data lineage, cross-workspace access, and audit logging. The **Hive Metastore** is workspace-scoped, lacks centralized governance, and has no built-in lineage or audit capabilities. They are fundamentally different architectures (A). Storage backend is cloud object storage, not Azure SQL (C). Both support all Delta/Parquet/etc. formats (D).  
**Key Concepts:** Unity Catalog Metastore, Hive Metastore, account-level vs. workspace-level, governance, lineage.

---

### Q45
A data engineer needs to mask the `SSN` column so that analysts can query the table but see only the last 4 digits. Which Unity Catalog feature enables this **without creating a separate view**?

- A) Column Encryption using a customer-managed key
- B) Row-Level Security with a filter function
- C) Column Masking with a masking function applied to the column
- D) Delta Lake schema enforcement blocking SSN writes

**✅ Answer: C**  
**Explanation:** **Unity Catalog Column Masking** (available in DBR 12.2+ with Unity Catalog) applies a user-defined function to a column at query time based on the querying user's identity or group membership. Example: `ALTER TABLE customers ALTER COLUMN SSN SET MASK mask_ssn_function;`. Analysts see `***-**-1234` while privileged users see the full value. This is enforced at the catalog level without separate views. Column encryption (A) is for data-at-rest. Row-level security (B) filters rows, not column values. Schema enforcement (D) prevents writes, not reads.  
**Key Concepts:** Column Masking, Unity Catalog, masking functions, data protection, PII handling.

---

### Q46
A Databricks service principal needs to access an Azure Data Lake Storage Gen2 account. What is the **most secure and recommended** authentication method?

- A) Embed the storage account key in the Databricks secret scope
- B) Use Microsoft Entra ID (AAD) passthrough authentication
- C) Configure a Databricks-managed service principal with an OAuth 2.0 credential (Client Secret or Federated Identity) in Unity Catalog's credential store
- D) Mount the ADLS path using `dbutils.fs.mount()` with a hardcoded SAS token

**✅ Answer: C**  
**Explanation:** The recommended approach is to register the service principal in Microsoft Entra ID, grant it `Storage Blob Data Contributor` on the ADLS container, and store the OAuth credentials (client ID, tenant ID, client secret) in a Unity Catalog **external location credential** or Databricks secret scope. This avoids shared keys (A) which grant full storage account access. AAD passthrough (B) is deprecated in Unity Catalog environments. SAS tokens (D) have limited expiry and hardcoding is a security anti-pattern.  
**Key Concepts:** Service principal, OAuth 2.0, Unity Catalog external locations, storage credentials, ADLS Gen2 authentication.

---

### Q47
What is the purpose of **Delta Sharing** in Databricks?

- A) Sharing Delta table schemas between workspaces within the same Databricks account
- B) An open protocol for securely sharing live data across organizations and cloud platforms without copying data
- C) Replicating Delta tables between Azure regions for disaster recovery
- D) Automatically syncing Delta tables to external databases via JDBC

**✅ Answer: B**  
**Explanation:** **Delta Sharing** is an open-source protocol (part of the Linux Foundation) that enables sharing live, read-only Delta table data with external recipients (other organizations, different cloud providers, non-Databricks users) without copying or moving data. Recipients access data via a REST API using a bearer token. It supports Python, Spark, Pandas, and Power BI clients. It's not limited to within-account sharing (A), not DR replication (C), and not JDBC sync (D).  
**Key Concepts:** Delta Sharing, open protocol, cross-org data sharing, zero-copy sharing, recipient access.

---

### Q48
A pipeline writes PII data to a Delta table. Compliance requires that specific users cannot access raw PII but can access aggregated data. Which combination of Unity Catalog features provides this?

- A) Column Masking + Row Filters
- B) Table ACLs (GRANT/REVOKE) + Column Masking
- C) Delta Lake encryption + VACUUM retention
- D) Cluster policies + Secret scopes

**✅ Answer: B**  
**Explanation:** **Table ACLs** (GRANT SELECT to specific groups, REVOKE from others) control which users can query a table at all. **Column Masking** dynamically masks PII columns (e.g., showing `****` for SSNs) based on user identity, allowing aggregated queries while protecting raw values. Together, these implement fine-grained, role-based PII protection without creating multiple tables. Delta encryption (C) protects data-at-rest storage, not query access. Cluster policies (D) control compute, not data access.  
**Key Concepts:** Table ACLs, Column Masking, Row Filters, PII protection, Unity Catalog privilege model.

---

## Section 5 — Monitoring & Logging (Questions 49–54)

### Q49
A Databricks Job fails unexpectedly. A data engineer wants to investigate the root cause. Which resource provides the **most detailed** task-level logs?

- A) The Databricks workspace audit log in Azure Monitor
- B) The Spark UI accessed from the cluster's "Spark UI" link in the Job Run view
- C) The Unity Catalog data lineage graph
- D) The `DESCRIBE HISTORY` output of the Delta table

**✅ Answer: B**  
**Explanation:** The **Spark UI** (available during and after a run via the cluster link) shows DAG visualization, stage breakdowns, task-level timing, shuffle statistics, executor logs, and error stack traces. This is the primary tool for debugging job failures. The workspace audit log (A) records API calls and user actions, not Spark execution details. Data lineage (C) shows table relationships. `DESCRIBE HISTORY` (D) shows Delta table operations, not job execution details.  
**Key Concepts:** Spark UI, DAG visualization, stage/task debugging, driver logs, executor logs.

---

### Q50
In Delta Live Tables, where can you view **data quality metrics** (expectation pass/fail rates) for a pipeline?

- A) In the Spark UI's SQL tab
- B) In the DLT pipeline UI under the "Data Quality" tab, and in the `event_log` system table
- C) In the Unity Catalog data lineage graph
- D) By running `DESCRIBE DETAIL` on each DLT table

**✅ Answer: B**  
**Explanation:** DLT provides a dedicated **pipeline UI** showing per-table data quality metrics: number of records passing/failing each `@dlt.expect` constraint. Additionally, DLT writes detailed event logs to the `event_log` table (queryable via SQL), which includes expectation statistics, pipeline events, warnings, and errors. This enables building custom data quality dashboards. Spark UI (A) shows execution metrics, not DLT expectations. Lineage (C) shows flow, not quality. `DESCRIBE DETAIL` (D) shows table stats, not DLT expectation results.  
**Key Concepts:** DLT event log, data quality UI, `event_log` table, expectation metrics, pipeline observability.

---

### Q51
A streaming pipeline's processing latency is increasing over time. Which Spark metric should a data engineer monitor to identify the **bottleneck**?

- A) `spark.executor.memory.used`
- B) `processingTime` and `inputRowsPerSecond` in Structured Streaming query progress
- C) Number of active Spark jobs in the Spark UI
- D) Delta transaction log size

**✅ Answer: B**  
**Explanation:** Structured Streaming exposes a **StreamingQueryProgress** object (available via `query.lastProgress` or the Spark UI Streaming tab) that includes: `processingTime` (time to process a micro-batch), `inputRowsPerSecond` (throughput), `numInputRows`, and `batchDuration`. Increasing `processingTime` with stable `inputRowsPerSecond` indicates compute bottleneck; decreasing `inputRowsPerSecond` may indicate source or network issues. Memory metrics (A) help but don't directly identify streaming-specific latency. Active job count (C) is too coarse. Log size (D) is irrelevant.  
**Key Concepts:** StreamingQueryProgress, `processingTime`, `inputRowsPerSecond`, streaming monitoring, latency diagnosis.

---

### Q52
A data engineer wants to receive an alert when a Databricks Job fails. What is the **built-in** mechanism for this?

- A) Configure a CloudWatch alarm in AWS
- B) Set up email or webhook notifications in the Job's notification settings
- C) Use a Databricks SQL alert with a threshold query
- D) Write a custom monitoring notebook that polls the Jobs API every minute

**✅ Answer: B**  
**Explanation:** Databricks Jobs natively support **notifications** (email and webhook) configured directly in the job settings under "Notifications". You can trigger on job start, success, failure, or skipped. Webhooks can integrate with Slack, PagerDuty, Teams, etc. CloudWatch (A) is an AWS-native service not specifically tied to Databricks job state. SQL Alerts (C) monitor query results, not job execution state. Custom polling (D) is unnecessary overhead when built-in notifications exist.  
**Key Concepts:** Job notifications, email alerts, webhooks, Slack integration, PagerDuty, job failure handling.

---

### Q53
Which command retrieves the **full operation history** of a Delta table including who made changes and when?

- A) `SHOW TBLPROPERTIES my_table`
- B) `DESCRIBE HISTORY my_table`
- C) `SELECT * FROM my_table@v0`
- D) `EXPLAIN HISTORY my_table`

**✅ Answer: B**  
**Explanation:** `DESCRIBE HISTORY table_name` returns the transaction log history: operation type (`WRITE`, `MERGE`, `DELETE`, `OPTIMIZE`, `VACUUM`), timestamp, user who performed the operation, operation parameters, metrics (rows added/removed), and Delta version number. This is the primary audit tool for Delta tables. `SHOW TBLPROPERTIES` (A) returns table properties. `@v0` syntax (C) queries data at a version, not history. `EXPLAIN HISTORY` (D) doesn't exist.  
**Key Concepts:** `DESCRIBE HISTORY`, Delta audit log, operation history, user attribution, version tracking.

---

### Q54
A DLT pipeline has a table defined with `@dlt.expect_or_fail("no_nulls_id", "id IS NOT NULL")`. What happens when a record with `id = NULL` arrives?

- A) The record is silently dropped
- B) The record is allowed through and flagged in the event log
- C) The entire pipeline run fails immediately
- D) The pipeline pauses and waits for manual intervention

**✅ Answer: C**  
**Explanation:** `@dlt.expect_or_fail()` is the strictest DLT expectation level — any violation **immediately fails the entire pipeline update**. This ensures data quality gates are enforced for critical, non-nullable fields where proceeding with bad data would corrupt downstream tables. Compare: `@dlt.expect()` = warn only, `@dlt.expect_or_drop()` = drop bad rows and continue, `@dlt.expect_or_fail()` = fail pipeline. The pipeline doesn't pause (D) — it terminates the run.  
**Key Concepts:** `@dlt.expect_or_fail`, DLT expectation levels, pipeline failure on quality violation, data quality enforcement.

---

## Section 6 — Testing & Deployment (Questions 55–59)

### Q55
A data engineer uses `databricks bundle deploy` to deploy a DLT pipeline to production. What does **Databricks Asset Bundles (DABs)** primarily provide?

- A) A drag-and-drop UI for deploying notebooks to production clusters
- B) A YAML-based framework for defining, versioning, and deploying Databricks resources (jobs, pipelines, notebooks) as code
- C) An automated testing framework for Delta table schema validation
- D) A cloud-agnostic container orchestration system for Databricks workloads

**✅ Answer: B**  
**Explanation:** **Databricks Asset Bundles (DABs)** enable Infrastructure-as-Code for Databricks. You define resources (jobs, DLT pipelines, clusters, notebooks, dashboards) in `databricks.yml` YAML files, commit to Git, and deploy via `databricks bundle deploy`. This enables: environment-specific configs (dev/staging/prod), CI/CD integration, version control of pipeline definitions, and reproducible deployments. Not a UI tool (A). Not a testing framework (C). Not container orchestration (D).  
**Key Concepts:** Databricks Asset Bundles, `databricks.yml`, IaC, `databricks bundle deploy`, environment promotion.

---

### Q56
During unit testing of a PySpark transformation function, which approach allows testing **without a live Spark cluster**?

- A) Deploy the function to a Databricks job cluster and run integration tests
- B) Use `pytest` with `pyspark.sql.SparkSession` in local mode (`spark://local`)
- C) Mock the Delta table reads using COPY INTO on a test table
- D) Only end-to-end integration tests are possible for PySpark functions

**✅ Answer: B**  
**Explanation:** PySpark supports a `local` execution mode where Spark runs entirely within the Python process on a single machine. Using `SparkSession.builder.master("local[*]").getOrCreate()` in `pytest` enables unit testing of DataFrame transformations without any cluster. This is fast, cheap, and suitable for CI/CD pipelines. Libraries like `chispa` or `pytest-spark` improve PySpark testing ergonomics. Integration tests (A) require a cluster but test the full pipeline. COPY INTO (C) is ingestion, not testing. Pure unit tests are absolutely possible (D is wrong).  
**Key Concepts:** PySpark local mode, `pytest`, unit testing, `SparkSession.builder.master("local")`, `chispa`, CI/CD testing.

---

### Q57
A data engineering team follows a GitOps workflow. They want to automatically deploy Databricks Jobs when code is merged to the `main` branch. Which combination of tools enables this?

- A) Databricks Repos + manual job re-configuration after each merge
- B) GitHub Actions (or Azure DevOps) + Databricks CLI / Asset Bundles in a CI/CD pipeline
- C) Delta Live Tables with continuous pipeline mode triggered by Git commits
- D) Databricks SQL Alerts triggered by changes in the Unity Catalog

**✅ Answer: B**  
**Explanation:** A GitOps CI/CD pipeline: GitHub Actions workflow (`.github/workflows/deploy.yml`) triggers on `push` to `main`, runs `databricks bundle deploy --target prod`, deploying updated job definitions, notebook code, and pipeline configurations. The Databricks CLI authenticates via a service principal and secret. This is the modern, recommended approach for automated Databricks deployments. Manual reconfiguration (A) defeats GitOps. DLT continuous mode (C) runs the pipeline, not deployment. SQL Alerts (D) monitor query results.  
**Key Concepts:** GitOps, GitHub Actions, Azure DevOps, Databricks CLI, Asset Bundles, automated deployment, CI/CD.

---

### Q58
A data engineer wants to test a DLT pipeline in a **development** environment before promoting to production. What is the **recommended approach** using Asset Bundles?

- A) Run the pipeline directly in the production workspace and check logs afterward
- B) Define separate `targets` (dev, staging, prod) in `databricks.yml` with environment-specific settings, and deploy to dev first
- C) Duplicate all notebooks and maintain separate versions per environment manually
- D) Use the DLT `development` mode toggle in the UI only and avoid code-based deployment

**✅ Answer: B**  
**Explanation:** Databricks Asset Bundles support **target environments** in `databricks.yml`: `targets: dev: ...`, `staging: ...`, `prod: ...`. Each target can specify different workspace URLs, cluster sizes, pipeline settings (dev mode), and table names. Deploy with `databricks bundle deploy --target dev` for safe testing. Running in prod first (A) risks data corruption. Manual notebook duplication (C) creates maintenance nightmares. UI-only dev mode (D) doesn't integrate with CI/CD.  
**Key Concepts:** DABs targets, environment promotion, `databricks.yml` targets, dev/staging/prod, pipeline dev mode.

---

### Q59
A production DLT pipeline fails with a schema mismatch error after a source system adds a new column. What is the **correct remediation** without losing existing data or history?

- A) Drop and recreate the DLT table to reset the schema
- B) Enable `schema evolution` in the DLT pipeline settings and set `autoMerge = true` or configure the pipeline for schema evolution
- C) Manually alter the table schema using `ALTER TABLE ADD COLUMN` and restart the pipeline
- D) Use `RESTORE TABLE` to roll back to the last successful version

**✅ Answer: B**  
**Explanation:** DLT supports **schema evolution** via pipeline settings (`"schema": "evolve"`) or through Auto Loader's `schemaEvolutionMode = "addNewColumns"` for source reading. When enabled, new columns in the source are automatically added to the target DLT table without failing the pipeline. Dropping the table (A) loses history and is destructive. Manual `ALTER TABLE` (C) may work for existing Delta tables but is not the DLT-managed approach. `RESTORE TABLE` (D) rolls back data, not forward-evolves schema.  
**Key Concepts:** DLT schema evolution, `schema evolve`, Auto Loader schema evolution, `addNewColumns`, pipeline resilience.

---

## Answer Key

| Q | Answer | Domain |
|---|---|---|
| 1 | B | Tooling |
| 2 | B | Tooling |
| 3 | B | Tooling |
| 4 | C | Tooling |
| 5 | B | Tooling |
| 6 | B | Tooling |
| 7 | C | Tooling |
| 8 | C | Tooling |
| 9 | B | Tooling |
| 10 | C | Tooling |
| 11 | B | Tooling |
| 12 | B | Tooling |
| 13 | B | Processing |
| 14 | A | Processing |
| 15 | C | Processing |
| 16 | B | Processing |
| 17 | C | Processing |
| 18 | B | Processing |
| 19 | B | Processing |
| 20 | D | Processing |
| 21 | B | Processing |
| 22 | B | Processing |
| 23 | B | Processing |
| 24 | B | Processing |
| 25 | B | Processing |
| 26 | A | Processing |
| 27 | B | Processing |
| 28 | B | Processing |
| 29 | B | Processing |
| 30 | B | Processing |
| 31 | B | Modeling |
| 32 | A | Modeling |
| 33 | A | Modeling |
| 34 | B | Modeling |
| 35 | B | Modeling |
| 36 | C | Modeling |
| 37 | B | Modeling |
| 38 | B | Modeling |
| 39 | A | Modeling |
| 40 | B | Modeling |
| 41 | B | Modeling |
| 42 | A | Modeling |
| 43 | B | Security |
| 44 | B | Security |
| 45 | C | Security |
| 46 | C | Security |
| 47 | B | Security |
| 48 | B | Security |
| 49 | B | Monitoring |
| 50 | B | Monitoring |
| 51 | B | Monitoring |
| 52 | B | Monitoring |
| 53 | B | Monitoring |
| 54 | C | Monitoring |
| 55 | B | Testing |
| 56 | B | Testing |
| 57 | B | Testing |
| 58 | B | Testing |
| 59 | B | Testing |

---
*Mock Exam 1 of 3 — Databricks Certified Data Engineer Professional Prep*
