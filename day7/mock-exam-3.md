# 📝 Mock Exam 3 — Databricks Data Engineer Associate

> **45 questions · 90-minute time limit · Passing score: ~70% (≈32/45)**  
> **Instructions:** Choose the single best answer. Answers + explanations at the bottom.

---

## Questions

**1.** Which statement best describes the Databricks Lakehouse architecture?

- A) A data warehouse that stores data in proprietary formats with full SQL support
- B) A system that combines the flexibility of data lakes with the reliability and performance of data warehouses
- C) A cloud-native message queue optimized for real-time event streaming
- D) A key-value store built on top of HDFS for low-latency lookups

---

**2.** A data engineer needs to create a table that is accessible from multiple workspaces and manages permissions centrally. Which Databricks feature should they use?

- A) Databricks File System (DBFS)
- B) Hive Metastore (workspace-local)
- C) Unity Catalog
- D) External Delta table in S3

---

**3.** What is the minimum Unity Catalog object hierarchy needed to create a table?

- A) Workspace → Database → Table
- B) Metastore → Catalog → Schema → Table
- C) Catalog → Table
- D) Metastore → Schema → Table

---

**4.** A cluster is configured with `spark.databricks.delta.optimizeWrite.enabled = true`. What is the effect?

- A) Automatically runs `VACUUM` after every write
- B) Coalesces shuffle partitions into right-sized files before writing
- C) Enables Z-ordering on all write operations
- D) Converts all Parquet files to Delta format automatically

---

**5.** Which Delta Lake operation would you use to permanently remove files that are no longer referenced by the transaction log?

- A) `OPTIMIZE`
- B) `ZORDER BY`
- C) `VACUUM`
- D) `PURGE TABLE`

---

**6.** A data engineer runs `VACUUM sales RETAIN 0 HOURS` without first disabling time travel. What happens?

- A) Succeeds and removes all old files, preserving the latest version
- B) Fails with an error because the default minimum retention is 7 days
- C) Removes all files including the current version, making the table unreadable
- D) Runs but logs a deprecation warning

---

**7.** You need to read the state of a Delta table as it existed exactly 3 versions ago. Which query is correct?

- A) `SELECT * FROM my_table AT VERSION 3`
- B) `SELECT * FROM my_table VERSION AS OF (CURRENT_VERSION() - 3)`
- C) `SELECT * FROM my_table VERSION AS OF 3`
- D) `SELECT * FROM my_table RESTORE TO VERSION 3`

---

**8.** Change Data Feed is enabled on table `orders`. Which column in the CDF output indicates whether a row was inserted, updated (before), updated (after), or deleted?

- A) `_cdf_action`
- B) `_change_type`
- C) `_operation`
- D) `_cdc_event`

---

**9.** A MERGE statement on a Delta table runs the following logic: when a matching key is found, update the row; when no match exists in the source, delete the row from the target; when no match exists in the target, insert the row. Which MERGE clause handles the "delete unmatched target rows" scenario?

- A) `WHEN NOT MATCHED BY SOURCE THEN DELETE`
- B) `WHEN MATCHED AND source.key IS NULL THEN DELETE`
- C) `WHEN NOT MATCHED THEN DELETE`
- D) MERGE does not support deletion of target rows

---

**10.** Auto Loader uses which mechanism to track which files have already been ingested?

- A) A metadata column `_source_file` added to each row
- B) An internal checkpoint directory and RocksDB state store
- C) A `_committed_files` table in the Hive Metastore
- D) File modification timestamps compared on every run

---

**11.** A PySpark DataFrame `df` has columns `id`, `category`, `value`. Which code correctly computes the sum of `value` per `category` and renames the result to `total`?

- A) `df.groupBy("category").sum("value").alias("total")`
- B) `df.groupBy("category").agg(F.sum("value")).rename("total")`
- C) `df.groupBy("category").agg(F.sum("value").alias("total"))`
- D) `df.groupBy("category").agg({"value": "sum"}).rename("total")`

---

**12.** Which PySpark function adds a column containing the current timestamp to a DataFrame?

- A) `F.now()`
- B) `F.current_timestamp()`
- C) `F.timestamp_now()`
- D) `F.lit(datetime.now())`

---

**13.** A window function needs to compute a running total of `amount` ordered by `event_date` within each `customer_id` partition. Which `rowsBetween` is correct?

- A) `Window.partitionBy("customer_id").orderBy("event_date").rowsBetween(0, Window.unboundedFollowing)`
- B) `Window.partitionBy("customer_id").orderBy("event_date").rowsBetween(Window.unboundedPreceding, 0)`
- C) `Window.partitionBy("customer_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)`
- D) `Window.orderBy("event_date").rowsBetween(Window.unboundedPreceding, 0)`

---

**14.** You use `spark.read.format("csv").option("inferSchema", "true").load(path)`. What is a known drawback of `inferSchema`?

- A) It reads the file twice, increasing read time
- B) It only works for files smaller than 1 GB
- C) It cannot infer nullable columns
- D) It requires all columns to have headers

---

**15.** Which SQL command lists all tables in the current schema?

- A) `SHOW DATABASES`
- B) `SHOW TABLES`
- C) `LIST TABLES`
- D) `DESCRIBE SCHEMA`

---

**16.** What does `DESCRIBE EXTENDED table_name` show that `DESCRIBE table_name` does not?

- A) Only column data types and nullable flags
- B) Table location, table type (managed/external), owner, creation time, and table properties
- C) Row count and file count statistics
- D) Column-level lineage information

---

**17.** A Structured Streaming query uses `trigger(processingTime="30 seconds")`. What does this mean?

- A) The stream will produce output rows every 30 seconds regardless of input
- B) A new micro-batch is triggered every 30 seconds
- C) The watermark delay is set to 30 seconds
- D) The query automatically stops after 30 seconds

---

**18.** Which output mode rewrites the entire result table on every trigger, suitable for aggregation queries?

- A) `append`
- B) `update`
- C) `complete`
- D) `overwrite`

---

**19.** A streaming query uses `withWatermark("event_time", "10 minutes")`. What does this configure?

- A) The maximum allowed processing delay for the cluster
- B) The time window size for `groupBy` aggregations
- C) The amount of late data the engine will tolerate before dropping it
- D) The checkpoint interval in minutes

---

**20.** `trigger(availableNow=True)` replaced `trigger(once=True)` starting with Databricks Runtime 10.3. What is the key difference?

- A) `availableNow` runs faster because it uses Continuous mode
- B) `availableNow` processes data in multiple micro-batches instead of one, improving parallelism
- C) `availableNow` requires a Unity Catalog table as the source
- D) There is no functional difference; they are aliases

---

**21.** In a Delta Live Tables pipeline, which decorator causes the pipeline to **fail entirely** when a data quality expectation is violated?

- A) `@dlt.expect`
- B) `@dlt.expect_or_drop`
- C) `@dlt.expect_or_fail`
- D) `@dlt.expect_and_halt`

---

**22.** A DLT table defined with `@dlt.table` (Python) or `CREATE LIVE TABLE` (SQL) is:

- A) A streaming table that appends new data each pipeline run
- B) A materialized view that is fully recomputed each pipeline run
- C) A temporary view that is dropped after the pipeline completes
- D) An external table stored in the cloud storage path provided

---

**23.** What is the difference between `dlt.read()` and `dlt.read_stream()` inside a DLT pipeline?

- A) `dlt.read()` performs a batch read; `dlt.read_stream()` performs a streaming (incremental) read
- B) `dlt.read()` reads from external data sources; `dlt.read_stream()` reads from internal DLT tables only
- C) `dlt.read_stream()` is deprecated; use `dlt.read()` with `trigger(availableNow=True)` instead
- D) There is no functional difference; they produce identical results

---

**24.** A DLT pipeline runs in **Triggered** mode. What happens after all data has been processed?

- A) The pipeline continues running and waits for new data
- B) The pipeline stops and releases cluster resources
- C) The pipeline enters a "paused" state until manually resumed
- D) The pipeline switches to Continuous mode automatically

---

**25.** Which Databricks Workflows feature allows a task to start only after two upstream tasks have both succeeded?

- A) Task clusters
- B) Task dependencies (linear or fan-in configuration)
- C) Task retries
- D) Job parameters

---

**26.** A data engineer wants to trigger a Databricks job every weekday at 8:00 AM Munich time (CET/CEST). Which approach is correct?

- A) Use `cron: 0 8 * * 1-5` and rely on Databricks auto-detecting the timezone
- B) Configure a Quartz cron expression with the timezone set to `Europe/Berlin` in the job UI
- C) Use `continuous trigger` with a 24-hour interval
- D) Use the Databricks REST API to submit a run at 8:00 AM using a cloud scheduler

---

**27.** What is a Databricks Asset Bundle (DAB)?

- A) A compressed archive of Databricks notebooks for sharing
- B) A YAML-based project structure for packaging and deploying Databricks workflows, notebooks, and pipelines via CI/CD
- C) A Delta table containing metadata about all assets in a workspace
- D) A feature of Unity Catalog that groups related tables into a deployable unit

---

**28.** In Unity Catalog, which privilege allows a user to query data from a table?

- A) `USE CATALOG`
- B) `USE SCHEMA`
- C) `SELECT`
- D) `READ FILES`

---

**29.** A data engineer wants to share a Delta table with an external organization without giving them access to the Databricks workspace. Which feature enables this?

- A) Unity Catalog external tables
- B) Delta Sharing
- C) Lakehouse Federation
- D) Databricks Partner Connect

---

**30.** What does Lakehouse Federation allow?

- A) Sharing Delta tables across multiple Databricks accounts
- B) Federating queries across external database systems (Snowflake, MySQL, PostgreSQL) without ingesting data into Databricks
- C) Connecting Databricks notebooks to external BI tools via JDBC
- D) Replicating Unity Catalog metadata to other cloud providers

---

**31.** A table in Unity Catalog is a **managed table**. Where is the underlying data stored?

- A) In DBFS root (dbfs:/user/hive/warehouse)
- B) In the cloud storage location configured for the metastore or catalog
- C) In the workspace-local Hive Metastore
- D) Managed tables cannot be created in Unity Catalog; only external tables are supported

---

**32.** What happens to the underlying data files when you `DROP TABLE` a **managed** Delta table?

- A) The metadata is deleted but the data files are retained for 30 days
- B) Both the metadata and the data files are permanently deleted
- C) The data files are moved to a trash location in DBFS
- D) The data files are archived to long-term storage (e.g., Glacier)

---

**33.** What happens to the underlying data files when you `DROP TABLE` an **external** Delta table?

- A) Both the metadata and the data files are permanently deleted
- B) Only the metadata is removed from Unity Catalog; the data files remain in the external storage location
- C) The data files are converted to Parquet format before deletion
- D) An error is thrown — external tables cannot be dropped

---

**34.** A data engineer runs the following SQL:
```sql
GRANT SELECT ON TABLE catalog.schema.sales TO user@company.com;
```
Which additional grants does the user need to successfully run `SELECT * FROM catalog.schema.sales`?

- A) No additional grants are needed
- B) `USE CATALOG catalog` and `USE SCHEMA catalog.schema`
- C) `READ FILES` on the external location
- D) `USAGE` on the metastore

---

**35.** Which SQL command shows the column-level lineage for a table in Unity Catalog?

- A) `DESCRIBE LINEAGE table_name`
- B) Lineage is only viewable in the Data Explorer UI, not via SQL
- C) `SHOW LINEAGE TABLE table_name`
- D) `DESCRIBE EXTENDED table_name` (includes lineage in the output)

---

**36.** A data engineer sees that a Databricks Workflow task failed on its 2nd retry. What setting controls the number of retry attempts for a task?

- A) `max_retries` in the task configuration
- B) `retry_on_timeout` in the cluster config
- C) `num_workers` in the autoscaling policy
- D) Retries must be handled in the notebook code itself

---

**37.** Which cluster type in Databricks is optimized for running automated workloads and should **not** be used for interactive development?

- A) All-Purpose Cluster
- B) High-Concurrency Cluster
- C) Job Cluster (now called "Jobs compute")
- D) SQL Warehouse

---

**38.** A SQL Warehouse (formerly SQL Endpoint) is best suited for:

- A) Running long-running Python ML training jobs
- B) Interactive SQL analytics, BI dashboards, and ad hoc queries
- C) Continuous streaming pipelines
- D) DLT pipeline execution

---

**39.** What is the purpose of the `_rescued_data` column that Auto Loader and `COPY INTO` can add to a Delta table?

- A) To track which files were successfully ingested
- B) To capture data that could not be parsed into the expected schema
- C) To store deleted rows for soft-delete patterns
- D) To log transformation errors from DLT expectations

---

**40.** A data engineer writes:
```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
```
What does `overwriteSchema=true` enable?

- A) It allows writing a DataFrame with fewer columns than the existing table
- B) It allows replacing the existing table schema with the new DataFrame schema
- C) It automatically merges new columns into the existing schema
- D) It validates that the new schema is backward-compatible before writing

---

**41.** A pipeline has Bronze → Silver → Gold tables. In which layer would you apply `@dlt.expect_or_drop` to filter out records with null primary keys?

- A) Bronze — raw data should be filtered immediately on ingestion
- B) Silver — the Silver layer is the appropriate place for data quality enforcement
- C) Gold — quality gates should be applied only on final output
- D) In all layers simultaneously to prevent propagation of bad data

---

**42.** Which command would you use to compact small Delta files and co-locate related data to improve query performance on a `customer_id` filter?

- A) `OPTIMIZE orders ZORDER BY (customer_id)`
- B) `REORG TABLE orders APPLY (PURGE)`
- C) `COMPACT TABLE orders BY customer_id`
- D) `OPTIMIZE orders CLUSTER BY (customer_id)`

---

**43.** A data engineer needs to process new files from cloud storage every hour as they arrive, without reprocessing already-seen files. The simplest Databricks-native approach is:

- A) Write a Python script that lists files, filters by modification time, and reads only new ones
- B) Use `COPY INTO` with `FORMAT_OPTIONS` to skip known files
- C) Use Auto Loader with `trigger(availableNow=True)` on a scheduled Job
- D) Re-read the entire directory with `spark.read` and use `MERGE` to deduplicate

---

**44.** What is the role of a **checkpoint** in Structured Streaming?

- A) It saves a snapshot of the cluster state so it can be resumed after failure
- B) It stores the stream's progress (offsets processed) and state so the query can recover exactly from where it left off
- C) It archives completed micro-batches to cold storage
- D) It triggers a garbage collection of old streaming state

---

**45.** A data engineer is reviewing a Databricks Workflow run. The job has 3 tasks: A → B → C. Task B fails. What happens by default?

- A) Task C runs anyway because it only depends on A
- B) The job pauses and waits for manual intervention
- C) Task C is skipped (not run), and the job run is marked as FAILED
- D) Task B is automatically retried 3 times before the job fails

---

## ✅ Answer Key + Explanations

| Q | Answer | Explanation |
|---|---|---|
| 1 | **B** | The Lakehouse combines data lake storage flexibility (any format, any scale) with warehouse reliability (ACID, schema enforcement, performance). |
| 2 | **C** | Unity Catalog is the cross-workspace governance layer. Workspace-local Hive Metastore is isolated to one workspace. |
| 3 | **B** | Unity Catalog hierarchy: Metastore → Catalog → Schema → Table (3-level namespace: `catalog.schema.table`). |
| 4 | **B** | `optimizeWrite` coalesces output partitions to produce right-sized files automatically, avoiding the small-file problem. |
| 5 | **C** | `VACUUM` removes files no longer referenced by the transaction log. `OPTIMIZE` compacts files but doesn't remove old ones. |
| 6 | **B** | Delta Lake enforces a minimum 7-day retention (168 hours) unless you explicitly set `spark.databricks.delta.retentionDurationCheck.enabled = false`. |
| 7 | **C** | Correct syntax is `VERSION AS OF <n>`. Option D is the RESTORE command (modifies the table), not a read. |
| 8 | **B** | CDF adds `_change_type` with values: `insert`, `update_preimage`, `update_postimage`, `delete`. |
| 9 | **A** | `WHEN NOT MATCHED BY SOURCE THEN DELETE` deletes target rows that have no matching row in the source. Available in Delta Lake 2.0+. |
| 10 | **B** | Auto Loader uses a checkpoint directory (and optionally RocksDB) to track processed files, enabling exactly-once ingestion. |
| 11 | **C** | `.agg(F.sum("value").alias("total"))` is the correct pattern. `.alias()` goes inside `.agg()`, applied to the aggregate function. |
| 12 | **B** | `F.current_timestamp()` returns the current timestamp. `F.now()` does not exist in PySpark. |
| 13 | **B** | Running total = all preceding rows up to current: `rowsBetween(Window.unboundedPreceding, 0)`. |
| 14 | **A** | `inferSchema` reads the file twice — once to infer types, once to load data. This doubles read cost for large files. |
| 15 | **B** | `SHOW TABLES` lists tables in the current/specified schema. `SHOW DATABASES` lists schemas. |
| 16 | **B** | `DESCRIBE EXTENDED` includes location, table type, owner, creation timestamp, table properties, and statistics. |
| 17 | **B** | `processingTime="30 seconds"` sets the micro-batch interval: a new batch starts every 30 seconds. |
| 18 | **C** | `complete` mode rewrites the full result table each trigger. Required for aggregations without watermarks. |
| 19 | **C** | Watermark defines how long the engine waits for late-arriving events before considering a window closed. |
| 20 | **B** | `availableNow` uses multiple micro-batches to process available data (better parallelism). `once=True` used a single batch. |
| 21 | **C** | `@dlt.expect_or_fail` halts the entire pipeline. `@dlt.expect` logs; `@dlt.expect_or_drop` filters rows. |
| 22 | **B** | A `LIVE TABLE` (non-streaming) is a materialized view — fully recomputed each run from scratch. |
| 23 | **A** | `dlt.read()` = batch (complete table); `dlt.read_stream()` = streaming (incremental, new data only). |
| 24 | **B** | Triggered mode stops the pipeline (and releases the cluster) after all available data is processed. |
| 25 | **B** | Task dependencies define execution order. A fan-in (multiple parents → one child) requires all parents to succeed. |
| 26 | **B** | Databricks Jobs support Quartz cron with an explicit timezone. Setting timezone ensures correct behavior during DST transitions. |
| 27 | **B** | DABs are YAML project configurations for CI/CD deployment of Databricks resources (jobs, pipelines, notebooks, etc.). |
| 28 | **C** | `SELECT` is the privilege to read table data. `USE CATALOG` and `USE SCHEMA` are needed to navigate to the table. |
| 29 | **B** | Delta Sharing is an open protocol for sharing Delta tables externally without requiring access to the Databricks workspace. |
| 30 | **B** | Lakehouse Federation lets Databricks query external databases (Snowflake, Redshift, MySQL, etc.) in-place via Unity Catalog without data movement. |
| 31 | **B** | Managed tables in Unity Catalog store data in the metastore's or catalog's configured cloud storage root, not in DBFS. |
| 32 | **B** | Dropping a managed table deletes both metadata and data files. This is the key distinction from external tables. |
| 33 | **B** | Dropping an external table only removes the metadata entry from Unity Catalog; the underlying data files are untouched. |
| 34 | **B** | `SELECT` alone is insufficient. The user also needs `USE CATALOG` and `USE SCHEMA` to navigate the namespace. |
| 35 | **B** | Column-level lineage is visualized in the **Data Explorer UI** in Databricks. It is not currently queryable via a SQL command. |
| 36 | **A** | Task-level `max_retries` (in the job/task JSON config) controls how many times a task is retried after failure. |
| 37 | **C** | Job Clusters (Jobs compute) are created for a specific job run and terminated afterward — not for interactive use. |
| 38 | **B** | SQL Warehouses are optimized for SQL-based analytics and BI tools (Tableau, Power BI, Looker). |
| 39 | **B** | `_rescued_data` captures fields that don't match the target schema (wrong type, extra columns) so data is never silently dropped. |
| 40 | **B** | `overwriteSchema=true` replaces the existing schema entirely with the new DataFrame schema. Use `mergeSchema=true` to add columns. |
| 41 | **B** | Quality enforcement belongs in the **Silver** layer. Bronze is raw; Gold is business-ready aggregates. |
| 42 | **A** | `OPTIMIZE ... ZORDER BY (customer_id)` co-locates data with similar `customer_id` values in the same files, accelerating range queries. |
| 43 | **C** | Auto Loader natively tracks ingested files via checkpointing, making it the standard solution for incremental cloud file ingestion. |
| 44 | **B** | Checkpoints store stream progress (offsets) and stateful aggregation state, enabling fault-tolerant exactly-once or at-least-once recovery. |
| 45 | **C** | By default, if a task fails (and retries are exhausted), all downstream dependent tasks are skipped and the job is marked FAILED. |
