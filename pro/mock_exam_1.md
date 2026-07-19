# Databricks Certified Data Engineer Professional — Mock Exam 1

> **Format:** 60 multiple-choice questions | **Time:** 120 minutes | **Passing Score:** 70% (42/60)
>
> **Domains:**
> - Section 1: Databricks Tooling — 20% (~12 questions)
> - Section 2: Data Processing — 30% (~18 questions)
> - Section 3: Data Modeling — 20% (~12 questions)
> - Section 4: Security and Governance — 10% (~6 questions)
> - Section 5: Monitoring and Logging — 10% (~6 questions)
> - Section 6: Testing and Deployment — 10% (~6 questions)
>
> **Instructions:** Choose the single best answer for each question. All answers are provided at the end of this file.

---

## Section 1: Databricks Tooling (Questions 1–12)

**Q1.** A data engineer needs to run a notebook on a schedule and wants to pass dynamic parameters such as today's date at runtime. Which Databricks feature is the MOST appropriate?

- A) Databricks SQL Dashboard with scheduled refresh
- B) Databricks Lakeflow Jobs with notebook task parameters
- C) Databricks Repos with CI/CD triggers
- D) Delta Live Tables pipeline with task configuration

---

**Q2.** A team wants to use the Databricks CLI to list all jobs in a workspace and export their configuration to JSON for version control. Which CLI command group should they use?

- A) `databricks fs`
- B) `databricks clusters`
- C) `databricks jobs`
- D) `databricks workspace`

---

**Q3.** Which statement BEST describes the difference between an All-Purpose cluster and a Job cluster in Databricks?

- A) All-Purpose clusters are cheaper per DBU than Job clusters
- B) Job clusters start fresh for each job run and terminate afterward, while All-Purpose clusters persist
- C) Job clusters support interactive notebook execution; All-Purpose clusters do not
- D) All-Purpose clusters automatically scale to zero when idle; Job clusters do not

---

**Q4.** A data engineer wants to define their Databricks job infrastructure as code and version it in Git. Which Databricks feature enables this?

- A) Databricks Repos
- B) Databricks Asset Bundles (DABs)
- C) Databricks SQL Warehouses
- D) Delta Live Tables continuous pipelines

---

**Q5.** Which of the following cluster autoscaling behaviors is TRUE for Databricks?

- A) Autoscaling always scales down to zero when no queries are running
- B) Enhanced autoscaling for streaming workloads is configured via the cluster policy
- C) Autoscaling reacts to pending tasks in the Spark scheduler and adds/removes workers accordingly
- D) Autoscaling is only available on Standard clusters, not on High Concurrency clusters

---

**Q6.** A data engineering team uses Databricks Repos. What is the PRIMARY benefit of using Repos over uploading notebooks directly?

- A) Repos allow notebooks to run faster due to Git-based compilation
- B) Repos integrate with Git providers for version control, branching, and collaboration
- C) Repos automatically document all notebook changes with AI summaries
- D) Repos provide dedicated compute for each notebook branch

---

**Q7.** When using Databricks Lakeflow Jobs, what is the effect of setting the `maximum concurrent runs` parameter to 1?

- A) The job skips if a previous run is still active
- B) The job queues new runs if a previous run is still active, running them sequentially
- C) Only one task within the job can run at a time, even for parallel tasks
- D) The cluster is restricted to one executor node

---

**Q8.** A Databricks workspace uses Unity Catalog. Which layer in the three-level namespace does a Delta table reside in?

- A) Workspace → Database → Table
- B) Catalog → Schema → Table
- C) Metastore → Catalog → Table
- D) Hive → Database → Table

---

**Q9.** Which Databricks feature allows engineers to define compute configurations (e.g., allowed instance types, max DBUs) that are applied to clusters created by users?

- A) Cluster tags
- B) Cluster policies
- C) Instance pools
- D) Spot instance configuration

---

**Q10.** A data engineer wants to reduce cluster startup time for frequent short jobs. Which Databricks feature should they use?

- A) Photon engine
- B) Instance pools
- C) Enhanced autoscaling
- D) Delta caching

---

**Q11.** Which Databricks REST API endpoint is used to create a new Delta Live Tables pipeline?

- A) `POST /api/2.0/jobs/create`
- B) `POST /api/2.0/pipelines`
- C) `POST /api/2.1/clusters/create`
- D) `POST /api/2.0/workflows/runs/submit`

---

**Q12.** A team uses Databricks Workflows (Lakeflow Jobs) with multiple tasks. They want Task B to only run if Task A succeeds, but Task C should run regardless of Task A's outcome. How should they configure this?

- A) Set Task B dependency to Task A with condition `ALL_SUCCESS`; set Task C with condition `ALL_DONE`
- B) Set Task B and Task C both to run after Task A with `ALL_SUCCESS`
- C) Use a separate job for Task C with no dependency
- D) Set Task C to run before Task A

---

## Section 2: Data Processing (Questions 13–30)

**Q13.** A data engineer is reading a large JSON dataset from cloud storage into a Delta table. They want to handle schema evolution automatically without failing the pipeline. Which Auto Loader option should they configure?

- A) `cloudFiles.schemaEvolutionMode = "rescue"`
- B) `cloudFiles.inferColumnTypes = true`
- C) `cloudFiles.schemaEvolutionMode = "addNewColumns"`
- D) `cloudFiles.validateOptions = true`

---

**Q14.** In a Structured Streaming job, a data engineer uses `trigger(availableNow=True)`. What is the behavior?

- A) The stream runs continuously, processing each micro-batch immediately
- B) The stream processes all available data at trigger time and then stops, acting like a batch job
- C) The stream triggers only once per hour
- D) The stream runs until the cluster auto-terminates

---

**Q15.** Which Delta Lake operation reclaims storage by physically removing files that are no longer referenced by the current table version and are older than the retention threshold?

- A) `OPTIMIZE`
- B) `ZORDER BY`
- C) `VACUUM`
- D) `RESTORE`

---

**Q16.** A data engineer wants to merge incoming CDC records into a Delta table. The merge should insert new rows, update changed rows, and delete rows flagged with `operation = 'DELETE'`. Which DML statement is appropriate?

- A) `INSERT OVERWRITE`
- B) `MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED`
- C) `UPDATE ... WHERE`
- D) `COPY INTO`

---

**Q17.** When using `MERGE INTO` for SCD Type 1 updates in Delta Lake, what is the purpose of the `WHEN NOT MATCHED BY SOURCE` clause (introduced in Delta 2.x)?

- A) It handles rows in the source that don't exist in the target
- B) It handles rows in the target that don't exist in the source (e.g., delete stale records)
- C) It matches records on a secondary key
- D) It creates a new partition for unmatched rows

---

**Q18.** A streaming pipeline reads from Kafka and writes to a Delta table using foreachBatch. Inside the foreachBatch function, the engineer calls `df.write.format("delta").mode("append").save(path)`. What issue may arise?

- A) The function will not work because foreachBatch doesn't support Delta format
- B) Exactly-once semantics are lost because Delta's idempotent writes require using `.save()` with the checkpoint
- C) This is idempotent by default due to Delta's transaction log
- D) foreachBatch with Delta append mode is non-idempotent; the engineer should use `merge` or `replaceWhere` for exactly-once guarantees

---

**Q19.** A data engineer runs `OPTIMIZE events ZORDER BY (user_id, event_date)`. What is the PRIMARY benefit of this operation?

- A) It sorts the table globally by `user_id` and `event_date` in ascending order
- B) It co-locates data with similar `user_id` and `event_date` values in the same files, improving data skipping
- C) It creates a secondary index on `user_id` and `event_date`
- D) It compresses all Parquet files using Snappy codec

---

**Q20.** Which of the following statements about Delta Lake Change Data Feed (CDF) is TRUE?

- A) CDF must be enabled at the metastore level before use
- B) CDF captures all row-level changes (INSERT, UPDATE, DELETE) and records `_change_type`, `_commit_version`, and `_commit_timestamp`
- C) CDF only captures INSERT operations
- D) CDF is automatically enabled on all new Delta tables

---

**Q21.** A data engineer needs to read only rows changed since version 10 of a Delta table using CDF. Which code is correct?

- A) `spark.read.format("delta").option("readChangeData", True).option("startingVersion", 10).load(path)`
- B) `spark.readStream.format("delta").option("readChangeData", True).load(path)`
- C) `spark.read.format("delta").option("versionAsOf", 10).load(path)`
- D) `spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)`

---

**Q22.** A DLT pipeline is configured with a `CONSTRAINT` that uses `ON VIOLATION DROP ROW`. What happens when a record violates this constraint?

- A) The pipeline fails and all records are rolled back
- B) The violating record is quarantined to a separate error table
- C) The violating record is silently dropped and metrics are recorded
- D) The record is logged to DBFS but still written to the target table

---

**Q23.** In Delta Live Tables, what is the difference between a `STREAMING TABLE` and a `MATERIALIZED VIEW`?

- A) Streaming tables process only new data incrementally; materialized views recompute from all data on each update
- B) Materialized views support streaming sources; streaming tables do not
- C) Streaming tables are faster because they skip the Delta transaction log
- D) There is no functional difference; they are aliases

---

**Q24.** A data engineer uses Auto Loader to ingest files. To avoid reprocessing files after a cluster restart, what mechanism does Auto Loader use?

- A) A watermark timestamp in the DataFrame
- B) A checkpoint directory storing metadata about processed files
- C) A manifest file in the source directory
- D) A VACUUM log of ingested files

---

**Q25.** A `foreachBatch` function in Structured Streaming must write data to both a Delta table and an external PostgreSQL database. What is the correct approach to maintain fault tolerance?

- A) Write to PostgreSQL first, then Delta; rely on PostgreSQL transactions
- B) Write to Delta first using idempotent writes, then use a conditional write to PostgreSQL using the batch ID
- C) Use `trigger(once=True)` to prevent duplicate processing
- D) Disable checkpointing to speed up the writes

---

**Q26.** Which PySpark API call correctly applies a window function to compute a running total of `sales` ordered by `date` within each `region`?

- A) `df.withColumn("running_total", F.sum("sales").over(Window.partitionBy("region").orderBy("date")))`
- B) `df.groupBy("region").agg(F.sum("sales").alias("running_total"))`
- C) `df.withColumn("running_total", F.sum("sales").over(Window.orderBy("date")))`
- D) `df.withColumn("running_total", F.cumsum("sales").over("region"))`

---

**Q27.** A data engineer notices their Spark job has a single task that takes 10x longer than all other tasks. What is the MOST likely cause?

- A) The cluster has insufficient memory
- B) Data skew — one partition contains significantly more data than others
- C) The driver node is overloaded with collect() calls
- D) Too many shuffle partitions are configured

---

**Q28.** Which configuration parameter controls the number of output files after a shuffle operation in Spark?

- A) `spark.sql.files.maxPartitionBytes`
- B) `spark.sql.shuffle.partitions`
- C) `spark.default.parallelism`
- D) `spark.sql.adaptive.coalescePartitions.enabled`

---

**Q29.** A data engineer wants to prevent small file accumulation in a Delta table that receives frequent micro-batch streaming writes. Which combination of features should they use?

- A) `OPTIMIZE` scheduled job + `spark.databricks.delta.optimizeWrite.enabled = true`
- B) Increasing shuffle partitions + disabling Delta caching
- C) Using COPY INTO instead of Auto Loader
- D) Enabling `spark.sql.adaptive.enabled` only

---

**Q30.** A data engineer needs to perform a deduplication on a streaming DataFrame, keeping only the latest record per `device_id`. Which approach is correct?

- A) Use `dropDuplicates(["device_id"])` with a watermark on the event timestamp
- B) Use `DISTINCT` in a SQL query inside foreachBatch
- C) Use `groupBy("device_id").agg(F.last("value"))` on the stream
- D) Use `MERGE INTO` with `WHEN MATCHED THEN DELETE`

---

## Section 3: Data Modeling (Questions 31–42)

**Q31.** A data engineer implements a Medallion Architecture. Which layer is responsible for storing raw, unprocessed data exactly as ingested from the source?

- A) Gold layer
- B) Silver layer
- C) Bronze layer
- D) Staging layer

---

**Q32.** In a Slowly Changing Dimension Type 2 (SCD2) implementation using Delta Lake, which columns are typically added to track history?

- A) `created_at` and `deleted_at`
- B) `effective_start_date`, `effective_end_date`, and `is_current`
- C) `version_number` and `checksum`
- D) `insert_timestamp` only

---

**Q33.** A data engineer uses `MERGE INTO` to implement SCD Type 2 for a `customers` table. For existing records that have changed, what MERGE action should be used to close the old record?

- A) `WHEN MATCHED AND source.hash != target.hash THEN UPDATE SET target.is_current = false, target.effective_end_date = current_date()`
- B) `WHEN MATCHED THEN DELETE`
- C) `WHEN NOT MATCHED THEN INSERT`
- D) `WHEN MATCHED THEN UPDATE SET target.effective_start_date = current_date()`

---

**Q34.** Which of the following is a key benefit of using a star schema in the Gold layer of a Medallion Architecture?

- A) It reduces the number of tables compared to 3NF normalized schemas
- B) It optimizes for analytical query performance by minimizing joins through denormalization
- C) It enforces strict referential integrity using foreign key constraints
- D) It enables faster write throughput for OLTP workloads

---

**Q35.** A Delta table has grown to millions of small Parquet files over time. Which combination of operations will BEST consolidate files AND improve query performance through data skipping?

- A) `VACUUM` followed by `RESTORE`
- B) `OPTIMIZE` followed by `ZORDER BY`
- C) `COPY INTO` followed by `COMPACT`
- D) `REFRESH TABLE` followed by `ANALYZE TABLE`

---

**Q36.** A data engineer wants to implement data deduplication at the Silver layer using Delta Lake. The source sends duplicate records with the same `event_id`. Which is the MOST efficient approach for large-scale deduplication?

- A) `SELECT DISTINCT * FROM silver_table`
- B) Use `MERGE INTO silver_table USING new_data ON silver_table.event_id = new_data.event_id WHEN NOT MATCHED THEN INSERT *`
- C) Drop and recreate the table after each batch
- D) Use `INSERT OVERWRITE` with a GROUP BY

---

**Q37.** What does `DESCRIBE HISTORY delta.\`/path/to/table\`` return?

- A) The schema of the table as of the latest version
- B) The table's partition structure and column statistics
- C) All committed operations on the table with version, timestamp, operation, and user details
- D) A list of all files currently in the Delta table

---

**Q38.** A data engineer uses Delta Lake time travel with `VERSION AS OF 5`. What happens if they subsequently run `VACUUM` with the default retention period?

- A) VACUUM has no effect on time travel capabilities
- B) VACUUM removes files older than 7 days (default), potentially making versions before the retention window inaccessible
- C) VACUUM deletes all historical versions except the latest
- D) VACUUM removes transaction log entries but keeps Parquet files

---

**Q39.** In a DLT pipeline, the `APPLY CHANGES INTO` command is used for CDC. What is the default behavior when multiple change records have the same key and timestamp?

- A) All records are written; duplicates are flagged
- B) The last record in the batch is applied
- C) The command fails with a duplicate key error
- D) The record with the lowest sequence value is applied

---

**Q40.** A data engineer needs to partition a Delta table by `event_date` for a time-series dataset. Which statement about partitioning is TRUE?

- A) Partitioning by a high-cardinality column like `user_id` improves performance
- B) Partitioning by `event_date` creates separate directories per date, enabling partition pruning for date-range queries
- C) Delta Lake does not support partitioning
- D) Partitioning always improves write performance regardless of column cardinality

---

**Q41.** What is a Bloom filter index in Delta Lake, and when should it be used?

- A) A B-tree index for range queries on numeric columns
- B) A probabilistic data structure that quickly determines if a value MIGHT be in a file, useful for equality lookups on high-cardinality columns
- C) A hash index for primary key lookups in joins
- D) An inverted index for full-text search within Delta tables

---

**Q42.** A `SHALLOW CLONE` of a Delta table is created. Which of the following is TRUE?

- A) Shallow clone copies all Parquet data files to the new location
- B) Shallow clone creates an independent copy with its own transaction log pointing to the original data files
- C) Shallow clone is identical to `DEEP CLONE` in all behaviors
- D) Shallow clone cannot be queried; it is only used for backup purposes

---

## Section 4: Security and Governance (Questions 43–48)

**Q43.** In Unity Catalog, which object is used to grant fine-grained row-level security by creating a view that filters data based on the current user's attributes?

- A) Row-level access policy
- B) Dynamic view using `current_user()` or `is_account_group_member()`
- C) Column mask function
- D) Data classification label

---

**Q44.** A data engineer needs to mask PII columns (e.g., `email`) in a Delta table so that users in the `analysts` group see `****` instead of actual values. Which Unity Catalog feature enables this?

- A) Table ACLs with DENY permission
- B) Row filters on the `email` column
- C) Column masking policies using `ALTER TABLE ... ALTER COLUMN ... SET MASK`
- D) Data encryption at the storage account level

---

**Q45.** Which privilege must be granted for a user to read data from a Delta table in Unity Catalog?

- A) `USAGE` on the catalog and schema, plus `SELECT` on the table
- B) `READ` on the table only
- C) `EXECUTE` on the schema and `SELECT` on the table
- D) `OWNER` privilege on the table

---

**Q46.** A data engineer wants to audit who accessed a specific Delta table over the last 30 days in a Unity Catalog workspace. Which feature provides this capability?

- A) Spark event logs from the cluster
- B) Delta table `DESCRIBE HISTORY`
- C) Unity Catalog system tables (e.g., `system.access.audit`)
- D) Databricks job run logs

---

**Q47.** In a Unity Catalog metastore, what is a `CREDENTIAL` object used for?

- A) Storing user passwords for JDBC connections
- B) Providing cloud storage access credentials (e.g., service principal, IAM role) for external locations
- C) Managing API tokens for Databricks REST API calls
- D) Encrypting Delta table data at rest

---

**Q48.** A data engineer wants to ensure all data written to an external S3 bucket from Databricks is encrypted using a customer-managed key. Which layer handles this configuration?

- A) Delta table properties (`delta.encryption = true`)
- B) Spark configuration at cluster startup
- C) The cloud storage bucket/account encryption settings (e.g., AWS KMS or Azure Key Vault)
- D) Unity Catalog column masking

---

## Section 5: Monitoring and Logging (Questions 49–54)

**Q49.** A data engineer notices that a Spark job's Stage 3 has one task that takes 5 minutes while all other tasks in the same stage complete in 10 seconds. Where in the Databricks UI should they investigate?

- A) Cluster event log
- B) Spark UI → Stages tab → Task metrics for Stage 3
- C) DBFS file browser
- D) SQL query history

---

**Q50.** Which Spark UI tab shows the DAG (Directed Acyclic Graph) of transformations and helps identify unnecessary wide transformations (shuffles)?

- A) Executors tab
- B) Storage tab
- C) Jobs tab → Stage DAG visualization
- D) Environment tab

---

**Q51.** A data engineer wants to set up an alert when a Delta Live Tables pipeline fails. What is the RECOMMENDED approach in Databricks?

- A) Poll the DLT REST API every minute with a cron job
- B) Configure email/webhook notifications on the DLT pipeline or the Lakeflow Job that runs it
- C) Write a custom health-check notebook that runs every 5 minutes
- D) Enable verbose logging on the cluster and monitor DBFS logs

---

**Q52.** Which metrics can be monitored from the Delta Live Tables pipeline event log to assess data quality?

- A) CPU utilization and memory usage per executor
- B) Row counts, constraint violation counts, and records dropped per expectation
- C) Network I/O and shuffle read/write sizes
- D) DBFS file sizes and partition counts

---

**Q53.** A data engineer wants to track the number of records processed per micro-batch in a Structured Streaming job over time. Which built-in feature provides per-batch metrics?

- A) Spark UI → Storage tab
- B) Streaming query progress logs accessible via `query.recentProgress`
- C) DBFS event logs
- D) Delta table `DESCRIBE HISTORY`

---

**Q54.** A production job consistently takes 45 minutes but SLA requires completion in 30 minutes. Using the Ganglia metrics and Spark UI, the engineer sees high GC (Garbage Collection) time. What is the MOST likely root cause and solution?

- A) Too many shuffle partitions; reduce `spark.sql.shuffle.partitions`
- B) Executor memory is insufficient, causing frequent GC; increase executor memory or use memory-optimized instances
- C) The driver is doing too much work; increase driver memory
- D) The cluster has too many worker nodes causing coordination overhead

---

## Section 6: Testing and Deployment (Questions 55–60)

**Q55.** A data engineer wants to implement unit tests for a PySpark transformation function. Which Python testing framework integrates BEST with Databricks for local and CI/CD testing?

- A) Selenium
- B) pytest with `pyspark.testing` utilities (e.g., `assertDataFrameEqual`)
- C) unittest with hardcoded Spark sessions
- D) Robot Framework

---

**Q56.** A team deploys Databricks jobs using Databricks Asset Bundles (DABs). Which file format defines the bundle configuration?

- A) JSON (`.json`)
- B) YAML (`.yaml` / `databricks.yml`)
- C) HCL (Terraform format)
- D) TOML (`.toml`)

---

**Q57.** In a CI/CD pipeline for Databricks using GitHub Actions, at which stage should integration tests against a staging Databricks workspace be run?

- A) Before any code linting or unit tests
- B) After unit tests pass and before merging to the main branch / deploying to production
- C) Only after deploying to production to validate the release
- D) During the code review phase using static analysis only

---

**Q58.** A data engineer uses `databricks bundle deploy --target prod` to deploy a job. The job configuration references a cluster policy by name. What happens if the cluster policy name does not exist in the target workspace?

- A) The job is deployed without a cluster policy, using defaults
- B) The deployment fails with a configuration validation error
- C) The job is created in PAUSED state until the policy is manually assigned
- D) The bundle automatically creates the missing cluster policy

---

**Q59.** Which approach BEST enables a data engineer to roll back a bad deployment of a Databricks job without downtime?

- A) Manually edit the job configuration in the UI
- B) Redeploy the previous bundle version from Git tag using `databricks bundle deploy`
- C) Delete the job and recreate it from scratch
- D) Restore the Delta table to a previous version

---

**Q60.** A data engineer wants to validate that a DLT pipeline produces the correct row counts and schema after a code change. What is the BEST practice?

- A) Run the pipeline in production and check logs manually
- B) Use DLT expectations (`CONSTRAINT`) in the pipeline definition as automated data quality checks, and run pipeline in development mode against sample data before promoting
- C) Compare source and target file sizes in DBFS
- D) Use a Databricks SQL dashboard to spot-check records after each deployment

---

---

# Answer Key — Mock Exam 1

> Each answer includes the correct option, a detailed explanation, and relevant knowledge points for deep understanding.

---

### Section 1: Databricks Tooling

**A1. B) Databricks Lakeflow Jobs with notebook task parameters**
> **Explanation:** Lakeflow Jobs (formerly Databricks Workflows) support task-level parameters that are passed at runtime, including dynamic values like `{{ds}}` for today's date. This is the standard way to parameterize scheduled notebook runs. SQL Dashboards refresh data but don't pass runtime parameters to notebooks. DLT pipelines have their own configuration scope and are not designed for arbitrary parameter passing.
> **Knowledge Points:** Lakeflow Jobs task parameters; `dbutils.widgets.get()` in notebooks; `{{ds}}`, `{{start_date}}` macros; job run-time configuration.

**A2. C) `databricks jobs`**
> **Explanation:** The `databricks jobs` CLI command group provides subcommands like `list`, `get`, and `create` that allow engineers to manage jobs programmatically. The output can be piped to JSON files for version control. `databricks fs` manages DBFS files, `databricks clusters` manages compute, and `databricks workspace` manages workspace objects like notebooks.
> **Knowledge Points:** Databricks CLI v2; `databricks jobs list --output JSON`; GitOps practices for job configuration management.

**A3. B) Job clusters start fresh for each job run and terminate afterward, while All-Purpose clusters persist**
> **Explanation:** Job clusters are ephemeral — they are created at job start and terminated when the job completes, making them cost-efficient for automated workloads. All-Purpose clusters persist until manually terminated or auto-terminated after inactivity, and they support interactive development. Job clusters are generally cheaper per DBU for automated tasks.
> **Knowledge Points:** Cluster lifecycle; cost optimization; automated vs. interactive workloads; auto-termination vs. ephemeral clusters.

**A4. B) Databricks Asset Bundles (DABs)**
> **Explanation:** Databricks Asset Bundles allow teams to define jobs, pipelines, clusters, and other workspace resources as code in YAML files, which can be version-controlled in Git. This supports Infrastructure-as-Code (IaC) practices and CI/CD deployment workflows. Repos manage notebook source code but not job/pipeline infrastructure configuration.
> **Knowledge Points:** DABs (`databricks.yml`); `databricks bundle deploy`; IaC for Databricks; difference between Repos (code) and Bundles (infrastructure).

**A5. C) Autoscaling reacts to pending tasks in the Spark scheduler and adds/removes workers accordingly**
> **Explanation:** Databricks autoscaling monitors the Spark scheduler's pending task queue. When tasks are pending and no workers are available, it scales up. When workers are idle, it scales down. Enhanced autoscaling for streaming specifically looks at input rates and processing lag. Autoscaling does not necessarily scale to zero (that requires separate auto-termination or serverless configurations).
> **Knowledge Points:** Standard autoscaling vs. Enhanced autoscaling; `spark.databricks.aggressiveWindowDownS`; streaming autoscaling behavior; Spot vs. On-Demand with autoscaling.

**A6. B) Repos integrate with Git providers for version control, branching, and collaboration**
> **Explanation:** Databricks Repos sync with Git repositories (GitHub, GitLab, Bitbucket, Azure DevOps), enabling version control, branching, pull requests, and collaborative development workflows. They don't affect execution speed or provide dedicated compute — these are workspace-level features for code management.
> **Knowledge Points:** Databricks Repos; Git integration; branch-based development; CI/CD with Repos; difference between Repos and Workspace notebooks.

**A7. B) The job queues new runs if a previous run is still active, running them sequentially**
> **Explanation:** When `maximum concurrent runs` is set to 1, Databricks queues new triggered runs while a previous run is active, ensuring they execute sequentially. This is different from skipping (which would require additional configuration). This setting is important for jobs with sequential dependencies or shared state.
> **Knowledge Points:** Job concurrency settings; queue behavior; idempotent job design; avoiding race conditions in pipelines.

**A8. B) Catalog → Schema → Table**
> **Explanation:** Unity Catalog uses a three-level namespace: `catalog.schema.table`. This is a key architectural difference from the legacy Hive metastore which used a two-level namespace (`database.table`). The metastore itself is the top-level container that holds catalogs, but it is not part of the object reference path.
> **Knowledge Points:** Unity Catalog three-level namespace; `catalog.schema.table` referencing; legacy Hive metastore (two-level) vs. Unity Catalog (three-level); `USE CATALOG`, `USE SCHEMA` commands.

**A9. B) Cluster policies**
> **Explanation:** Cluster policies define rules for allowed configurations (instance types, max workers, DBU limits, Spark configs) that apply when creating clusters. They enforce organizational standards, control costs, and simplify the cluster creation experience for users. Tags are metadata labels; Instance pools pre-warm VMs but don't restrict configurations; Spot instances are a pricing option.
> **Knowledge Points:** Cluster policy JSON definition; `dbeq`, `dbrange`, `dbregex` constraints; policy families; cost governance.

**A10. B) Instance pools**
> **Explanation:** Instance pools maintain a set of pre-warmed, idle VM instances ready for immediate use. When a job cluster is created from a pool, it attaches to an idle instance instead of waiting for cloud VM provisioning (which typically takes 3-5 minutes), dramatically reducing startup time. Photon improves query execution speed; Delta caching improves read performance; autoscaling manages worker count.
> **Knowledge Points:** Instance pool configuration; `idle_instance_autotermination_minutes`; pool vs. non-pool cluster startup time; cost implications of maintaining idle instances.

**A11. B) `POST /api/2.0/pipelines`**
> **Explanation:** The DLT REST API uses `/api/2.0/pipelines` for CRUD operations on Delta Live Tables pipelines. `/api/2.0/jobs/create` creates Lakeflow jobs, `/api/2.1/clusters/create` creates clusters, and `/api/2.0/workflows/runs/submit` submits one-time job runs.
> **Knowledge Points:** Databricks REST API endpoints; DLT pipeline management via API; `pipeline_id`; difference between Jobs API and Pipelines API.

**A12. A) Set Task B dependency to Task A with condition `ALL_SUCCESS`; set Task C with condition `ALL_DONE`**
> **Explanation:** Databricks Lakeflow Jobs support dependency conditions: `ALL_SUCCESS` (run only if all upstream tasks succeeded), `ALL_DONE` (run regardless of upstream outcome), `AT_LEAST_ONE_SUCCESS`, and `NONE_FAILED`. Setting Task B with `ALL_SUCCESS` on Task A ensures it only runs on success; setting Task C with `ALL_DONE` ensures it always runs.
> **Knowledge Points:** Task dependency conditions; `ALL_SUCCESS`, `ALL_DONE`, `NONE_FAILED`, `AT_LEAST_ONE_SUCCESS`; DAG-based workflow design; error handling in multi-task jobs.

---

### Section 2: Data Processing

**A13. C) `cloudFiles.schemaEvolutionMode = "addNewColumns"`**
> **Explanation:** Setting `cloudFiles.schemaEvolutionMode` to `addNewColumns` allows Auto Loader to automatically add new columns to the target schema when new fields appear in incoming files, without failing the stream. The `rescue` mode stores unmatched columns in a `_rescued_data` JSON column. `inferColumnTypes` controls type inference but not schema evolution behavior.
> **Knowledge Points:** Auto Loader schema evolution modes (`addNewColumns`, `rescue`, `failOnNewColumns`, `none`); `_rescued_data` column; schema inference with `cloudFiles.schemaLocation`.

**A14. B) The stream processes all available data at trigger time and then stops, acting like a batch job**
> **Explanation:** `trigger(availableNow=True)` (introduced to replace `trigger(once=True)`) processes all data available in the source at the time of the trigger in multiple micro-batches (for efficiency), then terminates the stream. This is ideal for scheduled incremental batch processing with streaming semantics. It differs from `trigger(once=True)` which processes in a single micro-batch.
> **Knowledge Points:** Streaming triggers: `processingTime`, `once`, `availableNow`, `continuous`; difference between `once` and `availableNow`; checkpointing with `availableNow`.

**A15. C) `VACUUM`**
> **Explanation:** `VACUUM` removes Parquet files that are no longer referenced by the current Delta table version AND are older than the retention threshold (default: 7 days = 168 hours). `OPTIMIZE` compacts small files