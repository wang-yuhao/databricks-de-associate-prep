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
> **Explanation:** `VACUUM` removes Parquet files that are no longer referenced by the current Delta table version AND are older than the retention threshold (default: 7 days = 168 hours). `OPTIMIZE` compacts small files into larger ones but does not delete old/unreferenced files. `ZORDER BY` is a clause used with `OPTIMIZE` to co-locate related data for data skipping. `RESTORE` reverts a table to a previous version — the opposite of permanently removing history.
> **Knowledge Points:** `VACUUM` default retention (168 hours); `VACUUM RETAIN <N> HOURS`; the tradeoff between `VACUUM` and time travel — vacuuming aggressively breaks the ability to time travel or read older CDF versions; `spark.databricks.delta.retentionDurationCheck.enabled`.

---

### Section 2: Data Processing (continued)

**A16. B) `MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED`**
> **Explanation:** `MERGE INTO` is purpose-built for CDC-style upserts — it evaluates each incoming row against the target on a join key and lets you branch behavior: `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`, and `WHEN MATCHED AND operation = 'DELETE' THEN DELETE`. `INSERT OVERWRITE` replaces entire partitions/tables (no row-level matching), plain `UPDATE ... WHERE` can't insert new rows, and `COPY INTO` is an ingestion command with no update/delete logic.
> **Knowledge Points:** `MERGE INTO` syntax; combining insert/update/delete in one atomic statement; using a condition inside `WHEN MATCHED AND <condition>` to route deletes; CDC/SCD1 implementation pattern.

**A17. B) It handles rows in the target that don't exist in the source (e.g., delete stale records)**
> **Explanation:** `WHEN NOT MATCHED BY SOURCE` is the "reverse" clause to the standard `WHEN NOT MATCHED` (which handles source rows missing from the target, i.e., new inserts). `WHEN NOT MATCHED BY SOURCE` catches target rows that have disappeared from the source feed — commonly used to delete or soft-delete records that no longer exist upstream. This closes a long-standing gap where `MERGE INTO` alone couldn't express "delete anything not present in the latest source snapshot" without a separate anti-join.
> **Knowledge Points:** `WHEN NOT MATCHED` vs. `WHEN NOT MATCHED BY SOURCE`; full outer merge semantics; using this clause to implement "delete removed records" CDC patterns; requires Delta Lake 2.3+/DBR 12.1+.

**A18. D) foreachBatch with Delta append mode is non-idempotent; the engineer should use `merge` or `replaceWhere` for exactly-once guarantees**
> **Explanation:** Structured Streaming guarantees each micro-batch is delivered to `foreachBatch` *at least once* — on task retry or job restart, the same batch can be reprocessed. A plain `.mode("append")` write has no concept of "I already wrote this batch," so a retry produces duplicate rows. To get effectively-once behavior, the engineer should either `MERGE` on a unique key (so retries just re-apply the same final state) or use `replaceWhere` scoped to the batch's partition/predicate, and can also use the batch ID (`batchId` parameter of `foreachBatch`) to track which batches have already been committed.
> **Knowledge Points:** `foreachBatch(func)` semantics; at-least-once vs. exactly-once; idempotent sink design; using `batchId` for dedup tracking; Delta `MERGE` as the standard idempotency mechanism for custom sinks.

**A19. B) It co-locates data with similar `user_id` and `event_date` values in the same files, improving data skipping**
> **Explanation:** Z-ordering does not produce a strict global sort (option A is a common misconception) — it uses a space-filling curve algorithm to interleave the specified columns so that rows with similar values end up physically co-located within the same files. Delta's file-level min/max statistics then let the query planner skip entire files that can't match a filter, without scanning them. It's not an index (option C) and not about compression (option D) — those are separate concerns from `OPTIMIZE`'s file compaction.
> **Knowledge Points:** Z-order multi-dimensional clustering; data skipping via file statistics (`min`/`max`/`null count`); Z-order vs. true sort order; diminishing returns of Z-ordering beyond 3–4 columns.

**A20. B) CDF captures all row-level changes (INSERT, UPDATE, DELETE) and records `_change_type`, `_commit_version`, and `_commit_timestamp`**
> **Explanation:** CDF is enabled per-table (`TBLPROPERTIES (delta.enableChangeDataFeed = true)`), not at the metastore level, and it is **not** on by default for new tables — you must explicitly turn it on. Once enabled, every `INSERT`, `UPDATE`, and `DELETE` is captured, with updates split into `update_preimage` (before) and `update_postimage` (after) rows so downstream consumers can see exactly what changed.
> **Knowledge Points:** `delta.enableChangeDataFeed`; `_change_type` values (`insert`, `update_preimage`, `update_postimage`, `delete`); CDF storage overhead (extra small CDC files); reading CDF via SQL (`table_changes()`) or DataFrame reader.

**A21. A) `spark.read.format("delta").option("readChangeData", True).option("startingVersion", 10).load(path)`** *(intended answer — see note below)*
> **Explanation:** Conceptually, option A is structured correctly for CDF reads: batch `read` (not `readStream`, which is for continuously streaming the change feed) combined with a `startingVersion` bound. Option B uses `readStream`, which would continuously tail changes rather than answer "changes since version 10" as a bounded query. Options C and D use `versionAsOf`/`timestampAsOf`, which are **time travel** options that return a full snapshot, not a change feed.
> ⚠️ **Correction worth knowing for the real exam:** the actual Delta option name is **`readChangeFeed`**, not `readChangeData`. The correct real-world code is:
> ```python
> spark.read.format("delta") \
>   .option("readChangeFeed", "true") \
>   .option("startingVersion", 10) \
>   .load(path)
> ```
> This mock question has a typo in the option name across all four choices, so don't memorize `readChangeData` — memorize `readChangeFeed`.
> **Knowledge Points:** `readChangeFeed` (correct option name); `startingVersion`/`endingVersion` or `startingTimestamp`/`endingTimestamp`; batch vs. streaming CDF reads; contrast with time travel options (`versionAsOf`, `timestampAsOf`).

**A22. C) The violating record is silently dropped and metrics are recorded**
> **Explanation:** DLT/Lakeflow Declarative Pipelines expectations support three violation behaviors: `ON VIOLATION DROP ROW` (silently drop the bad row, but count it in the pipeline event log metrics), `ON VIOLATION FAIL UPDATE` (halt the entire pipeline run), and simply omitting the `ON VIOLATION` clause (keep the row but track the violation count — "warn" mode). There is no automatic quarantine table built into `DROP ROW` — if you want a quarantine pattern, you build it yourself (e.g., a second expectation set with the inverse condition writing to a separate table).
> **Knowledge Points:** Three expectation behaviors — warn (default), `DROP ROW`, `FAIL UPDATE`; expectation metrics visible in the DLT event log and pipeline UI; building a manual quarantine pattern with `expect_or_drop` + inverse filter.

**A23. A) Streaming tables process only new data incrementally; materialized views recompute from all data on each update**
> **Explanation:** A streaming table is built on Structured Streaming semantics — each pipeline update reads and processes only the new data that has arrived since the last run. A materialized view is built from a batch query, and Lakeflow Declarative Pipelines decides how much of it needs to be refreshed based on what upstream data changed (sometimes incrementally, sometimes a fuller recompute), but conceptually it's "recompute based on dependency staleness," not a strict append-only incremental stream.
> **Knowledge Points:** Streaming table vs. materialized view vs. view (three DLT object types); when to choose each based on source characteristics (append-only stream vs. aggregation vs. reusable query logic); cost/latency tradeoffs.

**A24. B) A checkpoint directory storing metadata about processed files**
> **Explanation:** Auto Loader maintains a checkpoint (using RocksDB-backed state by default) that tracks exactly which source files have already been ingested. On restart, it consults this checkpoint to pick up only new files, avoiding reprocessing. This is separate from Structured Streaming's own checkpoint (which tracks offsets/progress) — Auto Loader specifically needs file-level tracking since cloud storage doesn't have native offsets like Kafka.
> **Knowledge Points:** `cloudFiles` checkpoint location (`checkpointLocation` option); RocksDB-based file listing state; directory listing mode vs. file notification mode; why deleting a checkpoint causes full reprocessing.

**A25. B) Write to Delta first using idempotent writes, then use a conditional write to PostgreSQL using the batch ID**
> **Explanation:** Since PostgreSQL is not part of Delta's transaction log, you can't get atomic all-or-nothing guarantees across both sinks. The standard pattern: write to Delta first (which is naturally idempotent if you use `MERGE` or check the batch ID), and then write to PostgreSQL guarded by a check against the batch ID (e.g., a small tracking table in Postgres recording "have I already applied batch N?") so that a retried batch doesn't double-write to Postgres.
> **Knowledge Points:** Multi-sink `foreachBatch` patterns; `batchId` parameter for idempotency tracking; the general principle of ordering writes so the "cheap to make idempotent" sink goes first; two-phase-commit-like patterns for heterogeneous sinks.

**A26. A) `df.withColumn("running_total", F.sum("sales").over(Window.partitionBy("region").orderBy("date")))`**
> **Explanation:** A running total "within each region, ordered by date" needs both `partitionBy("region")` (reset the running total per region) and `orderBy("date")` (accumulate in date order). Option C is missing `partitionBy`, so it would compute one running total across the entire DataFrame regardless of region. Option B uses `groupBy`, which collapses rows into one aggregate per region rather than producing a running total per row. Option D invents a nonexistent `F.cumsum` function — PySpark's approach to running totals is always `F.sum(...).over(window)`, not a dedicated cumulative-sum function.
> **Knowledge Points:** `Window.partitionBy().orderBy()`; default window frame for aggregate functions with `orderBy` (`rowsBetween(Window.unboundedPreceding, Window.currentRow)`); difference between `groupBy` (collapses rows) and window functions (preserve row count while adding a computed column).

**A27. B) Data skew — one partition contains significantly more data than others**
> **Explanation:** When one task takes dramatically longer than its siblings in the *same stage*, that's the textbook signature of data skew: one partition (often from a `groupBy` or `join` on a low-cardinality or unevenly distributed key) holds far more rows than the others, so its task has far more work. Insufficient memory would typically cause failures/spill across many tasks, not one slow outlier; too many shuffle partitions would create many small, fast tasks rather than one slow one.
> **Knowledge Points:** Data skew symptoms in Spark UI (Stages tab, "task duration" distribution, max vs. median task time); mitigation techniques — salting keys, Adaptive Query Execution (AQE) skew join optimization, broadcasting the smaller side of a join; `spark.sql.adaptive.skewJoin.enabled`.

**A28. B) `spark.sql.shuffle.partitions`**
> **Explanation:** This config sets the number of partitions used when Spark shuffles data for joins and aggregations (default 200). `spark.sql.files.maxPartitionBytes` controls input file split size for reads, `spark.default.parallelism` is the legacy RDD-API equivalent (mostly superseded by the shuffle.partitions setting for DataFrame/SQL workloads), and `spark.sql.adaptive.coalescePartitions.enabled` is an AQE feature that *automatically adjusts* the number of post-shuffle partitions at runtime — it works alongside `shuffle.partitions` rather than replacing the concept of "how many output files."
> **Knowledge Points:** `spark.sql.shuffle.partitions` default (200) and why it's often mistuned for small or huge datasets; Adaptive Query Execution's dynamic partition coalescing as a modern alternative to manual tuning; relationship between shuffle partition count and output file count.

**A29. A) `OPTIMIZE` scheduled job + `spark.databricks.delta.optimizeWrite.enabled = true`**
> **Explanation:** Frequent micro-batch streaming writes naturally produce many small files (one small commit per micro-batch). Two complementary fixes: **Optimized Writes** (`optimizeWrite.enabled`) reduces file count *at write time* by shuffling data to produce right-sized files as they're written, and a periodically scheduled `OPTIMIZE` job compacts whatever small files accumulate anyway. (Databricks also offers **Auto Compaction**, `autoOptimize.autoCompact`, as a related/complementary feature that runs a lightweight compaction automatically after writes.)
> **Knowledge Points:** Optimized Writes vs. Auto Compaction vs. manual `OPTIMIZE`; small-file problem in streaming ingestion; `spark.databricks.delta.autoCompact.enabled`; tradeoff between write latency and file-count management.

**A30. A) Use `dropDuplicates(["device_id"])` with a watermark on the event timestamp**
> **Explanation:** In streaming, `dropDuplicates` needs bounded state to avoid growing forever — pairing it with a watermark tells Spark it's safe to forget about old state (records older than the watermark threshold) once no more duplicates for them can arrive. `DISTINCT` inside `foreachBatch` only dedups within a single micro-batch, not across batches. `groupBy(...).agg(F.last(...))` on a stream doesn't guarantee "last" order deterministically and isn't the recommended dedup pattern. `MERGE INTO ... DELETE` is a batch/DML operation, not something you apply directly to a streaming DataFrame's dedup logic.
> **Knowledge Points:** `dropDuplicates` + `withWatermark` streaming dedup pattern; unbounded state growth without a watermark; watermark placement relative to dedup and aggregation operations.

---

### Section 3: Data Modeling

**A31. C) Bronze layer**
> **Explanation:** In the Medallion Architecture, **Bronze** stores raw data exactly as received from the source (often with minimal transformation, just schema-on-read metadata like ingestion timestamp). **Silver** cleans, deduplicates, and conforms this data. **Gold** aggregates and models data for direct business/analytics consumption (e.g., star schemas, dashboards).
> **Knowledge Points:** Bronze/Silver/Gold responsibilities; Bronze as an auditable "source of truth" replay layer; incremental refinement pattern across layers.

**A32. B) `effective_start_date`, `effective_end_date`, and `is_current`**
> **Explanation:** SCD Type 2 preserves full history by inserting a *new row* for every change rather than overwriting in place. The standard tracking columns are a start/end validity window (`effective_start_date`/`effective_end_date`, sometimes named `__START_AT`/`__END_AT` in DLT's `APPLY CHANGES`) and a current-record flag (`is_current`) for quickly filtering to "as of now" queries without date-range logic.
> **Knowledge Points:** SCD1 (overwrite, no history) vs. SCD2 (full history, multiple rows per key) vs. SCD3 (limited history, extra columns); DLT's native `__START_AT`/`__END_AT` columns when using `stored_as_scd_type="2"`.

**A33. A) `WHEN MATCHED AND source.hash != target.hash THEN UPDATE SET target.is_current = false, target.effective_end_date = current_date()`**
> **Explanation:** SCD2 via `MERGE INTO` is typically a two-part pattern: (1) this `WHEN MATCHED` clause closes out the *old* row (marks it no longer current, stamps an end date) when a hash/checksum comparison shows the incoming record differs from what's on file, and (2) a separate `WHEN NOT MATCHED THEN INSERT` (often fed by a pre-processed source that duplicates "changed" rows as new-key inserts) adds the *new* current version. Option A alone answers "how do you close the old record" — which is exactly what the question asks.
> **Knowledge Points:** Hash/checksum-based change detection (`hash(col1, col2, ...)` or `xxhash64`); the two-branch MERGE pattern for full SCD2 (close old + insert new); why a single plain `UPDATE` clause can't add a *new* row for the changed value.

**A34. B) It optimizes for analytical query performance by minimizing joins through denormalization**
> **Explanation:** A star schema (central fact table + surrounding dimension tables) intentionally denormalizes dimensions so that common analytical queries need far fewer joins than a fully normalized (3NF) design — trading some data redundancy for query speed and simplicity, which is exactly what BI/reporting workloads in the Gold layer want. It does *not* reduce table count versus 3NF (it typically has more, simpler tables), doesn't enforce referential integrity (Delta Lake has no native FK enforcement), and isn't about OLTP write throughput (Gold is a read-optimized analytical layer, not a transactional one).
> **Knowledge Points:** Star schema (fact + dimension tables) vs. snowflake schema vs. 3NF; denormalization tradeoffs; Gold layer as the "presentation" layer for BI tools; dimensional modeling concepts (surrogate keys, slowly changing dimensions within star schemas).

**A35. B) `OPTIMIZE` followed by `ZORDER BY`**
> **Explanation:** `OPTIMIZE` alone compacts many small files into fewer, larger ones (solving the small-file problem), and adding `ZORDER BY (columns)` on top of that same `OPTIMIZE` call additionally co-locates related data for better data skipping. `VACUUM`+`RESTORE` deal with cleanup/rollback, not compaction. `COPY INTO`+`COMPACT` and `REFRESH TABLE`+`ANALYZE TABLE` aren't real file-compaction mechanisms in Delta Lake.
> **Knowledge Points:** `OPTIMIZE table ZORDER BY (col1, col2)` as a single combined command; when to schedule `OPTIMIZE` for streaming tables; relationship between file compaction and data-skipping statistics.

**A36. B) Use `MERGE INTO silver_table USING new_data ON silver_table.event_id = new_data.event_id WHEN NOT MATCHED THEN INSERT *`**
> **Explanation:** An insert-only `MERGE` (matching on the natural key, inserting only unmatched rows) is Delta's recommended large-scale dedup pattern — it avoids a full-table shuffle/rewrite that `SELECT DISTINCT` or drop-and-recreate would require, and it works incrementally as new micro-batches arrive rather than needing to reprocess the whole table. `INSERT OVERWRITE` with `GROUP BY` would rewrite the entire table on every run, which doesn't scale.
> **Knowledge Points:** Insert-only merge for idempotent dedup; scaling dedup logic to avoid full-table scans; this is the same underlying mechanism used for idempotent streaming writes (A18/A25) applied at the batch/table level.

**A37. C) All committed operations on the table with version, timestamp, operation, and user details**
> **Explanation:** `DESCRIBE HISTORY` surfaces the Delta transaction log as a queryable table: each row is one committed operation (`WRITE`, `MERGE`, `DELETE`, `OPTIMIZE`, `VACUUM`, etc.) with its version number, timestamp, the user who ran it, and operation-specific metrics. It's the primary tool for auditing what happened to a table and for picking a version number to time-travel to.
> **Knowledge Points:** `DESCRIBE HISTORY` output columns (`version`, `timestamp`, `operation`, `operationParameters`, `userName`); using history to identify a rollback target version; relationship between `DESCRIBE HISTORY` (operation log) and `DESCRIBE DETAIL` (current table metadata/stats).

**A38. B) VACUUM removes files older than 7 days (default), potentially making versions before the retention window inaccessible**
> **Explanation:** Time travel and CDF both rely on old Parquet files still physically existing on storage. `VACUUM`'s job is to delete files that are no longer needed for the *current* version and are past the retention threshold — by default 7 days. If you `VACUUM` with the default settings, any table version whose files fall outside that 7-day window can become unreadable via time travel, even though the transaction log metadata for that version might still technically reference it. This is why Databricks recommends *not* shortening the retention period casually if time travel/CDF/audit needs are important.
> **Knowledge Points:** `VACUUM RETAIN <N> HOURS`; `spark.databricks.delta.retentionDurationCheck.enabled` (a safety guard against setting retention below 168 hours); the coupling between time travel, CDF, and physical file retention — deleting files breaks both.

**A39. B) The last record in the batch is applied** *(mock's intended answer — nuance noted below)*
> **Explanation:** `APPLY CHANGES` uses `SEQUENCE BY` to determine which change "wins" when the same key has multiple change events — normally the row with the highest sequence value wins. This question specifically asks about a **tie** (same key *and* same sequence value), which is an edge case; the safest characterization is that Databricks does not officially guarantee a specific deterministic outcome from a true tie — it depends on processing/arrival order. Practically, many study materials summarize this as "the last-processed record in the batch wins," which is close enough for exam purposes, but the real lesson is:
> ⚠️ **Best practice:** always choose a `sequence_by` column (or a composite `struct()` of columns) that's guaranteed unique per change event, precisely to avoid relying on undefined tie-breaking behavior.
> **Knowledge Points:** `sequence_by` as the CDC ordering mechanism; composite sequencing with `struct(col1, col2)` to break ties deterministically; why relying on arrival/batch order for correctness is an anti-pattern in distributed streaming systems.

**A40. B) Partitioning by `event_date` creates separate directories per date, enabling partition pruning for date-range queries**
> **Explanation:** Partitioning physically segregates data into subdirectories by the partition column's value, letting the query engine skip entire directories (partition pruning) for queries with a date filter — a natural fit for time-series data queried by date range. High-cardinality columns like `user_id` (option A) make poor partition columns because they create an excessive number of tiny partitions/directories ("over-partitioning"), which actually *hurts* performance — this is precisely the scenario where Liquid Clustering is now recommended instead.
> **Knowledge Points:** Partition pruning; low-cardinality vs. high-cardinality partition column selection; over-partitioning small-file problems; when Databricks now recommends Liquid Clustering over traditional partitioning for high-cardinality columns.

**A41. B) A probabilistic data structure that quickly determines if a value MIGHT be in a file, useful for equality lookups on high-cardinality columns**
> **Explanation:** A Bloom filter index is a compact, per-file probabilistic structure: for a given value, it can say "definitely not in this file" (allowing safe skipping) or "might be in this file" (never a false negative, but can have false positives). This makes it well suited to accelerate equality (`WHERE col = value`) lookups on high-cardinality columns, complementing Z-order/Liquid Clustering rather than replacing them. It's not a B-tree (not for ranges), not a hash join index, and not full-text search.
> **Knowledge Points:** `CREATE BLOOMFILTER INDEX`; false-positive-only probabilistic guarantee (never false negatives); best use case: point lookups (`=`) on high-cardinality columns not already covered by Z-order/clustering.

**A42. B) Shallow clone creates an independent copy with its own transaction log pointing to the original data files**
> **Explanation:** `SHALLOW CLONE` copies only the Delta *metadata* (transaction log) to the new location — it does **not** duplicate the underlying Parquet data files, instead pointing the new table's log at the original files. This makes shallow clones fast and cheap to create, ideal for quick testing or point-in-time metadata snapshots, but it means the original files must not be deleted (e.g., via `VACUUM` on the source) or the clone breaks. `DEEP CLONE`, by contrast, physically copies all data files, producing a fully independent table.
> **Knowledge Points:** `SHALLOW CLONE` vs. `DEEP CLONE`; use cases — shallow for quick/cheap testing snapshots, deep for full independent backups/migrations; the dependency risk shallow clones have on the source table's files remaining intact.

---

### Section 4: Security and Governance

**A43. B) Dynamic view using `current_user()` or `is_account_group_member()`**
> **Explanation:** Row-level security in Unity Catalog is commonly implemented via a dynamic view — a `CREATE VIEW` whose `WHERE` clause references functions like `current_user()` or `is_account_group_member('group')` so the same view returns different rows depending on who's querying it. (Unity Catalog also now offers native row filters — `ALTER TABLE ... SET ROW FILTER` — as a more centrally managed alternative to hand-rolled dynamic views, but the classic/foundational technique tested here is the dynamic view pattern.)
> **Knowledge Points:** Dynamic views with `is_account_group_member()`; native UC row filters as the newer equivalent; row-level security vs. column masking (rows vs. values).

**A44. C) Column masking policies using `ALTER TABLE ... ALTER COLUMN ... SET MASK`**
> **Explanation:** As covered earlier, column masking is a two-step process — define a SQL function with `CASE WHEN is_account_group_member(...) THEN real_value ELSE '****' END`, then attach it with `ALTER TABLE t ALTER COLUMN email SET MASK mask_func`. Row filters (option B) would hide entire *rows*, not redact a *column's value* while keeping the row visible — the wrong tool for "still show the row, just mask this field."
> **Knowledge Points:** Column masking function pattern; distinguishing "mask a column's value" (column mask) from "hide certain rows" (row filter); masking applies transparently to all queries against the table.

**A45. A) `USAGE` on the catalog and schema, plus `SELECT` on the table**
> **Explanation:** Unity Catalog's permission model is hierarchical: to read a table, a principal needs `USAGE` privilege on both the containing catalog and schema (this just grants the ability to "see into"/traverse that namespace — it doesn't grant data access by itself) **plus** `SELECT` on the specific table itself. Missing `USAGE` anywhere in the chain blocks access even if `SELECT` is granted on the table.
> **Knowledge Points:** UC privilege hierarchy (catalog → schema → table); `USAGE` as a traversal/namespace privilege vs. `SELECT` as a data-access privilege; permission inheritance — granting privileges at a catalog/schema level can cascade to contained objects depending on configuration.

**A46. C) Unity Catalog system tables (e.g., `system.access.audit`)**
> **Explanation:** Unity Catalog exposes audit logs as queryable system tables (under the `system` catalog), letting you run SQL directly against access history — including who queried what table and when — instead of manually parsing raw audit log files. `DESCRIBE HISTORY` shows *table-modifying* operations (writes/merges/deletes), not read/query access by arbitrary users. Spark event logs and job run logs don't capture cross-cutting, catalog-wide access auditing.
> **Knowledge Points:** `system.access.audit` and other system schemas (`system.billing`, `system.compute`, `system.lakeflow`); system tables as the modern, SQL-queryable replacement for manually exporting/parsing raw audit logs; enabling system tables at the metastore level.

**A47. B) Providing cloud storage access credentials (e.g., service principal, IAM role) for external locations**
> **Explanation:** A Unity Catalog `CREDENTIAL` (often called a *storage credential*) encapsulates the cloud identity (an AWS IAM role, Azure managed identity/service principal, or GCP service account) that Databricks assumes to read/write a given cloud storage location. It's then referenced by an `EXTERNAL LOCATION` object, which maps a specific storage path to that credential. This is distinct from user passwords, personal API tokens, or table-level encryption settings.
> **Knowledge Points:** `CREATE STORAGE CREDENTIAL`; relationship between `CREDENTIAL` and `EXTERNAL LOCATION` objects; why UC separates "who can access the cloud storage" (credential) from "which path is that credential allowed to touch" (external location) for least-privilege governance.

**A48. C) The cloud storage bucket/account encryption settings (e.g., AWS KMS or Azure Key Vault)**
> **Explanation:** Encryption-at-rest for data landing in cloud object storage (S3, ADLS, GCS) is configured at the storage layer itself — e.g., an S3 bucket policy specifying SSE-KMS with a customer-managed key, or an Azure Storage account's encryption scope tied to Key Vault. Databricks/Delta Lake writes through to whatever encryption the underlying storage enforces; there's no Delta table property or Spark cluster config that manages customer-managed-key encryption directly.
> **Knowledge Points:** Encryption at rest is a cloud-storage-layer concern, not a Databricks/Delta-layer one; customer-managed keys (CMK) via AWS KMS / Azure Key Vault / GCP KMS; contrast with Unity Catalog's *own* internal metadata, which has separate encryption handled by Databricks.

---

### Section 5: Monitoring and Logging

**A49. B) Spark UI → Stages tab → Task metrics for Stage 3**
> **Explanation:** The Stages tab breaks a stage down into its individual tasks with per-task metrics (duration, shuffle read/write, GC time, records processed) — exactly what's needed to spot the one outlier task and diagnose *why* it's slow (usually data skew, per A27). Cluster event logs track cluster-level lifecycle events (scaling, termination), not per-task performance; DBFS browser and SQL query history don't provide task-level execution detail.
> **Knowledge Points:** Spark UI navigation — Jobs → Stages → Tasks drill-down; task-level metrics (shuffle read size, GC time, task duration distribution); using this view to confirm and diagnose data skew.

**A50. C) Jobs tab → Stage DAG visualization**
> **Explanation:** The DAG visualization (viewable per-job, showing the chain of stages and where shuffle boundaries/wide transformations occur) is the tool for visually spotting unnecessary shuffles in the execution plan. The Executors tab shows resource usage per executor, the Storage tab shows cached/persisted RDDs/DataFrames, and the Environment tab shows Spark config values — none of these show the transformation DAG itself.
> **Knowledge Points:** Reading a Spark DAG to identify stage boundaries (each new stage = a shuffle boundary); distinguishing narrow transformations (no shuffle, e.g., `filter`, `map`) from wide transformations (shuffle required, e.g., `groupBy`, `join`, `repartition`).

**A51. B) Configure email/webhook notifications on the DLT pipeline or the Lakeflow Job that runs it**
> **Explanation:** Both DLT pipelines and Lakeflow Jobs have built-in notification configuration (email, or webhooks/Slack/PagerDuty via system destinations) that fire automatically on failure, without any custom polling code. Rolling your own polling script or health-check notebook (options A/C) reinvents something Databricks already provides natively, and adds unnecessary operational overhead and latency.
> **Knowledge Points:** Native job/pipeline notification settings (on start, on success, on failure, on duration threshold); webhook/system destinations for integrating with external alerting tools (PagerDuty, Slack); avoiding custom polling when native event-driven notifications exist.

**A52. B) Row counts, constraint violation counts, and records dropped per expectation**
> **Explanation:** The DLT/Lakeflow Declarative Pipelines event log specifically captures data-quality-relevant metrics tied to each defined expectation (`CONSTRAINT`) — how many rows passed, how many violated, how many were dropped vs. kept. Options A and C describe generic Spark/cluster resource metrics (available via the Spark UI/Ganglia instead), and option D is unrelated to data quality.
> **Knowledge Points:** DLT event log `flow_progress` events with `data_quality` metrics; per-expectation pass/fail/drop counts; querying the event log directly with SQL to build custom data-quality dashboards.

**A53. B) Streaming query progress logs accessible via `query.recentProgress`**
> **Explanation:** Every active `StreamingQuery` object exposes `query.recentProgress` (and `query.lastProgress`), returning a JSON-like structure per micro-batch with metrics like `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, and batch duration — exactly the "records processed per micro-batch over time" the question asks for. This is a first-class Structured Streaming API, not something you have to derive from generic UI tabs or Delta history.
> **Knowledge Points:** `StreamingQuery.recentProgress` / `.lastProgress` / `.status`; `StreamingQueryListener` for programmatically hooking into progress events (e.g., pushing metrics to a monitoring system); the Spark UI's "Structured Streaming" tab as the visual equivalent of this same data.

**A54. B) Executor memory is insufficient, causing frequent GC; increase executor memory or use memory-optimized instances**
> **Explanation:** High garbage collection time is a classic symptom of executors under memory pressure — the JVM spends excessive time trying to reclaim heap space rather than doing productive work, which directly explains a job running slower than its SLA. The fix is to give executors more memory headroom (bigger instance types, or memory-optimized instance families) rather than tweaking shuffle partition count or driver memory, which address different bottlenecks (task granularity and driver-side collection, respectively — neither of which is indicated by *executor* GC pressure).
> **Knowledge Points:** GC time as a memory-pressure indicator in the Spark UI/Ganglia; executor memory tuning (`spark.executor.memory`, memory-optimized instance types); distinguishing symptoms — GC pressure (memory) vs. long tail tasks (skew) vs. driver bottleneck (too much data pulled to driver via `collect()`).

---

### Section 6: Testing and Deployment

**A55. B) pytest with `pyspark.testing` utilities (e.g., `assertDataFrameEqual`)**
> **Explanation:** `pytest` is the de facto standard Python testing framework for PySpark projects, and Spark's own `pyspark.testing` module provides purpose-built assertion helpers (`assertDataFrameEqual`, `assertSchemaEqual`) designed to compare DataFrames sensibly (handling row order, floating-point tolerance, schema differences) — far better than hand-rolled comparison logic. Selenium and Robot Framework are UI/browser automation tools, unrelated to data transformation testing; bare `unittest` with hardcoded Spark sessions works but lacks the fixtures/tooling ecosystem `pytest` provides and isn't the recommended approach in Databricks docs.
> **Knowledge Points:** `pyspark.testing.utils.assertDataFrameEqual` / `assertSchemaEqual`; `DataFrame.transform` for composable, testable transformation chains; structuring PySpark unit tests to run locally (outside a cluster) for fast CI feedback.

**A56. B) YAML (`.yaml` / `databricks.yml`)**
> **Explanation:** Databricks Asset Bundles are defined declaratively in YAML, conventionally in a root `databricks.yml` file (which can `include` additional YAML files for resources, targets, variables, etc.). This is distinct from Terraform's HCL format (Databricks also has a Terraform provider, but that's a separate IaC path from DABs) and from JSON/TOML, which aren't used for bundle configuration.
> **Knowledge Points:** `databricks.yml` structure (`bundle`, `resources`, `targets`, `variables` top-level keys); `databricks bundle validate` / `deploy` / `run` CLI workflow; DABs vs. Terraform provider as two different (non-mutually-exclusive) IaC approaches for Databricks.

**A57. B) After unit tests pass and before merging to the main branch / deploying to production**
> **Explanation:** The standard CI/CD ordering is: lint → unit test (fast, no cluster needed) → integration test against a real staging workspace (validates actual Databricks behavior, e.g., cluster startup, real data access) → merge/deploy to production only if all of the above pass. Running integration tests only *after* production deployment (option C) defeats the purpose of having a gate at all — you'd be validating in production instead of catching issues before they reach it.
> **Knowledge Points:** CI/CD staging progression (lint → unit → integration → deploy); using a dedicated staging Databricks workspace/target for integration tests; GitHub Actions workflow structure with Databricks Asset Bundles (`bundle deploy -t staging` as a pipeline step before `-t prod`).

**A58. B) The deployment fails with a configuration validation error**
> **Explanation:** Databricks Asset Bundles validate resource references at deploy time — if a job configuration points to a cluster policy name/ID that doesn't exist in the target workspace, `databricks bundle deploy` fails fast with a clear validation error rather than silently deploying without the policy or auto-creating one. This fail-fast behavior is intentional: it prevents jobs from silently running with the wrong (unrestricted) compute configuration in a governed environment.
> **Knowledge Points:** Bundle deploy-time validation of resource references (cluster policies, instance pools, service principals); why bundles fail closed rather than silently degrading configuration; the importance of keeping environment-specific IDs (like cluster policy IDs) parameterized via bundle variables across dev/staging/prod targets.

**A59. B) Redeploy the previous bundle version from Git tag using `databricks bundle deploy`**
> **Explanation:** Because DABs make the entire job/pipeline configuration declarative and version-controlled, rolling back is as simple as checking out (or deploying from) a previous Git tag/commit and re-running `databricks bundle deploy`. This is fast, auditable, and doesn't require manual UI edits (error-prone, not tracked in Git) or destructive delete-and-recreate operations (causes downtime and loses job run history). Restoring a Delta *table* (option D) addresses data rollback, not job/infrastructure configuration rollback — a different concern entirely.
> **Knowledge Points:** Git-tag-based release/rollback strategy with DABs; the core IaC principle — infrastructure state should always be reproducible from version control, not manually patched; distinguishing "rolling back code/config" from "rolling back data" (Delta `RESTORE`/time travel).

**A60. B) Use DLT expectations (`CONSTRAINT`) in the pipeline definition as automated data quality checks, and run pipeline in development mode against sample data before promoting**
> **Explanation:** This combines two testing layers: (1) DLT expectations act as always-on, automated data quality gates that run every time the pipeline executes (not just during manual testing), catching schema/row-count/business-rule violations continuously; and (2) running the pipeline in **development mode** against a smaller sample dataset before promoting to production lets you validate schema and row-count expectations cheaply and quickly, without waiting on a full production-scale run. Manual log-checking, comparing raw file sizes, or ad hoc dashboard spot-checks are all less reliable and don't scale as a repeatable practice.
> **Knowledge Points:** DLT expectations as continuous automated data quality tests (not just one-off validation); development mode (`pipelines_development: true` / dev target) for cheap iteration against sample data; the general principle of testing schema and row-count correctness *before* promoting a pipeline change to production.
