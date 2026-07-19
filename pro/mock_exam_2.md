# Databricks Certified Data Engineer Professional — Mock Exam 2

> **Exam Simulation | July 2026 Edition**
> - **Questions:** 59 scored questions (mirroring the real exam)
> - **Time Limit:** 120 minutes
> - **Passing Score:** 70% (≥42 correct)
> - **Format:** Multiple choice (single best answer)
> - **Answers:** See [Answer Key](#answer-key--explanations) at the bottom

**Domain Weights (July 2026 Official Exam Guide):**

| # | Domain | Weight | ~Questions |
|---|--------|--------|-----------|
| 1 | Developing Code for Data Processing (Python & SQL) | 22% | 13 |
| 2 | Data Ingestion & Acquisition | 7% | 4 |
| 3 | Data Transformation, Cleansing, and Quality | 10% | 6 |
| 4 | Data Sharing and Federation | 5% | 3 |
| 5 | Monitoring and Alerting | 10% | 6 |
| 6 | Cost & Performance Optimisation | 13% | 8 |
| 7 | Ensuring Data Security and Compliance | 10% | 6 |
| 8 | Data Governance | 7% | 4 |
| 9 | Debugging and Deploying | 10% | 6 |
| 10 | Data Modelling | 6% | 3 |

---

## Section 1 — Developing Code for Data Processing (Python & SQL) [22%]

---

**Q1.** A data engineer writes the following PySpark code:

```python
df = spark.read.format("delta").load("/mnt/silver/orders")
df2 = df.filter(df.status == "PENDING").select("order_id","customer_id","amount")
df2.write.format("delta").mode("overwrite").save("/mnt/gold/pending_orders")
```

After the job runs, a colleague notices that historical pending orders previously saved to `/mnt/gold/pending_orders` are gone. What is the **most likely cause**?

A. Delta Lake `overwrite` mode replaces all existing data in the target path.  
B. The filter `status == "PENDING"` silently dropped all rows.  
C. The Silver table was corrupted by a concurrent write.  
D. PySpark's lazy evaluation prevented the filter from executing.

---

**Q2.** Which of the following Python patterns is **most efficient** for iterating over a large Delta table and applying a complex Python UDF that cannot be expressed in native Spark?

A. `df.collect()` then `for row in rows:`  
B. `df.toPandas()` and use pandas `apply()`  
C. `df.mapInPandas(func, schema)` using a vectorized UDF  
D. `df.rdd.map(lambda r: func(r))`

---

**Q3.** A SQL query on a Delta table uses `MERGE INTO`. Which statement about MERGE semantics in Delta Lake is **correct**?

A. MERGE is not ACID-compliant; it uses eventual consistency.  
B. MERGE acquires a table-level lock, preventing all concurrent reads.  
C. MERGE is atomic — either all matched/unmatched rows are processed or none.  
D. MERGE can only perform UPDATEs; INSERTs require a separate statement.

---

**Q4.** A data engineer needs to read JSON files where the schema evolves over time (new fields added). They want Spark to automatically incorporate new fields. Which option achieves this?

A. `spark.read.option("mergeSchema", "true").json(path)`  
B. `spark.read.schema(fixed_schema).json(path)`  
C. `spark.read.option("inferSchema", "false").json(path)`  
D. `spark.read.option("rescuedDataColumn", "_rescued").json(path)`

---

**Q5.** Consider the following SQL on a Delta table:

```sql
SELECT customer_id, SUM(amount) AS total
FROM orders
WHERE order_date >= '2025-01-01'
GROUP BY customer_id
HAVING SUM(amount) > 10000
ORDER BY total DESC
LIMIT 100;
```

A developer wants this query to run faster. The table is partitioned by `order_date`. Which **additional** optimisation should they apply?

A. Add a Z-ORDER index on `customer_id` and `amount`.  
B. Enable Data Skipping by running `OPTIMIZE orders ZORDER BY (customer_id)`.  
C. Replace `GROUP BY` with `DISTRIBUTE BY`.  
D. Use `EXPLAIN EXTENDED` to disable predicate pushdown.

---

**Q6.** A streaming DataFrame uses `foreachBatch` to write to a Delta table. The engineer wants to perform a MERGE operation within `foreachBatch`. Which is the **correct approach**?

```python
def upsert(df, epoch_id):
    # Option A
    df.write.format("delta").mode("append").save(target_path)

    # Option B
    deltaTable = DeltaTable.forPath(spark, target_path)
    deltaTable.alias("t").merge(df.alias("s"), "t.id = s.id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
```

A. Option A only — foreachBatch cannot use DeltaTable API.  
B. Option B only — this is the correct MERGE pattern inside foreachBatch.  
C. Both A and B are valid; they produce identical results.  
D. Neither is valid; MERGE is not supported in Structured Streaming.

---

**Q7.** A Python function is registered as a UDF and used in a Spark SQL query. The engineer observes the query is extremely slow. Which is the **primary reason**?

A. Python UDFs are not supported in Spark SQL.  
B. Python UDFs disable Catalyst optimizer and run row-by-row with Python serialization overhead.  
C. Python UDFs always trigger a full table scan regardless of filters.  
D. Python UDFs cannot return nullable types.

---

**Q8.** A data engineer uses `spark.sql("CREATE OR REPLACE TABLE ...")`. How does this differ from `CREATE TABLE IF NOT EXISTS`?

A. `CREATE OR REPLACE` atomically replaces the table definition and all data; `IF NOT EXISTS` only creates if absent.  
B. `CREATE OR REPLACE` only changes metadata; data is preserved.  
C. `IF NOT EXISTS` drops and recreates the table if it exists.  
D. Both statements are equivalent in Delta Lake.

---

**Q9.** Which Spark action triggers the **immediate execution** of a lazy transformation chain?

A. `df.filter()`  
B. `df.select()`  
C. `df.count()`  
D. `df.withColumn()`

---

**Q10.** A data engineer needs to add a column `processed_at` with the current timestamp to every row written to a Delta table. Which is the most Spark-native approach?

A. Use a Python UDF `lambda: datetime.now()`  
B. Add `current_timestamp()` as a column in the DataFrame transformation: `df.withColumn("processed_at", current_timestamp())`  
C. Insert the timestamp in a foreachBatch sink using Python's `time.time()`  
D. Use a DEFAULT constraint `processed_at TIMESTAMP DEFAULT NOW()` in DDL only

---

**Q11.** A Delta Live Tables pipeline uses Python. An engineer defines:

```python
@dlt.table
def silver_customers():
    return dlt.read("bronze_customers").filter("is_valid = true")
```

What is `dlt.read()` vs `spark.read.table()`?

A. They are identical; DLT just aliases Spark's reader.  
B. `dlt.read()` creates a lineage dependency tracked by DLT; `spark.read.table()` does not register the dependency in the pipeline graph.  
C. `dlt.read()` only works with streaming sources; `spark.read.table()` is for batch.  
D. `spark.read.table()` is preferred in DLT because it supports schema evolution.

---

**Q12.** An engineer wants to use SQL to pivot order data: rows of `(customer_id, product, amount)` into columns per product. Which SQL construct is used?

A. `UNPIVOT`  
B. `LATERAL VIEW`  
C. `PIVOT`  
D. `CUBE`

---

**Q13.** A data engineer writes a Spark SQL window function:

```sql
SELECT order_id, customer_id, amount,
       RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk
FROM orders;
```

What does `RANK()` return when two rows have the same `amount`?

A. Both rows get consecutive ranks (1, 2) — no gaps.  
B. Both rows get the same rank, and the next rank is skipped (e.g., 1, 1, 3).  
C. Both rows get the same rank, and no ranks are skipped (e.g., 1, 1, 2).  
D. `RANK()` throws an error on duplicate values.

---

## Section 2 — Data Ingestion & Acquisition [7%]

---

**Q14.** A team wants to ingest files dropped into an Azure Data Lake Storage (ADLS) container incrementally, processing only new files on each run. Which Databricks feature is **designed** for this pattern?

A. `spark.read.format("csv").load(path)` with a filter on file modification date  
B. Auto Loader (`cloudFiles` format) with `readStream`  
C. `COPY INTO` with `FORCE = TRUE`  
D. Delta Lake `RESTORE TABLE`

---

**Q15.** Auto Loader can operate in two modes for file discovery. Which statement **correctly** contrasts them?

A. **Directory listing** scans all files on every trigger; **File notification** uses cloud storage events for incremental discovery.  
B. **File notification** requires manual file registration; **Directory listing** uses cloud events.  
C. Both modes are identical in performance; only the configuration differs.  
D. **Directory listing** only works with Parquet; **File notification** supports all formats.

---

**Q16.** A data engineer uses `COPY INTO` to load CSV files from an external location into a Delta table. They run the command three times against the same source path. What happens on the second and third runs?

A. All files are reloaded each time, causing duplicates.  
B. Only new files not previously loaded are ingested; already-processed files are skipped.  
C. The command fails with a "duplicate key" error.  
D. The Delta table is truncated before each load.

---

**Q17.** Which statement about Auto Loader schema inference and evolution is **correct**?

A. Auto Loader cannot infer schemas; you must provide a schema manually.  
B. Auto Loader infers schema on the first batch and raises an error if new columns appear later.  
C. With `cloudFiles.schemaEvolutionMode = "addNewColumns"`, Auto Loader automatically adds new columns to the target table when they are detected in incoming files.  
D. Schema evolution in Auto Loader requires restarting the stream from the beginning.

---

## Section 3 — Data Transformation, Cleansing, and Quality [10%]

---

**Q18.** A Delta Live Tables pipeline uses expectations. A developer writes:

```python
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_transactions():
    return dlt.read("bronze_transactions")
```

What happens to rows where `amount <= 0`?

A. The pipeline fails immediately.  
B. Violating rows are kept in the table but flagged in a quarantine column.  
C. Violating rows are silently dropped and metrics are recorded in the event log.  
D. The expectation is a warning only; all rows are written.

---

**Q19.** An engineer needs to deduplicate a streaming DataFrame using a watermark. Which code is **correct**?

```python
# Option A
deduped = df.withWatermark("event_time", "1 hour").dropDuplicates(["event_id"])

# Option B
deduped = df.dropDuplicates(["event_id"]).withWatermark("event_time", "1 hour")
```

A. Option A — watermark must be applied before `dropDuplicates`.  
B. Option B — `dropDuplicates` must precede the watermark.  
C. Both are equivalent; order doesn't matter.  
D. Neither works; deduplication in streaming requires a separate stateful operator.

---

**Q20.** What is the purpose of the `rescued data` column (`_rescued_data`) in Auto Loader and Databricks readers?

A. It stores the raw file bytes for backup.  
B. It captures columns present in the data but absent from the declared schema, preventing data loss.  
C. It records the checksum of each ingested file.  
D. It is used for schema inference only and is dropped before writing.

---

**Q21.** A data engineer wants to enforce NOT NULL and CHECK constraints on a Delta table. Which SQL is **correct**?

A. `ALTER TABLE orders ADD CONSTRAINT chk_amount CHECK (amount > 0);`  
B. `ALTER TABLE orders SET TBLPROPERTIES ('constraint.amount' = 'amount > 0');`  
C. Constraints can only be defined at table creation, not altered.  
D. Delta Lake does not support CHECK constraints; use UDF validation instead.

---

**Q22.** Which DLT feature automatically tracks data lineage, records expectation pass/fail metrics, and surfaces pipeline health?

A. Delta Lake transaction log  
B. Unity Catalog lineage API  
C. DLT Event Log (stored as a Delta table)  
D. Databricks SQL Query History

---

**Q23.** A data engineer wants to apply a slowly changing dimension (SCD Type 2) pattern using Delta Lake. Which operation achieves this most directly?

A. `INSERT INTO … SELECT` with deduplication  
B. `MERGE INTO` with `whenMatchedUpdate` (set `is_current=false`) and `whenNotMatchedInsert` for the new row  
C. `COPY INTO` with `OVERWRITE = TRUE`  
D. `REPLACE WHERE` clause on the partition

---

## Section 4 — Data Sharing and Federation [5%]

---

**Q24.** A company wants to share a Delta table with an external organisation using Databricks without copying the data. Which feature enables **secure, read-only, zero-copy sharing**?

A. Delta Lake time travel with a shared URL  
B. Delta Sharing protocol via a Unity Catalog Share  
C. Exporting data as CSV to an external S3 bucket  
D. Cloning the table to a shared workspace

---

**Q25.** A data engineer uses `CREATE FOREIGN CATALOG` in Unity Catalog. What does this enable?

A. Creating a local copy of an external database.  
B. Federating queries to an external data source (e.g., PostgreSQL, Snowflake) without moving data.  
C. Sharing Unity Catalog tables with non-Databricks consumers.  
D. Encrypting a catalog with an external key.

---

**Q26.** When a recipient accesses a Delta Sharing share, which of the following is **true**?

A. The recipient must have Databricks installed.  
B. The data provider's storage credentials are exposed to the recipient.  
C. The recipient accesses data via pre-signed URLs served by the sharing server; storage credentials are never shared.  
D. The recipient receives a full copy of the data at share creation time.

---

## Section 5 — Monitoring and Alerting [10%]

---

**Q27.** A Lakeflow Job (Databricks Workflow) fails at task 3 of 6. Which built-in feature allows the engineer to **retry only from the failed task** without rerunning successful tasks?

A. `Repair Run` — reruns only failed and skipped tasks.  
B. `Re-run All` — required to ensure idempotency.  
C. `Clone Run` — creates a new run from the failed task.  
D. `Task retry policy` — automatically retries on the next schedule.

---

**Q28.** A data engineer wants to receive a notification when a DLT pipeline's data quality expectation violation rate exceeds 5%. What is the **correct approach**?

A. Set `spark.conf.set("dlt.alert.threshold", "0.05")` in the pipeline settings.  
B. Create a Databricks SQL Alert on a query against the DLT event log table filtering for expectation violations.  
C. Use the DLT UI notification toggle for "5% violation threshold."  
D. Configure an Auto Loader notification rule with a 5% threshold.

---

**Q29.** Which table in the Databricks system schema can be queried to monitor cluster usage, costs, and compute attribution?

A. `system.access.audit`  
B. `system.billing.usage`  
C. `system.runtime.events`  
D. `system.compute.clusters`

---

**Q30.** A Lakeflow Job uses a `task dependency` graph. Task B depends on Task A, and Task C depends on Task B. Task A succeeds, Task B fails. What is the **default** state of Task C?

A. Task C runs because Task A succeeded.  
B. Task C is skipped (SKIPPED state) because its upstream dependency Task B failed.  
C. Task C runs with the last successful output of Task B.  
D. Task C fails immediately with a dependency error.

---

**Q31.** An engineer wants to track when rows were inserted or updated in a Delta table for auditing purposes. Which Delta Lake feature provides this with zero additional code?

A. Delta table properties `delta.enableChangeDataFeed = true`  
B. Structured Streaming watermarks  
C. Unity Catalog column-level lineage  
D. Databricks SQL Query History

---

**Q32.** A data engineer sets up a Databricks SQL Alert. What does the alert do when triggered?

A. It pauses the associated Lakeflow Job.  
B. It sends a notification (email, webhook, Slack) and can trigger a downstream action.  
C. It rolls back the last Delta transaction.  
D. It automatically scales up the SQL Warehouse.

---

## Section 6 — Cost & Performance Optimisation [13%]

---

**Q33.** What does `OPTIMIZE table ZORDER BY (col1, col2)` achieve?

A. Partitions the table by `col1` and `col2`.  
B. Compacts small files into larger ones and co-locates related data based on Z-ORDER multi-dimensional clustering, improving data skipping for queries filtering on those columns.  
C. Encrypts the columns `col1` and `col2`.  
D. Creates a sorted index that Spark uses for binary search.

---

**Q34.** A Delta table has thousands of small Parquet files after many micro-batch streaming writes. Which command addresses this "small file problem"?

A. `VACUUM table`  
B. `OPTIMIZE table`  
C. `RESTORE TABLE table TO VERSION AS OF 0`  
D. `ALTER TABLE table SET TBLPROPERTIES ('delta.targetFileSize' = '128mb')`

---

**Q35.** What is the difference between **Photon** and the standard Spark engine in Databricks?

A. Photon is a Python execution engine; standard Spark uses JVM only.  
B. Photon is a native vectorized execution engine written in C++ that significantly accelerates SQL and DataFrame operations; it is compatible with the standard Spark API.  
C. Photon replaces the Spark scheduler with a cost-based optimizer.  
D. Photon is only available for streaming workloads.

---

**Q36.** An engineer notices a job spends 90% of its time in a shuffle stage. What is the **most effective** way to reduce shuffle cost?

A. Increase the number of partitions with `spark.sql.shuffle.partitions`.  
B. Broadcast the smaller DataFrame using `broadcast(small_df)` in a join to avoid shuffle entirely.  
C. Use `repartition()` before the join.  
D. Switch from Python to Scala.

---

**Q37.** What is **Predictive Optimization** in Databricks?

A. A feature that predicts query results using ML.  
B. An automated service that runs OPTIMIZE and VACUUM on Delta tables based on usage patterns, without manual scheduling.  
C. A cost estimator for Databricks Unit (DBU) billing.  
D. A Photon feature that pre-compiles frequent SQL queries.

---

**Q38.** A company runs many ad-hoc SQL queries with variable load. Which compute type minimises cost by automatically scaling to zero when idle?

A. All-Purpose Cluster (Standard)  
B. Job Cluster  
C. Serverless SQL Warehouse  
D. High Concurrency Cluster

---

**Q39.** Liquid Clustering (introduced as a Delta Lake feature) is designed to replace which older optimisation strategy?

A. Z-ORDER by  
B. Hive-style static partitioning  
C. Bloom filter indices  
D. Data skipping statistics

---

**Q40.** An engineer observes that `VACUUM` is removing files needed for time travel queries. What setting controls the minimum retention period for time travel files?

A. `spark.databricks.delta.retentionDurationCheck.enabled`  
B. `delta.logRetentionDuration`  
C. `delta.deletedFileRetentionDuration`  
D. `spark.sql.files.maxRecordsPerFile`

---

## Section 7 — Ensuring Data Security and Compliance [10%]

---

**Q41.** A data engineer needs to grant a user `SELECT` access to a specific table in Unity Catalog without granting access to other tables in the same schema. What is the **minimum privilege** set?

A. `GRANT USE CATALOG, USE SCHEMA, SELECT ON TABLE target_table TO user`  
B. `GRANT ALL PRIVILEGES ON SCHEMA schema_name TO user`  
C. `GRANT SELECT ON CATALOG catalog_name TO user`  
D. `GRANT USE CATALOG, SELECT ON TABLE target_table TO user` (no USE SCHEMA needed)

---

**Q42.** Which Unity Catalog object is used to provide Databricks clusters or SQL Warehouses with credentials to access external cloud storage (e.g., ADLS, S3)?

A. External Location  
B. Storage Credential  
C. Connection  
D. Service Principal

---

**Q43.** A data engineer needs to mask the `ssn` column so that analysts can query the column but only see the last 4 digits (e.g., `***-**-1234`). Which Unity Catalog feature achieves this?

A. Column-level encryption using customer-managed keys  
B. Dynamic views with `CASE WHEN is_account_group_member(...) THEN ssn ELSE '***-**-' || right(ssn,4) END`  
C. Column Masks (Unity Catalog Column Masking) applied via `ALTER TABLE ... ALTER COLUMN ... SET MASK`  
D. Row-level filters on the SSN column

---

**Q44.** What is the purpose of a **Service Principal** in Databricks security?

A. It is a human user account with elevated privileges.  
B. It is a non-human identity used by applications, jobs, and automated pipelines to authenticate with Databricks and cloud services.  
C. It is a shared team account for collaborative notebooks.  
D. It is a Unity Catalog object for managing external data sharing.

---

**Q45.** Data in a Delta table needs to be deleted to comply with GDPR "right to be forgotten" requests. Which Delta Lake operation physically removes the data?

A. `DELETE FROM table WHERE user_id = 123` followed by `VACUUM table`  
B. `RESTORE TABLE table TO VERSION AS OF 0`  
C. `ALTER TABLE table DROP COLUMN user_id`  
D. `OPTIMIZE table ZORDER BY (user_id)`

---

**Q46.** A company wants to audit every data access event in their Databricks workspace. Which system table provides this information?

A. `system.billing.usage`  
B. `system.access.audit`  
C. `system.compute.node_timeline`  
D. `system.runtime.query_history`

---

## Section 8 — Data Governance [7%]

---

**Q47.** In Unity Catalog, what is the **three-level namespace** for addressing a table?

A. `workspace.schema.table`  
B. `catalog.database.table`  
C. `catalog.schema.table`  
D. `metastore.catalog.table`

---

**Q48.** A data engineer wants to track which notebook read a specific Delta table and which downstream tables were derived from it. Which Unity Catalog capability provides this?

A. Delta Lake transaction log  
B. Data Lineage (column-level and table-level lineage in Unity Catalog)  
C. Databricks SQL Query History  
D. DLT Event Log

---

**Q49.** Which statement about Unity Catalog **metastore** is correct?

A. Each Databricks workspace has its own metastore; they cannot be shared.  
B. A single Unity Catalog metastore can be attached to multiple workspaces in the same region, enabling cross-workspace governance.  
C. The metastore is a compute resource that runs SQL queries.  
D. Unity Catalog metastore is a replacement for HDFS.

---

**Q50.** A data engineer wants to tag a column `customer_email` as containing PII. Which Unity Catalog feature supports this?

A. Delta table properties (`TBLPROPERTIES`)  
B. System table annotations  
C. Unity Catalog Tags applied at the column level  
D. Databricks Secrets for metadata tagging

---

## Section 9 — Debugging and Deploying [10%]

---

**Q51.** A Databricks Asset Bundle (DAB) is used to deploy a Lakeflow Job. The engineer runs `databricks bundle deploy`. What does this command do?

A. Runs the job immediately in production.  
B. Deploys the bundle resources (job definition, notebooks, etc.) to the target workspace as defined in `databricks.yml`.  
C. Publishes the bundle to the Databricks Marketplace.  
D. Creates a new Databricks workspace from the bundle configuration.

---

**Q52.** An engineer wants to test a Databricks notebook locally before deploying. Which tool enables **local unit testing** of PySpark code without a live Databricks cluster?

A. `databricks-connect` pointed at a production cluster  
B. `pytest` with `pyspark` installed locally and a local SparkSession  
C. The Databricks Web Terminal  
D. Databricks Repos `git pull` command

---

**Q53.** A CI/CD pipeline should validate a Databricks Asset Bundle before deploying to production. Which command performs **dry-run validation** without deploying?

A. `databricks bundle run`  
B. `databricks bundle validate`  
C. `databricks bundle deploy --dry-run`  
D. `databricks bundle destroy`

---

**Q54.** A data engineer needs to pass environment-specific configurations (dev, staging, prod) to a Databricks Asset Bundle. Which mechanism is designed for this?

A. Hard-code values in the notebook using `if spark.conf.get("env") == "prod":`  
B. Use DAB **targets** (e.g., `targets.dev`, `targets.prod`) in `databricks.yml` with environment-specific overrides  
C. Maintain three separate `databricks.yml` files and manually switch between them  
D. Use Databricks Secrets to store all environment variables

---

**Q55.** When a DLT pipeline encounters an unhandled exception in a Python transformation function, what is the **default** pipeline behaviour?

A. The pipeline retries the failing table indefinitely.  
B. The pipeline fails the affected table, marks it as ERROR, and halts the pipeline run.  
C. The exception is logged but the pipeline continues with empty output for that table.  
D. The pipeline automatically rolls back to the previous version of the table.

---

**Q56.** An engineer uses `dbutils.secrets.get(scope="prod", key="db_password")` in a notebook. Which statement is **correct**?

A. The secret value is visible in the notebook output as plain text.  
B. Databricks automatically redacts secret values from notebook outputs and logs.  
C. `dbutils.secrets.get` only works in cluster-mode, not SQL Warehouses.  
D. Secrets are stored in the Delta table `system.secrets.vault`.

---

## Section 10 — Data Modelling [6%]

---

**Q57.** In the Medallion Architecture, what is the **primary purpose** of the Silver layer?

A. Raw, unprocessed ingestion of source data  
B. Cleaned, validated, and conformed data — deduplication, schema enforcement, and quality checks applied  
C. Aggregated, business-level reporting tables  
D. A staging area for ML feature engineering only

---

**Q58.** A data engineer designs a dimensional model. The `fact_sales` table has a `date_key` referencing `dim_date`. Which Delta Lake feature enforces referential integrity?

A. Delta Lake foreign key constraints (enforced at write time)  
B. Delta Lake supports declaring foreign keys as **informational** constraints (not enforced), used by query optimisers for planning  
C. Referential integrity must be enforced using a MERGE-based UDF  
D. Constraints are only supported in Unity Catalog managed tables

---

**Q59.** A data engineer is choosing between a **wide table** (all columns in one table) and a **normalised star schema** for a Gold-layer reporting use case. When is the wide table approach **preferable**?

A. When storage is limited and write frequency is high.  
B. When query performance and simplicity are priorities, and the data is read-heavy with infrequent updates (typical BI/dashboard use case on Databricks SQL).  
C. When strict normalisation is required for OLTP workloads.  
D. When the table will be shared externally via Delta Sharing.

---

---

# Answer Key & Explanations

> Review every answer you got wrong. For each incorrect answer, re-read the explanation and consult the referenced documentation.

---

**A1. A**
`mode("overwrite")` in Spark/Delta replaces all existing data at the target path with the new DataFrame. To preserve history, use `mode("append")` or a MERGE. Delta Lake does retain the old version in time travel history, but the "current" table state only reflects the latest overwrite.

**A2. C**
`mapInPandas` applies a vectorised Python function to chunks of the DataFrame as pandas DataFrames, avoiding full Python row serialisation. `collect()` and `toPandas()` bring all data to the driver — dangerous for large datasets. `rdd.map` uses row-by-row Python serialisation (slowest).

**A3. C**
Delta Lake MERGE is ACID-compliant. It is executed atomically — either the entire merge operation succeeds (all matched and unmatched rows processed according to clauses) or it fails and no changes are committed. Concurrent reads are not blocked.

**A4. A**
`mergeSchema = true` allows Spark to merge schemas across files, adding new columns automatically. `inferSchema = false` disables inference. `rescuedDataColumn` (option D) is an Auto Loader/Databricks-specific feature that captures extra fields outside the declared schema.

**A5. B**
Z-ORDER on `customer_id` (the GROUP BY key) co-locates rows for the same customer, dramatically improving data skipping when filters on `customer_id` are present. Since the table is already partitioned by `order_date`, the date filter is handled by partition pruning.

**A6. B**
`foreachBatch` receives a static batch DataFrame and an `epoch_id`. Inside it, you can use the full DeltaTable API including MERGE. This is the canonical pattern for stateful upserts in Structured Streaming. Option A (append-only) would create duplicates for re-processed records.

**A7. B**
Python UDFs are opaque to Catalyst — the optimizer cannot push predicates through them or perform code generation. Each row crosses the JVM-Python boundary via serialisation (pickle), resulting in severe performance degradation. Use Pandas UDFs (`@pandas_udf`) or built-in Spark functions instead.

**A8. A**
`CREATE OR REPLACE TABLE` atomically drops the existing table (all data and history from the perspective of the new statement) and recreates it. `CREATE TABLE IF NOT EXISTS` is a no-op if the table already exists.

**A9. C**
`count()` is an **action** that forces the DAG to execute. `filter()`, `select()`, and `withColumn()` are **transformations** — they are lazy and only define the execution plan.

**A10. B**
`current_timestamp()` is a built-in Spark SQL function that runs inside the cluster. It is deterministic per micro-batch and integrates with Catalyst. Python's `datetime.now()` in a UDF is row-level Python serialisation overhead and is not recommended.

**A11. B**
`dlt.read()` registers a dependency edge in the DLT pipeline DAG, enabling automatic refresh ordering and lineage tracking. `spark.read.table()` reads the table but DLT is unaware of the dependency — if the upstream table updates, the downstream table is not automatically refreshed.

**A12. C**
`PIVOT` rotates rows into columns. `UNPIVOT` is the inverse. `LATERAL VIEW` is used with table-generating functions. `CUBE` generates multi-dimensional aggregations.

**A13. B**
`RANK()` assigns the same rank to ties and then skips the next rank(s) (leaving gaps). `DENSE_RANK()` assigns the same rank without gaps. Example: amounts [100, 100, 50] → RANK: [1, 1, 3]; DENSE_RANK: [1, 1, 2].

**A14. B**
Auto Loader (`cloudFiles`) is the purpose-built incremental file ingestion feature in Databricks. It tracks which files have been processed using checkpoints, ensuring exactly-once (or at-least-once with idempotent targets) ingestion.

**A15. A**
**Directory listing** rescans the full directory on each trigger — suitable for low-volume scenarios. **File notification** subscribes to cloud storage events (Azure Event Grid, AWS SNS/SQS), making it far more scalable for high-volume file arrival.

**A16. B**
`COPY INTO` is idempotent by default — it tracks processed files and skips re-ingesting them. Use `COPY INTO ... FILES = (...)` to force specific files or `FORCE = TRUE` to re-process all files.

**A17. C**
With `cloudFiles.schemaEvolutionMode = "addNewColumns"`, Auto Loader detects new fields in incoming data and automatically evolves the target table's schema. Without this, a `UnknownFieldException` (or similar) would halt ingestion on schema change.

**A18. C**
`@dlt.expect_or_drop` drops rows that violate the expectation (rows where `amount <= 0` are removed) and records violation metrics in the DLT event log. Use `@dlt.expect` to keep rows + warn, `@dlt.expect_or_fail` to halt the pipeline.

**A19. A**
Watermark must be applied before `dropDuplicates` so that Spark knows the event-time column and can bound the stateful deduplication window. Option B would apply dropDuplicates on an unbounded stream (memory issue) before the watermark is registered.

**A20. B**
The `_rescued_data` column captures any columns present in the source data but absent from the target schema (or malformed data). This prevents silent data loss when source schemas diverge from declared schemas.

**A21. A**
`ALTER TABLE ... ADD CONSTRAINT ... CHECK (...)` is the correct SQL syntax for adding a CHECK constraint to a Delta table. Delta Lake enforces CHECK constraints on all subsequent writes. NOT NULL constraints use `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL`.

**A22. C**
The DLT Event Log is a Delta table automatically maintained by DLT pipelines. It records pipeline runs, expectation metrics (pass/fail counts), flow progress, and data quality events. It can be queried with SQL for custom monitoring.

**A23. B**
SCD Type 2 requires setting the existing current record to non-current (`is_current = false`, `end_date = today`) and inserting a new record. This is exactly what `MERGE INTO` with `whenMatchedUpdate` + `whenNotMatchedInsert` achieves.

**A24. B**
Delta Sharing (part of Unity Catalog) enables zero-copy, protocol-based sharing of Delta tables. The data stays in the provider's storage; recipients access it via pre-signed URLs without data movement.

**A25. B**
`CREATE FOREIGN CATALOG` creates a federated catalog that maps to an external data source. Queries are pushed down to the external system. Supported systems include PostgreSQL, MySQL, Snowflake, Redshift, and others via Lakehouse Federation.

**A26. C**
Delta Sharing uses a REST protocol where the sharing server generates short-lived pre-signed URLs for the underlying Parquet/Delta files. The recipient's credentials never touch the provider's storage account.

**A27. A**
`Repair Run` re-executes only the failed and downstream-dependent tasks, skipping already-successful ones. This is critical for cost efficiency in long pipelines.

**A28. B**
Databricks SQL Alerts run on a schedule against a SQL query. By querying the DLT event log for expectation violations and setting an alert threshold, engineers receive notifications when quality degrades.

**A29. B**
`system.billing.usage` contains DBU consumption records per workspace, cluster, and job. `system.access.audit` is for access auditing. The system schema is part of Unity Catalog's observability layer.

**A30. B**
In Lakeflow Jobs, if a task fails and downstream tasks depend on it, those tasks are automatically **skipped** (not failed). Only the directly-failed task is in FAILED state; its downstream tasks show SKIPPED.

**A31. A**
Change Data Feed (`delta.enableChangeDataFeed = true`) captures INSERT, UPDATE, and DELETE operations as change records. Consumers can read CDC with `spark.read.format("delta").option("readChangeFeed", "true")`.

**A32. B**
Databricks SQL Alerts evaluate a query on schedule and trigger notifications (email, Slack, PagerDuty webhook) when the result meets a threshold condition. They do not directly pause jobs or roll back transactions.

**A33. B**
ZORDER BY compacts files (similar to OPTIMIZE) AND applies Z-order space-filling curve multi-dimensional sorting within files. This co-locates related data and improves data skipping when queries filter on the Z-ordered columns.

**A34. B**
`OPTIMIZE` merges small files into files closer to the target file size (default 1 GB for Delta). `VACUUM` removes old/deleted files but does not compact. `RESTORE` reverts to a prior version.

**A35. B**
Photon is a Databricks-native vectorized C++ query engine. It processes data in batches of columns (vectorized), bypassing JVM overhead for many operations. It's transparent to the Spark API and achieves 2–10x speedups on SQL-heavy workloads.

**A36. B**
Broadcasting eliminates the shuffle by sending the smaller table to every executor. The large table is never moved. `broadcast(small_df)` in a join condition triggers a BroadcastHashJoin. Increasing partitions (A) makes the shuffle larger, not smaller.

**A37. B**
Predictive Optimization is a Databricks intelligent service that automatically schedules OPTIMIZE and VACUUM based on table usage statistics (write frequency, query patterns), so engineers don't need to manually configure maintenance jobs.

**A38. C**
Serverless SQL Warehouses provision compute on-demand per query and scale to zero when idle. All-Purpose Clusters stay running until manually terminated. Job Clusters shut down after job completion but take minutes to start.

**A39. B**
Liquid Clustering uses clustering keys that allow data layout to be updated incrementally without full rewrites. It is designed to replace static Hive-style `PARTITIONED BY` definitions, which require rewriting entire partitions when the clustering strategy changes.

**A40. C**
`delta.deletedFileRetentionDuration` (default: 7 days) controls how long deleted/overwritten data files are retained before VACUUM can remove them. Setting this to less than 7 days risks losing time travel history; Databricks blocks this by default unless `retentionDurationCheck.enabled = false`.

**A41. A**
In Unity Catalog, to SELECT from a table, a user needs: (1) `USE CATALOG` on the catalog, (2) `USE SCHEMA` on the schema, and (3) `SELECT` on the specific table. Granting privileges higher in the hierarchy (catalog/schema level) would grant access to more objects than intended.

**A42. B**
A **Storage Credential** is a Unity Catalog object that holds cloud provider credentials (IAM role for AWS, managed identity for Azure) used by Databricks to access cloud storage. **External Locations** use Storage Credentials to define accessible paths.

**A43. C**
Unity Catalog **Column Masks** allow a masking function to be applied transparently when the column is queried. The mask function can use `current_user()` or group membership to return different values for different principals — more scalable than managing individual views.

**A44. B**
Service Principals are non-human identities (OAuth/JWT-based) for automation. Jobs, DABs, CI/CD pipelines, and API clients authenticate as Service Principals to avoid tying production workloads to individual user credentials.

**A45. A**
`DELETE` marks rows as deleted in Delta's transaction log. `VACUUM` then physically removes the data files that are no longer referenced by the current table version (and are outside the retention window). Both steps are required for physical deletion (GDPR compliance).

**A46. B**
`system.access.audit` logs all workspace access events including table reads, writes, cluster starts, user logins, and permission changes — essential for compliance and security auditing.

**A47. C**
Unity Catalog uses `catalog.schema.table` (three-level namespace). The `catalog` is the top level (replaces the Hive metastore), `schema` is the database, and `table` is the object. Full reference: `my_catalog.my_schema.my_table`.

**A48. B**
Unity Catalog Data Lineage automatically captures table-level and column-level lineage across notebooks, jobs, SQL queries, and DLT pipelines. The lineage graph is queryable via the Unity Catalog UI or REST API.

**A49. B**
A Unity Catalog metastore is a regional governance control plane. Multiple Databricks workspaces in the same region can attach to the same metastore, sharing catalogs, schemas, governance policies, and lineage data.

**A50. C**
Unity Catalog **Tags** can be applied to catalogs, schemas, tables, and individual columns. Tagging `customer_email` as PII enables governance workflows (e.g., access policies, data discovery, compliance reports) based on tag values.

**A51. B**
`databricks bundle deploy` uploads local artifacts (notebooks, Python files) and creates/updates workspace resources (jobs, clusters, DLT pipelines) as defined in `databricks.yml`. It does NOT run the job; use `databricks bundle run` for execution.

**A52. B**
Local unit testing with `pytest` + a local SparkSession (installed via `pip install pyspark`) is the standard approach for testing transformation logic without a cluster. `databricks-connect` requires a live cluster and is for interactive development, not unit tests.

**A53. B**
`databricks bundle validate` checks the `databricks.yml` and all referenced files for syntax errors and schema violations without deploying anything. This is the pre-deployment gate in CI pipelines.

**A54. B**
DAB **targets** allow environment-specific overrides in `databricks.yml`:
```yaml
targets:
  dev:
    workspace:
      host: https://dev.azuredatabricks.net
  prod:
    workspace:
      host: https://prod.azuredatabricks.net
```
Deploy with `databricks bundle deploy --target prod`.

**A55. B**
DLT pipelines halt on unhandled exceptions in transformation functions. The affected table is marked ERROR and the pipeline run is terminated. This is by design to prevent silent data quality issues from propagating downstream.

**A56. B**
Databricks automatically redacts secret values from notebook cell outputs, replacing them with `[REDACTED]`. This prevents accidental secret exposure in shared notebooks or logs. Secrets are stored in Databricks Secrets (backed by Azure Key Vault or Databricks' own secrets store), not in a Delta table.

**A57. B**
In the Medallion Architecture: Bronze = raw data (as-is from source). Silver = cleaned, validated, deduplicated, conformed data. Gold = business-aggregated reporting/analytics layer. Silver is the "single source of truth" for downstream consumers.

**A58. B**
Delta Lake supports declaring primary key and foreign key constraints as **informational** (not enforced at write time). These declarations help query optimisers (like Photon and Databricks SQL) build better query plans but do not block writes that violate them. Enforcement is the application's responsibility.

**A59. B**
Wide (denormalised) tables avoid joins at query time, which is the main bottleneck for analytical workloads on large datasets. Databricks SQL with Photon is optimised for columnar reads of wide tables. The tradeoff is higher storage and more complex update logic, which is acceptable for read-heavy BI use cases.
