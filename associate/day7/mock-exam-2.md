# 📝 Mock Exam 2 — Databricks Data Engineer Associate

> **Format:** 45 questions · Simulate 90-minute exam · No reference materials  
> **Scoring:** 37/45 (≈82%) recommended to feel confident for the real exam  
> **Answers:** Scroll to bottom — do NOT peek until you finish all 45 questions!

---

## Instructions

1. Set a 90-minute timer before you begin
2. Answer all 45 questions on paper or in a separate document
3. Mark questions you're unsure about with `?`
4. After 90 minutes, check your answers
5. Review ALL wrong answers in the relevant day's study-notes.md

---

## PART 1 — Databricks Intelligence Platform (Questions 1–9)

**Q1.** Which component of the Databricks architecture runs in the **customer's cloud account** (not Databricks')?

A) The web application UI  
B) The cluster manager and REST API  
C) The Data Plane (clusters and storage)  
D) The account console  

---

**Q2.** A data engineer wants a cluster that automatically shuts down after 30 minutes of inactivity. Which setting controls this?

A) Spot instance timeout  
B) Auto-termination  
C) Cluster policy timeout  
D) Driver node heartbeat  

---

**Q3.** What is Photon in Databricks?

A) A machine learning framework for deep learning  
B) A C++-based vectorized query engine that accelerates Spark SQL and DataFrame operations  
C) A real-time streaming engine replacing Structured Streaming  
D) A proprietary replacement for Apache Parquet  

---

**Q4.** A team wants to enforce that all clusters use a specific Databricks Runtime version and have auto-termination enabled. Which feature should they use?

A) Cluster tags  
B) Instance pools  
C) Cluster policies  
D) Databricks Repos  

---

**Q5.** What is the purpose of an Instance Pool in Databricks?

A) To share compute between multiple workspaces  
B) To pre-allocate and keep a set of idle cloud VMs ready, reducing cluster start time  
C) To pool storage buckets across regions  
D) To manage Unity Catalog access grants  

---

**Q6.** In Databricks, what is the **Control Plane** responsible for?

A) Running Spark executors and storing data  
B) Hosting the Databricks web UI, REST APIs, cluster management, and job orchestration  
C) Processing all SQL queries  
D) Managing cloud object storage  

---

**Q7.** Which statement correctly describes **Databricks Repos (Git Folders)**?

A) Repos allow version-controlling notebooks and other files using Git, with support for branches, commits, and pull requests  
B) Repos replace Delta Live Tables for pipeline versioning  
C) Repos only work with GitHub; other Git providers are not supported  
D) Repos automatically deploy code changes to production clusters  

---

**Q8.** What is the **Databricks Lakehouse** architecture designed to unify?

A) SQL databases and NoSQL databases  
B) The reliability and governance of a data warehouse with the flexibility and scale of a data lake  
C) On-premises storage with cloud compute  
D) Batch and real-time ML inference pipelines  

---

**Q9.** When should you use a **Multi-node cluster** instead of a **Single-node cluster**?

A) For small datasets and local testing  
B) For distributed processing of large datasets that exceed a single machine's memory  
C) For all SQL queries; Single-node clusters only support Python  
D) Single-node clusters are always preferred for cost savings  

---

## PART 2 — Development & Ingestion (Questions 10–20)

**Q10.** What does the `explode()` function do in PySpark?

A) Raises an exception if a null value is found  
B) Converts an array or map column into multiple rows, one element per row  
C) Splits a string column into an array using a delimiter  
D) Expands all nested struct fields into top-level columns  

---

**Q11.** A data engineer writes:
```python
df2 = df.groupBy("category").agg(count("*").alias("cnt"), avg("price").alias("avg_price"))
```
What does this code produce?

A) A DataFrame with one row per unique `category`, plus `cnt` and `avg_price` columns  
B) A DataFrame with the original `df` rows filtered by category  
C) An error — `count("*")` is not valid PySpark syntax  
D) A DataFrame sorted by category with running totals  

---

**Q12.** What is the difference between `df.write.mode("overwrite")` and `df.write.mode("append")`?

A) `overwrite` deletes and replaces all data in the target; `append` adds new rows without deleting existing ones  
B) `overwrite` adds rows and updates matches; `append` adds rows only  
C) They are equivalent — both replace the target  
D) `append` is only supported for Delta format  

---

**Q13.** A data engineer needs to join two DataFrames on a common key. What is the correct PySpark syntax?

A) `df1.merge(df2, on="id", how="inner")`  
B) `df1.join(df2, df1.id == df2.id, "inner")`  
C) `df1.union(df2).where(df1.id == df2.id)`  
D) `spark.join(df1, df2, on="id")`  

---

**Q14.** What is the difference between `createOrReplaceTempView()` and `createGlobalTempView()`?

A) Temp views persist after the session ends; global temp views do not  
B) Temp views are session-scoped; global temp views survive across sessions within the same Spark application (referenced as `global_temp.view_name`)  
C) They are identical in scope  
D) Global temp views are stored as Delta tables  

---

**Q15.** Auto Loader's **schema inference** and **schema evolution** features are stored in which location?

A) The Delta table metadata  
B) The `_schemas` subdirectory of the checkpoint location  
C) The Databricks metastore  
D) A separate JSON config file in DBFS  

---

**Q16.** Which command copies data **idempotently** from cloud storage into a Delta table and tracks which files have been loaded?

A) `INSERT INTO table SELECT * FROM read_files(path)`  
B) `COPY INTO table FROM path FILEFORMAT = PARQUET`  
C) `df.write.mode("append").saveAsTable("table")`  
D) `spark.read.format("parquet").load(path).write.insertInto("table")`  

---

**Q17.** A DataFrame has a column `ts` of type `StringType` with values like `"2024-01-15 10:30:00"`. How do you cast it to `TimestampType`?

A) `df.withColumn("ts", df.ts.cast("timestamp"))`  
B) `df.withColumn("ts", to_timestamp(df.ts))`  
C) Both A and B are valid approaches  
D) `df.withColumn("ts", df.ts.astype(TimestampType()))`  

---

**Q18.** Which PySpark function is used to **rename** a column without changing its values?

A) `df.rename("old", "new")`  
B) `df.withColumnRenamed("old_name", "new_name")`  
C) `df.alias("old_name", "new_name")`  
D) `df.select(col("old_name").name("new_name"))`  

---

**Q19.** A data engineer wants to read only files matching the pattern `events_2024*.json` from a directory. Which option supports this in Auto Loader?

A) `option("pathGlobFilter", "events_2024*.json")`  
B) `option("filePattern", "events_2024*")`  
C) `option("schema", "events_2024")`  
D) `option("fileFilter", "*.json")`  

---

**Q20.** What is the result of running `spark.sql("SHOW TABLES IN my_catalog.my_schema")`?

A) Creates a new table  
B) Returns a list of tables in the specified catalog and schema  
C) Displays the schema/DDL for all tables  
D) Grants SELECT permission on all tables  

---

## PART 3 — Delta Lake (Questions 21–28)

**Q21.** Which SQL statement enables Change Data Feed on an existing Delta table?

A) `ALTER TABLE orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`  
B) `UPDATE TABLE orders ENABLE CDC`  
C) `SET delta.cdf = true ON TABLE orders`  
D) `CREATE TABLE orders USING DELTA WITH CDC`  

---

**Q22.** A data engineer queries `SELECT * FROM orders TIMESTAMP AS OF '2024-06-01'`. What happens if the version for that timestamp has been removed by `VACUUM`?

A) The query returns an empty result set  
B) The query raises an error because the files are gone  
C) Delta automatically fetches data from a backup  
D) The query returns the current table state  

---

**Q23.** What is **schema enforcement** in Delta Lake?

A) Delta automatically evolves the schema when new columns are detected  
B) Delta rejects writes that don't match the existing table schema (prevents schema corruption)  
C) Delta enforces NOT NULL constraints on all columns  
D) Delta compresses all columns to match a fixed schema  

---

**Q24.** To enable schema evolution in Delta Lake (allow adding new columns on write), which option must be set?

A) `.option("mergeSchema", "true")`  
B) `.option("schemaEvolution", "true")`  
C) `.option("overwriteSchema", "true")`  
D) No special option is needed — Delta evolves schemas automatically  

---

**Q25.** A Delta table has been updated 150 times. Running `DESCRIBE HISTORY orders` shows all 150 versions. After running `VACUUM orders RETAIN 168 HOURS`, what happens?

A) The transaction log entries are deleted  
B) The underlying data files that are no longer referenced by any version within the last 7 days are permanently deleted  
C) All versions older than 150 are deleted  
D) Nothing — you must explicitly specify which versions to delete  

---

**Q26.** What is the **Silver layer** in the Medallion Architecture responsible for?

A) Storing raw, unprocessed data as it arrives  
B) Curating, joining, and aggregating data for business intelligence and reporting  
C) Cleaning, validating, deduplicating, and conforming raw data into a reliable, queryable format  
D) Storing ML model artifacts  

---

**Q27.** How do you restore a Delta table to a specific version?

A) `ROLLBACK TABLE orders TO VERSION 5`  
B) `RESTORE TABLE orders TO VERSION AS OF 5`  
C) `REVERT TABLE orders VERSION AS OF 5`  
D) `ALTER TABLE orders RESTORE VERSION 5`  

---

**Q28.** A `MERGE INTO` statement has both `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT` clauses. What happens to rows in the **source** that already exist in the **target** but need no changes?

A) They raise an error — all matched rows must be updated  
B) They are updated with the same values (no-op write)  
C) They are skipped if no `WHEN MATCHED THEN UPDATE` clause applies (or a `WHEN MATCHED AND condition` is not met)  
D) They are automatically deleted  

---

## PART 4 — Structured Streaming (Questions 29–33)

**Q29.** In Structured Streaming, which **output mode** is required when using streaming aggregations with a watermark?

A) Append (always valid for aggregations)  
B) Complete (writes the entire updated result table)  
C) Update (writes only rows that changed since last trigger) — also valid  
D) Both B and C are valid for windowed aggregations with watermark  

---

**Q30.** A streaming query writes to a Delta table. What happens if the query fails mid-write and is restarted?

A) Data is duplicated because Spark re-processes the last batch  
B) The checkpoint and Delta's idempotent writes ensure exactly-once semantics — no duplicates  
C) The query resumes from the beginning of the stream  
D) Data from the failed batch is permanently lost  

---

**Q31.** Which trigger type processes data **as fast as possible** in micro-batches without any fixed interval?

A) `trigger(processingTime="0 seconds")`  
B) `trigger(continuous="1 second")`  
C) `trigger(once=True)`  
D) `trigger(availableNow=True)`  

---

**Q32.** A streaming job aggregates events by a 10-minute tumbling window with a 5-minute watermark. An event arrives 8 minutes late. What happens?

A) The event is included in the window — it's within the watermark threshold  
B) The event is dropped — 8 minutes exceeds the 5-minute watermark  
C) The event triggers a new window  
D) The event is buffered indefinitely  

---

**Q33.** What is the purpose of `foreachBatch()` in Structured Streaming?

A) To process each row individually in a custom Python function  
B) To apply arbitrary batch DataFrame operations (like MERGE) to each micro-batch output  
C) To iterate over all historical records in the stream  
D) To loop over multiple streaming sources  

---

## PART 5 — Delta Live Tables & Workflows (Questions 34–38)

**Q34.** In a DLT pipeline, what does setting `expect_or_drop("valid_price", "price > 0")` do?

A) Fails the entire pipeline if any row has `price <= 0`  
B) Quarantines invalid rows in a separate table  
C) Drops rows where `price <= 0` and records the count as a metric  
D) Logs a warning but keeps all rows  

---

**Q35.** What is the difference between DLT **Triggered** and **Continuous** pipeline modes?

A) Triggered pipelines run once and stop; Continuous pipelines run indefinitely  
B) Continuous pipelines support only batch sources; Triggered pipelines support streaming  
C) Triggered pipelines are deprecated in favor of Continuous mode  
D) They are identical except for the schedule configuration  

---

**Q36.** A Databricks Workflow has 3 tasks (A, B, C). Task B depends on Task A. Task C has no dependencies. In which order do they execute?

A) A → B → C (sequential)  
B) A and C run in parallel; B runs after A completes  
C) C runs first because it has no dependencies  
D) The order is random unless explicitly specified  

---

**Q37.** A Databricks Job is configured with a **retry policy of 3 attempts**. Task A fails on attempt 1 and 2. What happens?

A) The job is immediately marked as failed after the first failure  
B) Databricks automatically retries Task A up to 3 total attempts before marking the task as failed  
C) The job skips Task A and runs downstream tasks  
D) A manual confirmation is required before each retry  

---

**Q38.** Which Databricks feature allows you to define Jobs, DLT pipelines, and permissions as YAML/JSON files stored in a Git repository for CI/CD deployment?

A) Databricks Repos  
B) MLflow Projects  
C) Databricks Asset Bundles (DABs)  
D) Cluster Policies  

---

## PART 6 — Unity Catalog & Data Governance (Questions 39–45)

**Q39.** To allow a user to query tables in `my_catalog.my_schema.orders`, which set of minimum permissions is required?

A) `SELECT` on the table only  
B) `USE CATALOG` on `my_catalog`, `USE SCHEMA` on `my_schema`, and `SELECT` on the table  
C) `READ` on `my_catalog` only  
D) `ALL PRIVILEGES` on `my_catalog`  

---

**Q40.** Which Unity Catalog feature applies a filtering predicate on a table so that certain users only see rows matching a condition (e.g., only rows from their region)?

A) Column masks  
B) Row filters  
C) Table ACLs  
D) Dynamic views  

---

**Q41.** What happens to data lineage when a notebook reads from `table_A` and writes to `table_B` in Unity Catalog?

A) Lineage must be manually configured via a LINEAGE command  
B) Unity Catalog automatically captures the read/write relationship and displays it in the lineage graph  
C) Lineage is only tracked for Delta Live Tables pipelines  
D) Lineage is not supported for Python notebooks — only SQL  

---

**Q42.** A data provider wants to share a Delta table with an external company. The recipient uses their own Databricks workspace on a different cloud. Which feature enables this?

A) Unity Catalog cross-workspace replication  
B) Delta Sharing with an open protocol that supports any client  
C) External locations with shared SAS tokens  
D) Databricks Asset Bundles deployment  

---

**Q43.** What is an **External Location** in Unity Catalog?

A) A table stored outside the Unity Catalog metastore  
B) A Unity Catalog object that maps a cloud storage path (e.g., `s3://bucket/path`) to a credential, enabling governed access  
C) A Databricks workspace in another region  
D) A foreign table in Lakehouse Federation  

---

**Q44.** A data engineer creates a table with `CREATE TABLE catalog.schema.pii_table (name STRING, ssn STRING MASK pii_mask)`. What is `pii_mask` in this context?

A) A Unity Catalog role  
B) A user-defined function applied to the `ssn` column that controls what different users see  
C) A Delta table property  
D) A cluster-level encryption policy  

---

**Q45.** Which of the following is a valid use of `INFORMATION_SCHEMA` in Unity Catalog?

A) To modify table permissions  
B) To query metadata about tables, columns, schemas, and privileges within a catalog  
C) To run DLT pipeline definitions  
D) To create external locations  

---

## ✅ ANSWER KEY

> **Stop here — only read after completing all 45 questions!**

| Q | Answer | Key Concept |
|---|---|---|
| 1 | **C** | Data Plane runs in customer's cloud account |
| 2 | **B** | Auto-termination = idle shutdown setting |
| 3 | **B** | Photon = C++ vectorized engine for Spark SQL/DF ops |
| 4 | **C** | Cluster policies enforce runtime, settings at org level |
| 5 | **B** | Instance pools = pre-allocated idle VMs, faster start |
| 6 | **B** | Control Plane = UI, APIs, orchestration (Databricks-managed) |
| 7 | **A** | Repos = Git versioning with branch/commit/PR support |
| 8 | **B** | Lakehouse = warehouse reliability + lake flexibility |
| 9 | **B** | Multi-node = distributed processing for large datasets |
| 10 | **B** | `explode()` = array/map → multiple rows |
| 11 | **A** | `groupBy().agg()` = one row per group with aggregates |
| 12 | **A** | `overwrite` deletes then replaces; `append` adds rows |
| 13 | **B** | `df1.join(df2, condition, "inner")` is correct syntax |
| 14 | **B** | TempView = session-scoped; GlobalTempView = app-scoped |
| 15 | **B** | Auto Loader schema stored in `_schemas` under checkpoint |
| 16 | **B** | `COPY INTO` = idempotent, tracks loaded files |
| 17 | **C** | Both `.cast("timestamp")` and `to_timestamp()` work |
| 18 | **B** | `withColumnRenamed()` = rename without value change |
| 19 | **A** | `pathGlobFilter` option for glob pattern matching |
| 20 | **B** | `SHOW TABLES` returns a list of tables in the schema |
| 21 | **A** | `ALTER TABLE ... SET TBLPROPERTIES` enables CDF |
| 22 | **B** | After VACUUM, time travel to removed versions fails |
| 23 | **B** | Schema enforcement = rejects mismatched writes |
| 24 | **A** | `mergeSchema=true` enables schema evolution on write |
| 25 | **B** | VACUUM deletes unreferenced files outside retention window |
| 26 | **C** | Silver = cleaned, validated, deduplicated reliable data |
| 27 | **B** | `RESTORE TABLE ... TO VERSION AS OF N` |
| 28 | **C** | Rows skipped if no MATCHED clause condition is met |
| 29 | **D** | Both Complete and Update modes valid for windowed aggs |
| 30 | **B** | Checkpoint + Delta idempotency = exactly-once on restart |
| 31 | **A** | `processingTime="0 seconds"` = continuous micro-batches |
| 32 | **B** | 8 min late > 5 min watermark → event dropped |
| 33 | **B** | `foreachBatch()` = run batch ops (MERGE etc.) per micro-batch |
| 34 | **C** | `expect_or_drop` = drops invalid rows, records metric |
| 35 | **A** | Triggered = run-once; Continuous = always-on |
| 36 | **B** | A and C parallel; B waits for A (dependency graph) |
| 37 | **B** | Retry policy = N total attempts before failure |
| 38 | **C** | DABs = YAML/JSON resource definitions for CI/CD |
| 39 | **B** | Need USE CATALOG + USE SCHEMA + SELECT (3-level) |
| 40 | **B** | Row filters = predicate-based row visibility control |
| 41 | **B** | Unity Catalog auto-captures lineage from read/writes |
| 42 | **B** | Delta Sharing = open protocol for cross-org sharing |
| 43 | **B** | External Location = cloud path + credential mapping in UC |
| 44 | **B** | Column mask = UDF controlling column visibility by user |
| 45 | **B** | INFORMATION_SCHEMA = query catalog/table/privilege metadata |

---

## 📊 Score Interpretation

| Score | % | Assessment |
|---|---|---|
| 40–45 | 89–100% | 🟢 Exam ready — register now! |
| 37–39 | 82–88% | 🟡 Strong — review wrong answers then go |
| 33–36 | 73–81% | 🟠 Close — focus on weak domains, take a break and retake |
| <33 | <73% | 🔴 More study needed — revisit relevant day's notes |

---

## 🔍 Weak Area Drill

After scoring, group your wrong answers by domain and revisit:

- **Q1–9 wrong** → Re-read `day1/study-notes.md`
- **Q10–20 wrong** → Re-read `day2/study-notes.md`
- **Q21–28 wrong** → Re-read `day3/study-notes.md`
- **Q29–33 wrong** → Re-read `day4/study-notes.md`
- **Q34–38 wrong** → Re-read `day5/study-notes.md`
- **Q39–45 wrong** → Re-read `day6/study-notes.md`

---

## 📌 Final Tips for the Real Exam

1. **Read every option** — Databricks questions often have two plausible answers; the correct one is more specific
2. **SQL-first mindset** — many tasks have both SQL and PySpark solutions; know both
3. **Unity Catalog hierarchy** — Metastore → Catalog → Schema → Table/Volume/View (memorize this)
4. **Delta Lake transaction log** — `_delta_log/` is the answer to most "how does Delta support X" questions
5. **Trigger types** — `availableNow` (batch-like), `processingTime` (interval), `continuous` (low-latency), `once` (deprecated)
6. **DLT expectations** — `expect` (warn), `expect_or_drop` (drop rows), `expect_or_fail` (fail pipeline)
7. **VACUUM default** — 7 days (168 hours) retention; cannot go below without disabling the safety check
