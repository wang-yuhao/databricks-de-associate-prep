# 📝 Mock Exam 1 — Databricks Data Engineer Associate

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

**Q1.** What is the primary architectural benefit of the Databricks Lakehouse Platform compared to a traditional two-tier data lake + data warehouse architecture?

A) It eliminates the need for SQL queries  
B) It provides ACID transactions and BI-quality performance directly on open-format storage  
C) It requires no compute resources  
D) It replaces cloud object storage with proprietary storage  

---

**Q2.** Which layer of the Databricks architecture handles query optimization, caching, and execution?

A) Control Plane  
B) Unity Catalog  
C) Data Plane (compute)  
D) DBFS  

---

**Q3.** In Databricks, what is the difference between an All-Purpose cluster and a Job cluster?

A) Job clusters support SQL only; All-Purpose clusters support Python only  
B) All-Purpose clusters are persistent and interactive; Job clusters are created/terminated per job run  
C) Job clusters are cheaper because they use spot instances exclusively  
D) There is no functional difference — only the pricing model differs  

---

**Q4.** What does DBFS (Databricks File System) represent?

A) A proprietary replacement for cloud object storage  
B) A FUSE-mounted abstraction layer that presents cloud storage as a local filesystem  
C) An in-memory distributed cache  
D) A metadata catalog for file management  

---

**Q5.** Which Databricks feature allows you to run version-controlled notebooks and collaborate using Git workflows?

A) Databricks Repos (Git Folders)  
B) Databricks Asset Bundles  
C) MLflow Tracking  
D) Databricks Connect  

---

**Q6.** A data engineer wants to ensure that ML models, notebooks, and workflows are treated as code with version history. Which Databricks feature best supports this?

A) Delta Live Tables  
B) Databricks Repos (Git integration)  
C) Unity Catalog  
D) Auto Loader  

---

**Q7.** Which statement about Databricks Runtime is TRUE?

A) It is an open-source distribution of Apache Spark only  
B) It includes optimized versions of Apache Spark, Delta Lake, and pre-installed libraries  
C) A new runtime must be installed manually for each cluster  
D) Databricks Runtime is identical across all cluster types  

---

**Q8.** In a Unity Catalog hierarchy, what is the correct order from broadest to most specific?

A) Catalog → Metastore → Schema → Table  
B) Metastore → Catalog → Schema → Table  
C) Schema → Catalog → Metastore → Table  
D) Metastore → Schema → Catalog → Table  

---

**Q9.** What is a "Volume" in Unity Catalog?

A) A Delta table containing binary data  
B) A Unity Catalog object that governs access to non-tabular files in cloud storage  
C) A compute resource allocation unit  
D) A partition of a Delta table  

---

## PART 2 — Development & Ingestion (Questions 10–20)

**Q10.** Which PySpark DataFrame method is equivalent to a SQL `WHERE` clause?

A) `select()`  
B) `filter()` / `where()`  
C) `groupBy()`  
D) `agg()`  

---

**Q11.** A data engineer wants to read a CSV file with a header row and automatically infer the schema. Which is the correct PySpark code?

A) `spark.read.csv("/path/file.csv")`  
B) `spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/path/file.csv")`  
C) `spark.read.format("csv").option("schema", "auto").load("/path/file.csv")`  
D) `spark.read.csv("/path/file.csv", schema="infer")`  

---

**Q12.** What is the key advantage of specifying an explicit schema when reading data rather than using `inferSchema`?

A) Explicit schema is required for Delta format  
B) It avoids a full data scan, improving performance and preventing schema mismatches  
C) It enables Auto Loader  
D) Spark cannot infer schemas for nested data types  

---

**Q13.** Which function would you use to add a new column to a DataFrame based on an expression?

A) `df.add("col", expr)`  
B) `df.withColumn("col", expr)`  
C) `df.select("col", expr)`  
D) `df.transform("col", expr)`  

---

**Q14.** Auto Loader in Databricks is designed for which primary use case?

A) Bulk loading historical data from a data warehouse  
B) Incrementally ingesting new files from cloud storage as they arrive  
C) Replicating data between Delta tables using MERGE  
D) Streaming data from Apache Kafka  

---

**Q15.** What file format does Auto Loader use to track which files have already been processed?

A) A Delta table checkpoint  
B) A JSON manifest file  
C) RocksDB state store  
D) A Parquet metadata file  

---

**Q16.** Which SQL command would you use to read from one table and write to another in a single statement?

A) `INSERT INTO target SELECT * FROM source`  
B) `COPY INTO target FROM source`  
C) `CREATE TABLE target AS SELECT * FROM source`  
D) A and C are both valid  

---

**Q17.** A PySpark DataFrame contains a nested struct column `address` with a field `city`. How do you select only the `city` field?

A) `df.select("address.city")`  
B) `df.select(df.address["city"])`  
C) Both A and B are valid  
D) You must first flatten the struct with `explode()`  

---

**Q18.** What is the purpose of the `cache()` or `persist()` method on a DataFrame?

A) To write the DataFrame to disk permanently  
B) To store the DataFrame in memory/disk so repeated actions don't re-compute the lineage  
C) To register the DataFrame as a SQL temp view  
D) To pin the DataFrame to a specific cluster node  

---

**Q19.** Which of the following correctly creates a temporary view accessible only within the current Spark session?

A) `df.createGlobalTempView("my_view")`  
B) `df.createOrReplaceTempView("my_view")`  
C) `spark.sql("CREATE VIEW my_view AS SELECT * FROM df")`  
D) `df.registerTempTable("my_view")` (deprecated in Spark 3.x)  

---

**Q20.** When reading data with `COPY INTO`, what happens if the same files are run through the command again?

A) It re-ingests the same data, causing duplicates  
B) It skips already-processed files automatically (idempotent)  
C) It raises an error — files can only be read once  
D) It overwrites the target table each time  

---

## PART 3 — Delta Lake (Questions 21–28)

**Q21.** What enables Delta Lake to support ACID transactions on top of cloud object storage?

A) A distributed lock manager running in the Control Plane  
B) The `_delta_log/` transaction log (a series of JSON commit files)  
C) The Parquet footer metadata  
D) RocksDB key-value store  

---

**Q22.** A data engineer runs `DELETE FROM orders WHERE status = 'cancelled'`. A downstream analyst then queries `orders VERSION AS OF 2`. What data will they see?

A) The current table state after the DELETE  
B) The table state before the DELETE (with cancelled orders present)  
C) Only the deleted rows  
D) An error — time travel is not supported after DELETE  

---

**Q23.** What is the purpose of `OPTIMIZE` in Delta Lake?

A) To rewrite the transaction log  
B) To compact many small files into larger files for better read performance  
C) To enforce schema validation  
D) To vacuum old versions  

---

**Q24.** What is the difference between `ZORDER BY` and table partitioning?

A) They are equivalent — `ZORDER` is just the syntax for partitioning  
B) Partitioning creates physical directory boundaries; `ZORDER` co-locates related data within files using multi-dimensional clustering  
C) `ZORDER` only works on string columns  
D) Partitioning is deprecated in favor of `ZORDER`  

---

**Q25.** Which Delta Lake command merges new records while updating existing ones based on a match condition?

A) `UPSERT INTO target USING source ON condition`  
B) `MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`  
C) `UPDATE target FROM source WHERE condition`  
D) `INSERT OR REPLACE INTO target SELECT * FROM source`  

---

**Q26.** What does `VACUUM` do in Delta Lake, and what is the default retention period?

A) Removes duplicate rows; default 7 days  
B) Permanently deletes files no longer referenced by the transaction log; default retention 7 days (168 hours)  
C) Compacts small files; default retention 30 days  
D) Clears the Delta cache; no retention period  

---

**Q27.** A table was created with `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`. What is now possible?

A) The table can be shared via Delta Sharing  
B) You can read a log of row-level changes (inserts, updates, deletes) using `table_changes()`  
C) The table automatically syncs to a downstream system  
D) Auto Loader can read from this table  

---

**Q28.** What is the Medallion Architecture?

A) A security model with Bronze (public), Silver (internal), Gold (restricted) access levels  
B) A data organization pattern: Bronze (raw), Silver (cleaned/validated), Gold (aggregated/business-ready)  
C) A Databricks-specific term for partitioning strategies  
D) A Unity Catalog governance framework  

---

## PART 4 — Structured Streaming (Questions 29–33)

**Q29.** In Structured Streaming, what does "exactly-once" processing guarantee?

A) Each record is read exactly once from the source  
B) Each record is processed and written to the sink exactly once, even with failures/restarts  
C) The streaming query runs exactly once per hour  
D) The output file is written exactly once per micro-batch  

---

**Q30.** What is the role of a checkpoint in Structured Streaming?

A) To store the streaming query's execution plan  
B) To track progress (offsets) and state so the query can recover from failures  
C) To create Delta table snapshots  
D) To cache processed data in memory  

---

**Q31.** A streaming job uses `trigger(availableNow=True)`. What is the behavior?

A) Processes data continuously  
B) Processes all available data in one micro-batch, then stops — like a batch job  
C) Triggers once per day  
D) Waits for new data indefinitely without processing existing backlog  

---

**Q32.** What is the purpose of a watermark in Structured Streaming?

A) To limit the number of micro-batches per hour  
B) To define a threshold for how late arriving data is tolerated in aggregations, allowing state cleanup  
C) To mark checkpoints with a timestamp  
D) To set the trigger interval  

---

**Q33.** Which output mode writes only NEW rows added to a result table since the last trigger?

A) Complete  
B) Update  
C) Append  
D) Overwrite  

---

## PART 5 — Delta Live Tables & Workflows (Questions 34–38)

**Q34.** In Delta Live Tables (DLT), what is the difference between a `@dlt.table` and a `@dlt.view`?

A) `@dlt.view` results are stored as Delta tables; `@dlt.table` are temporary  
B) `@dlt.table` results are materialized as Delta tables; `@dlt.view` are not stored — computed each query  
C) They are identical except for naming conventions  
D) `@dlt.view` supports streaming; `@dlt.table` does not  

---

**Q35.** What are DLT Expectations used for?

A) Scheduling pipeline runs  
B) Declaring data quality rules; rows failing expectations can be dropped, quarantined, or cause the pipeline to fail  
C) Defining cluster configurations  
D) Encrypting sensitive columns  

---

**Q36.** A DLT pipeline is set to run in **Continuous** mode. What does this mean?

A) It runs once per day  
B) It processes all available data on demand, then stops  
C) It runs continuously, immediately processing new data as it arrives  
D) It uses triggered micro-batches every 5 minutes  

---

**Q37.** In Databricks Workflows (Lakeflow Jobs), what is a "task dependency"?

A) A Python package required by a task  
B) A defined execution order: Task B only runs after Task A completes successfully  
C) A permission grant allowing a task to access a table  
D) A timeout configuration  

---

**Q38.** What are Databricks Asset Bundles (DABs) used for?

A) Bundling Python libraries into a single wheel file  
B) Packaging Databricks resources (jobs, pipelines, notebooks) as code for CI/CD deployment  
C) Exporting notebooks as HTML  
D) Creating Delta table snapshots for deployment  

---

## PART 6 — Unity Catalog & Data Governance (Questions 39–45)

**Q39.** Which Unity Catalog privilege allows a user to read data from a table?

A) `USE CATALOG`  
B) `USE SCHEMA`  
C) `SELECT`  
D) `READ`  

---

**Q40.** A data engineer runs: `GRANT SELECT ON TABLE catalog.schema.orders TO 'analyst_group'`. What is `analyst_group` in this context?

A) A Unity Catalog catalog  
B) A group in the account console or identity provider (e.g., Entra ID group)  
C) A Databricks cluster policy  
D) A Delta Sharing recipient  

---

**Q41.** What is data lineage in Unity Catalog?

A) The chronological list of table versions  
B) Automatically tracked read/write relationships showing how data flows between tables and notebooks  
C) A graph of user permissions  
D) The audit log for GDPR compliance  

---

**Q42.** Delta Sharing is designed for which use case?

A) Sharing Delta tables between clusters within the same workspace  
B) Sharing data securely with external organizations without copying it  
C) Sharing notebooks via GitHub  
D) Sharing streaming data with Kafka consumers  

---

**Q43.** What is Lakehouse Federation in Databricks?

A) A multi-cloud replication service  
B) A feature that allows querying external databases (Snowflake, PostgreSQL, MySQL) through Unity Catalog without moving data  
C) A Databricks-managed ETL service  
D) A Delta Sharing extension for federated learning  

---

**Q44.** A data engineer needs to mask the `ssn` column so that only users with the `pii_admin` role see the real values; all others see `****`. Which Unity Catalog feature supports this?

A) Table ACLs with DENY permissions  
B) Column masks (dynamic data masking)  
C) Row filters  
D) Encryption at rest  

---

**Q45.** Which of the following is TRUE about the Unity Catalog metastore?

A) Each Databricks workspace has its own metastore  
B) A metastore is a per-region, account-level object that can serve multiple workspaces  
C) Unity Catalog requires a separate compute cluster to host the metastore  
D) Metastores can only contain Parquet-format tables  

---

## ✅ ANSWER KEY

> **Stop here — only read after completing all 45 questions!**

| Q | Answer | Key Concept |
|---|---|---|
| 1 | **B** | Lakehouse = ACID + open format + BI performance |
| 2 | **C** | Data Plane (clusters) handles execution |
| 3 | **B** | Job clusters are ephemeral; All-Purpose are interactive |
| 4 | **B** | DBFS is a FUSE-mount abstraction over cloud storage |
| 5 | **A** | Databricks Repos = Git integration |
| 6 | **B** | Git integration for version control of assets |
| 7 | **B** | Runtime = Spark + Delta Lake + libraries |
| 8 | **B** | Metastore → Catalog → Schema → Table |
| 9 | **B** | Volumes = Unity Catalog governance of files |
| 10 | **B** | `filter()` / `where()` = WHERE clause |
| 11 | **B** | Full options chain for CSV with header + inferSchema |
| 12 | **B** | Explicit schema = no scan, better performance |
| 13 | **B** | `withColumn()` adds/replaces a column |
| 14 | **B** | Auto Loader = incremental file ingestion |
| 15 | **A** | Auto Loader uses Delta checkpoint (or RocksDB for large) |
| 16 | **D** | Both INSERT INTO SELECT and CTAS are valid |
| 17 | **C** | Both dot notation and bracket notation work for structs |
| 18 | **B** | cache/persist stores intermediate results |
| 19 | **B** | `createOrReplaceTempView` = session-scoped temp view |
| 20 | **B** | COPY INTO is idempotent — skips processed files |
| 21 | **B** | `_delta_log/` JSON files = transaction log |
| 22 | **B** | Time travel shows state BEFORE the DELETE |
| 23 | **B** | OPTIMIZE compacts small files into larger ones |
| 24 | **B** | Partitioning = directories; ZORDER = within-file co-location |
| 25 | **B** | Full MERGE INTO syntax |
| 26 | **B** | VACUUM removes unreferenced files; default 7 days |
| 27 | **B** | CDF = row-level change log via `table_changes()` |
| 28 | **B** | Bronze/Silver/Gold = raw/clean/aggregate |
| 29 | **B** | Exactly-once = each record written to sink exactly once |
| 30 | **B** | Checkpoint = offset tracking + state for fault tolerance |
| 31 | **B** | availableNow = process backlog then stop |
| 32 | **B** | Watermark = late data threshold for state cleanup |
| 33 | **C** | Append mode = only new rows |
| 34 | **B** | `@dlt.table` = materialized; `@dlt.view` = not stored |
| 35 | **B** | Expectations = data quality rules with configurable actions |
| 36 | **C** | Continuous mode = always-on, process data immediately |
| 37 | **B** | Task dependency = execution order control |
| 38 | **B** | DABs = package Databricks resources for CI/CD |
| 39 | **C** | `SELECT` privilege for reading tables |
| 40 | **B** | `analyst_group` = account-level group (e.g., AAD group) |
| 41 | **B** | Lineage = tracked read/write relationships |
| 42 | **B** | Delta Sharing = external org data sharing without copy |
| 43 | **B** | Lakehouse Federation = query external DBs via UC |
| 44 | **B** | Column masks = dynamic data masking |
| 45 | **B** | Metastore = account-level, per-region, multi-workspace |

---

## 📊 Score Interpretation

| Score | % | Assessment |
|---|---|---|
| 40–45 | 89–100% | 🟢 Exam ready — register now! |
| 37–39 | 82–88% | 🟡 Strong — review wrong answers then go |
| 33–36 | 73–81% | 🟠 Close — focus on weak domains, take Mock Exam 2 |
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
