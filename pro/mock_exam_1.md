# Databricks Certified Data Engineer Professional
# Mock Exam 1 — Code, Ingestion, Transformation & Data Sharing

> **Exam reference:** July 2026 Exam Guide (30 Nov 2025 version, confirmed current as of May 2026)  
> **Topics covered:** Section 1 (22%), Section 2 (7%), Section 3 (10%), Section 4 (5%)  
> **Questions:** 20 multiple-choice | **Time:** ~40 min

---

## Section 1 — Developing Code for Data Processing (Python & SQL) — 22%

### Key Knowledge Points
- **Python UDFs vs. Pandas UDFs (vectorized):** Pandas UDFs use Apache Arrow for serialization, reducing Python–JVM overhead. Use `@pandas_udf` decorator. Return types must be declared.
- **Higher-order functions:** `TRANSFORM`, `FILTER`, `AGGREGATE` operate on array columns without explode/collect.
- **Window functions:** `OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE BETWEEN ...)` — understand frame specification.
- **Delta Lake DML:** `MERGE INTO`, `UPDATE`, `DELETE` with full predicate push-down.
- **Structured Streaming API:** `readStream` / `writeStream`, trigger modes (`Once`, `AvailableNow`, `ProcessingTime`, `Continuous`), output modes (`append`, `complete`, `update`).
- **dbutils:** `dbutils.widgets`, `dbutils.secrets`, `dbutils.fs`, `dbutils.notebook.run()` / `exit()`.
- **Auto Loader (`cloudFiles`):** `cloudFiles.format`, `cloudFiles.schemaLocation`, `cloudFiles.inferColumnTypes`.

---

### Q1
A data engineer needs to apply a custom Python function to a column in a large Spark DataFrame. The function is computationally expensive and involves NumPy operations. What is the MOST efficient approach?

**A.** Register a standard Python UDF with `spark.udf.register()` and apply it with `withColumn()`  
**B.** Use `DataFrame.apply()` to call the Python function row-by-row  
**C.** Use a Pandas UDF (`@pandas_udf`) with `PandasUDFType.SCALAR` to leverage Apache Arrow serialization  
**D.** Convert the DataFrame to a Pandas DataFrame, apply the function, and convert back  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: C**

**Explanation:**  
Pandas UDFs (also called vectorized UDFs) use **Apache Arrow** to transfer data between the JVM and Python in columnar batches, dramatically reducing serialization overhead compared to standard Python UDFs which process one row at a time.  

- **Option A** is inefficient for NumPy-heavy work because row-by-row Python UDFs incur high serialization cost.  
- **Option B** is invalid — Spark DataFrames don't have `.apply()`.  
- **Option D** would break distributed processing and fail on large datasets that don't fit on the driver.  

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np

@pandas_udf(DoubleType())
def compute_score(s: pd.Series) -> pd.Series:
    return pd.Series(np.log1p(s.values))

df = df.withColumn("score", compute_score("amount"))
```

**Key knowledge:** Pandas UDFs were introduced in Spark 2.3. For Spark 3.x, use the type-hint syntax which is more Pythonic and avoids the deprecated `PandasUDFType` enum.
</details>

---

### Q2
A data engineer writes the following SQL query:

```sql
SELECT
  customer_id,
  order_date,
  amount,
  SUM(amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM orders;
```

What does this query compute?

**A.** The total amount for each customer across all orders  
**B.** The running cumulative sum of `amount` per customer, ordered by `order_date`  
**C.** The average amount per customer up to the current row  
**D.** The rank of each order by amount within each customer partition  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
`SUM(...) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` is the classic **running total (cumulative sum)** window function.  

- `PARTITION BY customer_id` — restarts the window for each customer  
- `ORDER BY order_date` — determines the ordering within each partition  
- `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — the frame includes all rows from the start of the partition up to and including the current row  

This is different from `RANGE BETWEEN` (which can include ties by value). `ROWS BETWEEN` always counts by physical row position.
</details>

---

### Q3
A data engineer wants to filter elements in an array column `tags` (type `ARRAY<STRING>`) to keep only tags that start with `"prod_"`. Which SQL expression is correct?

**A.** `SELECT FILTER(tags, t -> t LIKE 'prod_%') AS prod_tags FROM events`  
**B.** `SELECT EXPLODE(tags) AS t FROM events WHERE t LIKE 'prod_%'`  
**C.** `SELECT ARRAY_FILTER(tags, 'prod_%') AS prod_tags FROM events`  
**D.** `SELECT tags[tags LIKE 'prod_%'] AS prod_tags FROM events`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: A**

**Explanation:**  
The SQL **higher-order function `FILTER`** takes an array and a lambda expression `element -> condition` and returns a new array containing only elements where the condition is true. This is far more efficient than `EXPLODE` because it doesn't change the row structure.

- **Option B** with `EXPLODE` produces one row per array element — it changes cardinality and then requires re-aggregation.
- **Option C** — `ARRAY_FILTER` is not a valid Spark SQL function name; the correct name is `FILTER`.
- **Option D** — invalid syntax.

Other useful higher-order functions: `TRANSFORM(array, x -> expr)`, `AGGREGATE(array, zero, merge, finish)`, `EXISTS(array, x -> condition)`.
</details>

---

### Q4
A streaming job uses `trigger(availableNow=True)`. What is the behavior?

**A.** Processes data continuously without stopping  
**B.** Processes all available data at the time of trigger, then stops  
**C.** Processes exactly one micro-batch every second  
**D.** Processes data once and keeps the stream running indefinitely  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
`trigger(availableNow=True)` was introduced in Spark 3.3 as an improvement over `trigger(once=True)`. It processes **all available data** in multiple micro-batches (optimally) until caught up, then **terminates the stream**. This makes it ideal for scheduled batch-like streaming jobs (e.g., in Lakeflow Jobs).

| Trigger | Behavior |
|---|---|
| `once=True` | One micro-batch, then stop (deprecated in favor of `availableNow`) |
| `availableNow=True` | Multiple micro-batches until caught up, then stop |
| `processingTime='1 minute'` | Runs every 1 minute, continuous |
| `continuous='1 second'` | Experimental millisecond latency mode |

</details>

---

### Q5
A data engineer needs to pass a secret (database password) to a notebook securely without hardcoding it. Which `dbutils` command retrieves the secret?

**A.** `dbutils.secrets.get(scope="prod", key="db_password")`  
**B.** `dbutils.env.get("DB_PASSWORD")`  
**C.** `dbutils.notebook.getContext().getEnvVar("db_password")`  
**D.** `spark.conf.get("spark.secret.db_password")`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: A**

**Explanation:**  
Databricks **Secrets API** integrates with Azure Key Vault or Databricks-backed secret stores. Secrets are organized in **scopes** and accessed via `dbutils.secrets.get(scope, key)`. The secret value is **redacted** in notebook outputs (shown as `[REDACTED]`).

```python
password = dbutils.secrets.get(scope="prod-secrets", key="db_password")
# Use directly in connection string — value is masked in logs
url = f"jdbc:postgresql://server:5432/db?user=admin&password={password}"
```

**Best practice:** Never print secrets or store them in Delta tables. Use Unity Catalog external locations + service principals for storage access instead of embedding credentials.
</details>

---

## Section 2 — Data Ingestion & Acquisition — 7%

### Key Knowledge Points
- **Auto Loader:** Cloud-native file ingestion using `cloudFiles` source. Supports `directory listing` and `file notification` (event-based) modes. Schema inference, schema evolution, schema hints.
- **COPY INTO:** SQL-based idempotent batch ingestion. Tracks loaded files to avoid duplicates. Simpler but less scalable than Auto Loader for massive file counts.
- **Kafka / Event Hubs integration:** `readStream.format("kafka")`, topic subscription, offset management, binary `value` column must be cast.
- **JDBC ingestion:** `spark.read.jdbc()`, predicate pushdown, `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound`.
- **Lakeflow Connect:** Managed connectors for SaaS sources (Salesforce, etc.).

---

### Q6
An organization receives millions of new JSON files daily in an Azure Data Lake Storage container. A data engineer needs to incrementally ingest only new files into a Delta table. Which approach is BEST for production scale?

**A.** Use a `COPY INTO` statement in a daily scheduled job  
**B.** Use Auto Loader (`cloudFiles`) in a streaming job with `trigger(availableNow=True)`  
**C.** Use `spark.read.json()` with a `WHERE` clause filtering on file modification date  
**D.** Use `spark.read.format("delta").load()` with `.filter("date = current_date()")`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**Auto Loader** is purpose-built for large-scale incremental file ingestion. It uses cloud storage notifications (file notification mode) to discover new files efficiently without listing the entire directory — critical when millions of files accumulate.

- **Option A** — `COPY INTO` works for moderate file counts but struggles at millions-of-files scale because it tracks state in the Delta log (less efficient than Auto Loader's RocksDB-based checkpoint).
- **Option C** — File modification date is unreliable and `spark.read` (batch) cannot track state between runs.
- **Option D** — Reads from an existing Delta table, not a raw file source.

**Auto Loader config example:**
```python
df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", "/checkpoints/schema")
  .option("cloudFiles.useNotifications", "true")  # file notification mode
  .load("/mnt/raw/events/")
)
```
</details>

---

### Q7
When using Auto Loader, what is the purpose of `cloudFiles.schemaLocation`?

**A.** Specifies where to write the inferred schema so it persists across runs and enables schema evolution  
**B.** Points to the directory where source files are located  
**C.** Defines the checkpoint directory for streaming offsets  
**D.** Stores the list of already-processed files  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: A**

**Explanation:**  
`cloudFiles.schemaLocation` is a **persistent directory** where Auto Loader stores the inferred schema. This serves two purposes:
1. **Persistence** — the schema is remembered across restarts so re-inference is not needed every time.
2. **Evolution** — when new columns are added to incoming files, Auto Loader can detect the change and either merge the schema (with `mergeSchema=true`) or quarantine bad records.

Note: `checkpointLocation` (set on `writeStream`) tracks which files/offsets have been processed. These are two separate locations with different purposes.

**Schema evolution options:**  
- `cloudFiles.schemaEvolutionMode = "addNewColumns"` (default) — adds new columns as nullable  
- `cloudFiles.schemaEvolutionMode = "rescue"` — unknown columns go to `_rescued_data`  
- `cloudFiles.schemaEvolutionMode = "none"` — fails on schema change  
</details>

---

### Q8
A data engineer reads from a Kafka topic. The `value` column arrives as binary. The engineer needs to deserialize it as UTF-8 JSON and extract a `user_id` field. Which code is correct?

**A.**
```python
df.select(col("value").cast("string"), get_json_object(col("value"), "$.user_id"))
```
**B.**
```python
df.select(get_json_object(col("value"), "$.user_id"))
```
**C.**
```python
df.select(from_json(col("value"), schema).getField("user_id"))
```
**D.**
```python
df.select(json_tuple(col("value"), "user_id"))
```

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: A** (or **C** for full schema parsing)

**Explanation:**  
The Kafka `value` column is of type `BINARY`. You must **cast to STRING first** before JSON functions can parse it.  

- **Option B** — `get_json_object` expects a STRING input. Passing binary directly fails.
- **Option A** — Correctly casts to string then extracts a single field with `get_json_object`. This is appropriate for extracting a few fields.
- **Option C** — The most production-ready approach: `from_json` parses the full JSON into a struct using a declared schema, enabling type safety. Note: `value` must be cast to string first: `from_json(col("value").cast("string"), schema)`.
- **Option D** — `json_tuple` also requires a string input.

**Best practice for Kafka ingestion:**
```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

schema = StructType().add("user_id", StringType()).add("event_ts", LongType())

df_parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_ts")
).select("data.*", "kafka_ts")
```
</details>

---

## Section 3 — Data Transformation, Cleansing, and Quality — 10%

### Key Knowledge Points
- **Expectations in DLT (Declarative Pipelines):** `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`, `@dlt.expect_all`, `@dlt.expect_all_or_drop`, `@dlt.expect_all_or_fail`.
- **Constraint violations:** tracked in event log; use `dlt.read_stream("system.events")` to query.
- **Schema evolution in Delta:** `mergeSchema`, `overwriteSchema`.
- **`MERGE INTO` patterns:** SCD Type 1 (upsert), SCD Type 2 (history preservation).
- **Data quality rules:** null handling, deduplication (`dropDuplicates`, `QUALIFY ROW_NUMBER() = 1`), data type casting.

---

### Q9
A Declarative Pipeline (DLT) table uses `@dlt.expect_or_drop("valid_amount", "amount > 0")`. A batch of records arrives where 15% have `amount <= 0`. What happens?

**A.** The entire pipeline run fails with an exception  
**B.** All records are written, and a warning metric is recorded  
**C.** Records violating the expectation are silently dropped; compliant records are written  
**D.** Records violating the expectation are written to a quarantine table  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: C**

**Explanation:**  
DLT (Apache Spark Declarative Pipelines) expectations have three enforcement modes:

| Decorator | Behavior on Violation |
|---|---|
| `@dlt.expect` | Record the violation metric; **keep the record** (warn only) |
| `@dlt.expect_or_drop` | **Drop** violating records; rest are written normally |
| `@dlt.expect_or_fail` | **Fail** the pipeline update immediately |

For `expect_or_drop`: the 15% invalid records are dropped. The 85% valid records proceed. Violation statistics are stored in the **event log** and visible in the DLT pipeline UI.

**Accessing violation metrics:**
```sql
SELECT
  details:flow_progress:data_quality:dropped_records,
  details:flow_progress:data_quality:expectations
FROM event_log(TABLE(catalog.schema.pipeline_table))
WHERE event_type = 'flow_progress'
```
</details>

---

### Q10
A data engineer needs to implement **SCD Type 2** (Slowly Changing Dimension Type 2) to preserve full history of customer address changes. Which `MERGE INTO` pattern correctly accomplishes this?

**A.** Update the existing row's address whenever a change is detected  
**B.** When a change is detected, close the existing row (set `is_current = false`, `end_date = today`) and insert a new row with `is_current = true`  
**C.** Delete the old row and insert a new row with the updated address  
**D.** Append every incoming record regardless of whether it changed  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**SCD Type 2** maintains full history by **never updating or deleting old rows**. Instead, when a change is detected:
1. The **existing current row** is updated: set `is_current = FALSE` and `end_date = current_date`
2. A **new row** is inserted with the new data and `is_current = TRUE`, `start_date = current_date`, `end_date = NULL`

```sql
MERGE INTO customers_dim AS target
USING (
  SELECT customer_id, new_address, current_date() AS effective_date
  FROM staging
) AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND target.address != source.new_address THEN
  UPDATE SET target.is_current = FALSE, target.end_date = source.effective_date
WHEN NOT MATCHED THEN
  INSERT (customer_id, address, start_date, end_date, is_current)
  VALUES (source.customer_id, source.new_address, source.effective_date, NULL, TRUE)
```

Note: Delta Lake's `MERGE INTO` supports multiple `WHEN MATCHED` clauses, which is needed for the full SCD Type 2 pattern (close + insert in one merge).
</details>

---

### Q11
A DataFrame contains duplicate events based on `event_id`. A data engineer wants to keep only the **most recent** record per `event_id` (based on `event_timestamp`). Which approach is correct?

**A.** `df.dropDuplicates(["event_id"])`  
**B.**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("event_id").orderBy(desc("event_timestamp"))
df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
```
**C.** `df.groupBy("event_id").agg(max("event_timestamp"))`  
**D.** `df.orderBy(desc("event_timestamp")).dropDuplicates(["event_id"])`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
- **Option A** — `dropDuplicates` drops rows with all specified columns identical, keeping an arbitrary duplicate. It does not guarantee keeping the most recent.
- **Option C** — `groupBy + max` returns only `event_id` and `max timestamp`, not the full row.
- **Option D** — `orderBy` then `dropDuplicates` is **non-deterministic** in distributed Spark; ordering is not guaranteed to be globally respected before `dropDuplicates`.
- **Option B** — Using a **window function with `row_number()`** is the correct, deterministic approach. It assigns rank 1 to the most recent event per `event_id`, then filters to keep only rank 1 rows while preserving all columns.

This is the canonical deduplication pattern in production Spark pipelines.
</details>

---

## Section 4 — Data Sharing and Federation — 5%

### Key Knowledge Points
- **Delta Sharing:** Open protocol for sharing data across clouds/organizations without copying data. Provider creates shares; recipients query via REST API or connectors.
- **Unity Catalog Federation:** Query external data sources (PostgreSQL, MySQL, Snowflake, etc.) directly via **Foreign Catalogs** without moving data.
- **Lakehouse Federation:** Uses `CREATE CONNECTION` and `CREATE FOREIGN CATALOG` in Unity Catalog.
- **Delta Sharing vs. Lakehouse Federation:** Sharing = giving access to Delta/Iceberg tables to external parties. Federation = querying external databases from within Databricks.

---

### Q12
A company wants to share a Delta table with a partner organization using a different cloud provider, without copying data or granting direct storage access. Which technology is MOST appropriate?

**A.** Databricks Lakehouse Federation with a Foreign Catalog  
**B.** Delta Sharing with the Databricks-to-Databricks sharing model  
**C.** COPY INTO to export data to the partner's storage  
**D.** Unity Catalog external table pointing to the partner's storage  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**Delta Sharing** is designed exactly for this use case: cross-organizational, cross-cloud data sharing **without data duplication** and without exposing underlying storage credentials.

- The **provider** creates a Share object in Unity Catalog and adds tables to it.
- The **recipient** (even on a different cloud or using open-source Delta Sharing clients) gets a time-limited credential token to access the data via the Delta Sharing REST protocol.
- Data stays in the provider's storage; the recipient reads directly from the signed URLs.

**Option A** (Lakehouse Federation) is for the opposite direction: querying **external systems from within Databricks**.

```sql
-- Provider side
CREATE SHARE partner_share;
ALTER SHARE partner_share ADD TABLE catalog.schema.sales_data;
CREATE RECIPIENT partner_org USING ID 'partner@company.com';
GRANT SELECT ON SHARE partner_share TO RECIPIENT partner_org;
```
</details>

---

### Q13
A data engineer needs to query a PostgreSQL database table from within Databricks SQL without ETL. Which Unity Catalog feature enables this?

**A.** Delta Sharing  
**B.** Lakehouse Federation (Foreign Catalog via `CREATE CONNECTION`)  
**C.** Auto Loader with JDBC format  
**D.** External tables with JDBC location  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

**Explanation:**  
**Lakehouse Federation** allows Databricks to query external databases (PostgreSQL, MySQL, Snowflake, SQL Server, etc.) **in-place** using federated queries. The data is not copied into the lakehouse.

```sql
-- Step 1: Create connection to PostgreSQL
CREATE CONNECTION pg_prod
TYPE POSTGRESQL
OPTIONS (
  host 'pg.prod.company.com',
  port '5432',
  database 'analytics',
  user SECRET ('scope', 'pg_user'),
  password SECRET ('scope', 'pg_password')
);

-- Step 2: Create foreign catalog
CREATE FOREIGN CATALOG pg_catalog
USING CONNECTION pg_prod;

-- Step 3: Query directly
SELECT * FROM pg_catalog.public.customers LIMIT 100;
```

Connections are governed by Unity Catalog — only users with `USE CONNECTION` privilege can use them.
</details>

---

### Q14
A data team shares a Delta table using Delta Sharing. The recipient has Databricks. How does the recipient access the shared data?

**A.** The recipient clones the Delta table into their workspace  
**B.** The recipient mounts the provider's storage account  
**C.** The recipient creates a share alias in their Unity Catalog using the activation link  
**D.** The recipient runs `SYNC TABLE` to pull data on a schedule  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: C**

**Explanation:**  
With **Databricks-to-Databricks Delta Sharing**, the recipient:
1. Receives an **activation link** (email or shared securely)
2. Opens it to create a **share alias** in their Unity Catalog (`CREATE CATALOG USING SHARE <provider>.<share>`)
3. Queries the data using standard SQL — no data movement, no direct storage access

This is a **read-only, live view** of the provider's data — changes made by the provider are immediately visible to the recipient. The provider controls access and can revoke it at any time.
</details>

---

### Q15 — Bonus: Code + Ingestion Scenario
A data engineer builds an Auto Loader pipeline that ingests JSON files. After running in production for 3 months, the source team adds a new field `region` to the JSON. The pipeline uses `cloudFiles.schemaEvolutionMode = "rescue"`. What happens to the `region` field in new records?

**A.** The pipeline fails with a `SchemaEvolutionException`  
**B.** The `region` field is silently ignored  
**C.** The `region` field and its value are stored in the `_rescued_data` column as a JSON string  
**D.** The schema automatically evolves to add `region` as a new column  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: C**

**Explanation:**  
With `schemaEvolutionMode = "rescue"`, any **unexpected columns** that don't match the existing schema are not dropped or cause failures — instead, the values are captured in the `_rescued_data` column as a JSON string. This is useful for:
- Auditing unexpected schema changes without pipeline failures
- Recovering data that would otherwise be lost

To add `region` as a proper column, change to `schemaEvolutionMode = "addNewColumns"` and restart the stream. Auto Loader will then update the stored schema at `schemaLocation` and start materializing `region` as a proper column.

**Schema Evolution Modes Summary:**
| Mode | Behavior |
|---|---|
| `addNewColumns` | New columns added to schema automatically |
| `rescue` | New columns go to `_rescued_data` |
| `failOnNewColumns` | Pipeline fails if new column detected |
| `none` | New columns silently ignored (no rescue) |
</details>

---

### Q16–Q20 — Mixed Rapid Fire

### Q16
Which Delta Lake feature allows you to query data **as it existed 7 days ago**?

**A.** `DESCRIBE HISTORY`  
**B.** `SELECT * FROM table TIMESTAMP AS OF '2026-07-12'`  
**C.** `RESTORE TABLE table TO TIMESTAMP AS OF '2026-07-12'`  
**D.** `VACUUM table RETAIN 168 HOURS`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

`TIME TRAVEL` syntax — `TIMESTAMP AS OF` or `VERSION AS OF` — queries the table at a historical snapshot. Data must not have been vacuumed. Option C **restores** the table (destructive to current state). Option D sets retention for vacuum.
</details>

---

### Q17
A job needs to read parameters passed from a Lakeflow Job. Which code retrieves the `env` parameter?

**A.** `os.environ["env"]`  
**B.** `dbutils.widgets.get("env")`  
**C.** `spark.conf.get("env")`  
**D.** `dbutils.jobs.get("env")`  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

Lakeflow Jobs pass notebook parameters as **widgets**. `dbutils.widgets.get("env")` retrieves the widget value. Best practice: define a default with `dbutils.widgets.text("env", "dev")` at the top of the notebook for local testing.
</details>

---

### Q18
A `COPY INTO` command is run twice on the same source directory. What happens on the second run?

**A.** All files are re-ingested, creating duplicates  
**B.** An error is raised because the target table already has data  
**C.** Only new files not previously loaded are ingested (idempotent)  
**D.** The second run is skipped silently with no output  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: C**

`COPY INTO` is **idempotent** — it tracks which files have been loaded in the Delta table's transaction log and skips already-processed files. This makes it safe to re-run. Note: use `COPY INTO ... FORMAT_OPTIONS ('mergeSchema' = 'true')` if schema may change.
</details>

---

### Q19
A data engineer uses `from_json()` to parse a column. The JSON string contains a field not defined in the schema. What is the default behavior?

**A.** `from_json` throws a parse exception  
**B.** The extra field is silently ignored; only schema-defined fields are returned  
**C.** The extra field is added dynamically to the struct  
**D.** A `_corrupt_record` column captures the raw JSON  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

`from_json` only extracts fields defined in the declared schema. Unknown fields are **silently ignored**. If the JSON is malformed (invalid syntax), the result is `null` for the struct. Add a `_corrupt_record` column by specifying `options({"mode": "PERMISSIVE"})` and including the column in the schema.
</details>

---

### Q20
A Declarative Pipeline is defined with `table_properties = {"pipelines.autoOptimize.managed": "true"}`. What does this enable?

**A.** Automatic schema evolution for the table  
**B.** Databricks manages Auto Optimize (auto compaction + optimized writes) for the DLT table  
**C.** The table is automatically vacuumed after each pipeline run  
**D.** Statistics are automatically collected for query optimization  

<details>
<summary>✅ Answer & Explanation</summary>

**Answer: B**

Setting `pipelines.autoOptimize.managed = true` enables **Auto Optimize** (Optimized Writes + Auto Compaction) for the DLT-managed Delta table. Databricks automatically manages file compaction to prevent small-file problems — critical for streaming pipelines that append frequently.
</details>

---

## Score Guide

| Correct | Score | Assessment |
|---|---|---|
| 18–20 | 90–100% | Excellent — Exam ready |
| 15–17 | 75–85% | Good — Review weak areas |
| 12–14 | 60–70% | Borderline — More practice needed |
| < 12 | < 60% | Needs significant study |

**Exam passing score: 70% (≥ 42/59 on the real exam)**

---

*Source: Databricks Certified Data Engineer Professional Exam Guide, July 2026 edition*
