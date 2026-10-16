# Day 2 Practice Tasks — Auto Loader & Advanced Ingestion Patterns

> **Exam section:** Data Ingestion & Acquisition (7%)  
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.  
> **Estimated time:** 2–3 hours  
> **Difficulty:** ⭐⭐⭐ Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last.  
Every task has:
- 📖 **Context** — why this matters for the exam
- 🛠 **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- 💡 **Exam trap** — a common wrong-answer pitfall

---

## Task 1 — Auto Loader Baseline: JSON Ingestion with Schema Inference

📖 **Context:**  
Auto Loader (`cloudFiles`) is tested heavily in the Professional exam. You must understand
`schemaLocation`, `inferColumnTypes`, and `trigger(availableNow=True)` — the most
common options tested in multiple-choice questions.

🛠 **Instructions:**

**Step 1 — Create the landing volume directory structure:**
```python
# Run in a Databricks notebook cell
dbutils.fs.mkdirs("/Volumes/training/prep/landing/raw_json/")
dbutils.fs.mkdirs("/Volumes/training/prep/landing/checkpoints/day2/schema_json/")
dbutils.fs.mkdirs("/Volumes/training/prep/landing/checkpoints/day2/stream_json/")
```

**Step 2 — Seed sample data (simulate arriving files):**
```python
import json

sample_events = [
    {"event_id": 1, "event_type": "click", "user_id": "u001", "amount": 9.99,  "event_ts": "2026-07-01T08:00:00"},
    {"event_id": 2, "event_type": "view",  "user_id": "u002", "amount": None,  "event_ts": "2026-07-01T08:01:00"},
    {"event_id": 3, "event_type": "buy",   "user_id": "u001", "amount": 49.99, "event_ts": "2026-07-01T08:05:00"},
]

# Write a single JSON file (one record per line = JSON Lines format)
content = "\n".join(json.dumps(e) for e in sample_events)
dbutils.fs.put("/Volumes/training/prep/landing/raw_json/batch_001.json", content, overwrite=True)
print("File written. Verifying:")
dbutils.fs.ls("/Volumes/training/prep/landing/raw_json/")
```

**Step 3 — Write the Auto Loader streaming query:**
```python
from pyspark.sql.functions import current_timestamp

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")          # <-- key option
    .option("cloudFiles.schemaLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/schema_json/")
    .load("/Volumes/training/prep/landing/raw_json/")
)

# Add ingestion metadata
df_enriched = df.withColumn("_ingested_at", current_timestamp()) \
                .withColumn("_source_file", df["_metadata.file_path"])

query = (
    df_enriched.writeStream
    .format("delta")
    .option("checkpointLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/stream_json/")
    .trigger(availableNow=True)                             # <-- key option
    .table("training.prep.day2_raw_events")
)
query.awaitTermination()
```

**Step 4 — Verify results:**
```sql
-- In a SQL cell
SELECT * FROM training.prep.day2_raw_events;
DESCRIBE EXTENDED training.prep.day2_raw_events;
```

✅ **Expected outcome:**
- 3 rows in `day2_raw_events`
- `amount` column is `DOUBLE` (not `STRING`) because `inferColumnTypes=true`
- `event_ts` is `TIMESTAMP`
- `_ingested_at` and `_source_file` metadata columns are populated
- Query terminates after processing all available files (because of `availableNow=True`)

💡 **Exam trap:**  
Without `cloudFiles.inferColumnTypes=true`, JSON fields default to `STRING`.
The exam often shows a query where someone queries `amount > 10.0` on an all-string schema
and asks why it returns unexpected results.

---

## Task 2 — Schema Evolution: addNewColumns vs rescue Mode

📖 **Context:**  
Schema evolution is a core Professional exam topic. You must know the difference between all
four `schemaEvolutionMode` options and when Databricks automatically rescues data.

🛠 **Instructions:**

**Step 1 — Simulate a schema change (new field arrives):**
```python
import json

# New batch has an extra field: "region" — schema change!
new_events = [
    {"event_id": 4, "event_type": "buy", "user_id": "u003",
     "amount": 99.99, "event_ts": "2026-07-01T09:00:00", "region": "EU"},
    {"event_id": 5, "event_type": "click", "user_id": "u004",
     "amount": 5.00,  "event_ts": "2026-07-01T09:01:00", "region": "US"},
]
content = "\n".join(json.dumps(e) for e in new_events)
dbutils.fs.put("/Volumes/training/prep/landing/raw_json/batch_002.json", content, overwrite=True)
```

**Step 2 — Re-run Auto Loader with `addNewColumns` mode:**
```python
# IMPORTANT: Delete old checkpoint first to reset schema tracking
dbutils.fs.rm("/Volumes/training/prep/landing/checkpoints/day2/schema_json/", recurse=True)
dbutils.fs.rm("/Volumes/training/prep/landing/checkpoints/day2/stream_json/", recurse=True)
spark.sql("DROP TABLE IF EXISTS training.prep.day2_raw_events_evolved")

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # <-- key option
    .option("cloudFiles.schemaLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/schema_json/")
    .load("/Volumes/training/prep/landing/raw_json/")
)

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/stream_json/")
    .trigger(availableNow=True)
    .table("training.prep.day2_raw_events_evolved")
)
query.awaitTermination()
```

**Step 3 — Verify schema evolution:**
```sql
DESCRIBE training.prep.day2_raw_events_evolved;
SELECT event_id, region FROM training.prep.day2_raw_events_evolved ORDER BY event_id;
```

**Step 4 — Answer these questions in a comment cell:**
```python
# Q1: What value does the "region" column have for rows from batch_001.json?
# Answer: ___________

# Q2: What would happen to "region" values if mode was "rescue" instead of "addNewColumns"?
# Answer: ___________

# Q3: When would you choose "failOnNewColumns" in production?
# Answer: ___________
```

✅ **Expected outcome:**
- `DESCRIBE` shows a `region STRING` column added automatically
- Rows from `batch_001.json` have `NULL` in `region`
- Rows from `batch_002.json` have `"EU"` or `"US"` in `region`

💡 **Exam trap:**  
`addNewColumns` adds the column to **both** the stream schema AND the target Delta table.
The exam asks: "A stream fails when a new field arrives — which `schemaEvolutionMode` was likely configured?"
Answer: `failOnNewColumns`.

---

## Task 3 — COPY INTO: Idempotent Batch Ingestion

📖 **Context:**  
`COPY INTO` is an important exam concept often contrasted with Auto Loader.
Key exam question type: "Which should you use when X?" — you must know the correct answer.

🛠 **Instructions:**

**Step 1 — Create the target table:**
```sql
CREATE OR REPLACE TABLE training.prep.day2_copy_into_events (
    event_id   BIGINT,
    event_type STRING,
    user_id    STRING,
    amount     DOUBLE,
    event_ts   TIMESTAMP,
    region     STRING,
    _load_ts   TIMESTAMP
)
USING DELTA
COMMENT 'Events loaded via COPY INTO for batch ingestion demo';
```

**Step 2 — Run COPY INTO:**
```sql
COPY INTO training.prep.day2_copy_into_events
FROM (
  SELECT
    CAST(event_id   AS BIGINT)    AS event_id,
    event_type,
    user_id,
    CAST(amount     AS DOUBLE)    AS amount,
    CAST(event_ts   AS TIMESTAMP) AS event_ts,
    region,
    current_timestamp()           AS _load_ts
  FROM '/Volumes/training/prep/landing/raw_json/'
)
FILEFORMAT = JSON
PATTERN = '*.json'
COPY_OPTIONS ('force' = 'false', 'mergeSchema' = 'true');
```

**Step 3 — Run COPY INTO a second time (idempotency test):**
```sql
-- Re-run the EXACT same COPY INTO statement above again.
-- Then check the row count:
SELECT COUNT(*) AS row_count FROM training.prep.day2_copy_into_events;
```

**Step 4 — Force re-load with `force=true`:**
```sql
COPY INTO training.prep.day2_copy_into_events
FROM '/Volumes/training/prep/landing/raw_json/'
FILEFORMAT = JSON
COPY_OPTIONS ('force' = 'true');   -- <-- forces reload of already-loaded files
SELECT COUNT(*) AS row_count FROM training.prep.day2_copy_into_events;
```

**Step 5 — Answer in a comment cell:**
```python
# Q1: After step 3, is the row count the same as after step 2? Why?
# Answer: ___________

# Q2: After step 4, has the row count doubled? Why?
# Answer: ___________

# Q3: Auto Loader vs COPY INTO — complete this decision table:
#
# | Scenario                                      | Use Auto Loader | Use COPY INTO |
# |-----------------------------------------------|-----------------|---------------|
# | Continuous ingestion of millions of new files |                 |               |
# | One-time historical backfill of 500 files     |                 |               |
# | Nightly scheduled batch from a known folder   |                 |               |
# | Real-time low-latency stream from cloud store |                 |               |
```

✅ **Expected outcome:**
- After step 3: same row count as step 2 — `force=false` skips already-loaded files
- After step 4: row count doubles — `force=true` reloads all files regardless

💡 **Exam trap:**  
`COPY INTO` with `force=false` (default) is idempotent — it tracks loaded files internally.
The exam may show code that runs `COPY INTO` in a loop and ask if it causes duplicates.
Answer: No — not with default `force=false`.

---

## Task 4 — Auto Loader Metadata Columns & File-Level Lineage

📖 **Context:**  
The `_metadata` struct is a favourite exam topic. You must know exactly which fields exist
and how to persist them for downstream audit/lineage purposes.

🛠 **Instructions:**

**Step 1 — Inspect available metadata columns:**
```python
# Create a stream and display metadata WITHOUT writing to Delta
df_meta = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/meta_schema/")
    .load("/Volumes/training/prep/landing/raw_json/")
)

# Print available metadata fields
print(df_meta.schema.simpleString())
```

**Step 2 — Write with full lineage columns:**
```python
from pyspark.sql.functions import col, current_timestamp

df_with_lineage = (
    df_meta
    .withColumn("source_file_path",  col("_metadata.file_path"))
    .withColumn("source_file_name",  col("_metadata.file_name"))
    .withColumn("source_file_size",  col("_metadata.file_size"))
    .withColumn("source_file_mtime", col("_metadata.file_modification_time"))
    .withColumn("ingested_at",       current_timestamp())
    # Drop _metadata struct before writing (not supported as a Delta column)
    .drop("_metadata")
)

query = (
    df_with_lineage.writeStream
    .format("delta")
    .option("checkpointLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/lineage_stream/")
    .trigger(availableNow=True)
    .table("training.prep.day2_events_with_lineage")
)
query.awaitTermination()
```

**Step 3 — Validate lineage data:**
```sql
SELECT
    event_id,
    source_file_name,
    source_file_size,
    source_file_mtime,
    ingested_at
FROM training.prep.day2_events_with_lineage
ORDER BY event_id;
```

**Step 4 — Answer in a comment cell:**
```python
# Q1: Which metadata field would you use to detect files modified AFTER ingestion started?
# Answer: ___________

# Q2: Can you filter on _metadata.file_name INSIDE the .load() call?
# (Hint: try adding .filter(col("_metadata.file_name") == "batch_001.json") before writeStream)
# Answer: ___________
```

✅ **Expected outcome:**
- `source_file_name` shows `batch_001.json` and `batch_002.json` for the corresponding rows
- `source_file_size` is populated (non-zero bytes)
- `ingested_at` is a recent timestamp

💡 **Exam trap:**  
`_metadata` is a **struct** — you cannot write it as a column directly to Delta.
You must extract fields individually first. The exam may ask what happens if you skip `.drop("_metadata")`.

---

## Task 5 — Kafka Ingestion: Decode & Parse JSON Payload

📖 **Context:**  
Azure Event Hubs uses the Kafka protocol, making Kafka ingestion directly relevant
for Azure Databricks deployments. The exam tests your knowledge of key/value decoding
and `from_json` for payload parsing.

🛠 **Instructions:**

**Step 1 — Simulate a Kafka-like DataFrame in memory (no broker needed):**
```python
from pyspark.sql import Row
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, LongType, TimestampType
import json

# Simulate what Kafka returns — key and value are BINARY
kafka_rows = [
    Row(key=b"order-001", value=json.dumps({"order_id": 1, "customer": "Alice", "amount": 120.50}).encode(), topic="orders", partition=0, offset=0, timestamp=None),
    Row(key=b"order-002", value=json.dumps({"order_id": 2, "customer": "Bob",   "amount": 85.00}).encode(),  topic="orders", partition=0, offset=1, timestamp=None),
    Row(key=b"order-003", value=json.dumps({"order_id": 3, "customer": "Carol", "amount": 210.75}).encode(), topic="orders", partition=0, offset=2, timestamp=None),
]

kafka_schema = StructType([
    StructField("key",       BinaryType(),    True),
    StructField("value",     BinaryType(),    True),
    StructField("topic",     StringType(),    True),
    StructField("partition", LongType(),      True),
    StructField("offset",    LongType(),      True),
    StructField("timestamp", TimestampType(), True),
])

kafka_df = spark.createDataFrame(kafka_rows, schema=kafka_schema)
kafka_df.printSchema()
kafka_df.show(truncate=False)
```

**Step 2 — Decode key and value:**
```python
# Step 2a: Cast binary key and value to strings
decoded_df = kafka_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("kafka_value_raw"),
    col("topic"),
    col("partition"),
    col("offset"),
)
decoded_df.show(truncate=False)
```

**Step 3 — Parse JSON value with `from_json`:**
```python
# Define the schema of the JSON payload
order_schema = "order_id BIGINT, customer STRING, amount DOUBLE"

parsed_df = decoded_df.select(
    col("kafka_key"),
    from_json(col("kafka_value_raw"), order_schema).alias("order"),
    col("topic"),
    col("offset"),
).select(
    "kafka_key",
    "order.*",     # Flatten the struct
    "topic",
    "offset",
)

parsed_df.show()
parsed_df.printSchema()
```

**Step 4 — Answer in a comment cell:**
```python
# Q1: In a real Kafka stream, what is the type of the "value" column BEFORE casting?
# Answer: ___________

# Q2: What happens to records where the JSON value is malformed (unparseable)?
#     (Hint: what does from_json() return for a bad JSON string?)
# Answer: ___________

# Q3: In Azure, which service is "Kafka-protocol compatible" and commonly used with Databricks?
# Answer: ___________

# Q4: Write the readStream code skeleton for a REAL Kafka source (fill in the blanks):
# df = (
#     spark.readStream
#     .format("_______")
#     .option("kafka.bootstrap.servers", "host:9092")
#     .option("_______", "orders-topic")
#     .option("startingOffsets", "_______")    # start from beginning
#     .load()
# )
```

✅ **Expected outcome:**
- `parsed_df` has columns: `kafka_key`, `order_id`, `customer`, `amount`, `topic`, `offset`
- `amount` is `DOUBLE`, `order_id` is `BIGINT` (not strings)
- Schema is flat (no nested struct)

💡 **Exam trap:**  
`from_json` returns `NULL` for all fields when the JSON is malformed — it does NOT raise an error.
The exam may ask how to detect records that failed to parse:
```python
# Detect failed parses
parsed_df.filter(col("order_id").isNull()).count()
```

---

## Task 6 — Multi-Format Ingestion: CSV and Parquet

📖 **Context:**  
Auto Loader handles multiple formats. The exam tests whether you know the correct
`cloudFiles.format` value and format-specific options (like `header` for CSV).

🛠 **Instructions:**

**Step 1 — Create CSV sample data:**
```python
csv_content = """product_id,product_name,price,category
P001,Laptop,999.99,Electronics
P002,Notebook,4.99,Stationery
P003,Headphones,149.99,Electronics
P004,Pen,1.29,Stationery
"""
dbutils.fs.mkdirs("/Volumes/training/prep/landing/raw_csv/")
dbutils.fs.put("/Volumes/training/prep/landing/raw_csv/products_001.csv", csv_content, overwrite=True)
```

**Step 2 — Ingest CSV with Auto Loader:**
```python
df_csv = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")          # format = csv
    .option("header", "true")                     # CSV-specific option
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/csv_schema/")
    .load("/Volumes/training/prep/landing/raw_csv/")
)

query_csv = (
    df_csv.writeStream
    .format("delta")
    .option("checkpointLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/csv_stream/")
    .trigger(availableNow=True)
    .table("training.prep.day2_products_csv")
)
query_csv.awaitTermination()
```

**Step 3 — Create and ingest Parquet data:**
```python
# Write a small Parquet file first
import pandas as pd
transactions = pd.DataFrame({
    "txn_id":    [1001, 1002, 1003],
    "product_id":["P001", "P002", "P003"],
    "qty":       [2, 5, 1],
    "txn_date":  ["2026-07-01", "2026-07-01", "2026-07-02"],
})
spark_txn = spark.createDataFrame(transactions)
spark_txn.write.mode("overwrite").parquet(
    "/Volumes/training/prep/landing/raw_parquet/transactions_001"
)

# Ingest with Auto Loader
df_parquet = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")       # format = parquet
    .option("cloudFiles.schemaLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/parquet_schema/")
    .load("/Volumes/training/prep/landing/raw_parquet/")
)

query_parquet = (
    df_parquet.writeStream
    .format("delta")
    .option("checkpointLocation",
            "/Volumes/training/prep/landing/checkpoints/day2/parquet_stream/")
    .trigger(availableNow=True)
    .table("training.prep.day2_transactions_parquet")
)
query_parquet.awaitTermination()
```

**Step 4 — Compare results:**
```sql
SELECT 'csv'     AS source, COUNT(*) AS rows FROM training.prep.day2_products_csv
UNION ALL
SELECT 'parquet' AS source, COUNT(*) AS rows FROM training.prep.day2_transactions_parquet;
```

✅ **Expected outcome:**
- `day2_products_csv` has 4 rows with `price DOUBLE` (from `inferSchema=true`)
- `day2_transactions_parquet` has 3 rows with proper types preserved from Parquet schema

💡 **Exam trap:**  
For Parquet, you do NOT need `inferSchema` — Parquet already embeds the schema.
For CSV, `header=true` only skips the header row; you still need `inferSchema=true` or manual type hints for correct types.

---

## Task 7 — Exam-Style Multiple Choice (Self-Assessment)

Answer each question, then reveal the answer below.

**Q1.** An Auto Loader stream has been running for 2 days. A new JSON file arrives with a previously unseen field `device_type`. With `schemaEvolutionMode = "rescue"`, what happens?

- A) The stream fails with a schema mismatch error
- B) `device_type` is added as a new column automatically
- C) The entire record is discarded
- D) `device_type` value is stored in `_rescued_data` as a JSON string

<details>
<summary>🔍 Reveal Answer</summary>

**D** — `rescue` mode stores unexpected fields in `_rescued_data` as JSON. No stream failure.  
`addNewColumns` would produce answer B.  
`failOnNewColumns` would produce answer A.

</details>

---

**Q2.** You run `COPY INTO` three times on the same source folder with `force = 'false'` (default). How many times are the source files loaded?

- A) 3 — each run reloads all files
- B) 1 — already-loaded files are tracked and skipped
- C) 2 — second run catches any missed files
- D) Depends on whether the target table has a MERGE operation

<details>
<summary>🔍 Reveal Answer</summary>

**B** — `COPY INTO` tracks loaded files in Delta metadata. Subsequent runs with `force=false` skip them.

</details>

---

**Q3.** Which Auto Loader trigger mode processes all currently available files and then **terminates the stream**?

- A) `.trigger(once=True)`
- B) `.trigger(continuous="1 second")`
- C) `.trigger(availableNow=True)`
- D) `.trigger(processingTime="0 seconds")`

<details>
<summary>🔍 Reveal Answer</summary>

**C** — `trigger(availableNow=True)` is the modern replacement for `trigger(once=True)`.  
It processes all available files in multiple micro-batches (more efficient) then stops.  
`trigger(once=True)` processes in a single micro-batch (legacy, deprecated for large loads).

</details>

---

**Q4.** A data engineering team needs to ingest 50 million small JSON files from Azure Blob Storage with minimum latency. Which Auto Loader mode should they use?

- A) Directory Listing (default)
- B) File Notification mode via Azure Event Grid
- C) COPY INTO with `force=true`
- D) Kafka streaming source

<details>
<summary>🔍 Reveal Answer</summary>

**B** — File Notification mode uses Azure Event Grid to push new-file events rather than listing all 50M files.  
Directory Listing scales poorly at that file count.

</details>

---

**Q5.** In a Kafka stream, what is the data type of the `value` column returned by Databricks Structured Streaming?

- A) `STRING`
- B) `MAP<STRING, STRING>`
- C) `BINARY`
- D) `STRUCT`

<details>
<summary>🔍 Reveal Answer</summary>

**C** — Both `key` and `value` are `BINARY`. You must cast to `STRING` before parsing with `from_json`.

</details>

---

## Task 8 — Clean Up Resources

```python
# Run this cell when done to free up space
tables_to_drop = [
    "training.prep.day2_raw_events",
    "training.prep.day2_raw_events_evolved",
    "training.prep.day2_copy_into_events",
    "training.prep.day2_events_with_lineage",
    "training.prep.day2_products_csv",
    "training.prep.day2_transactions_parquet",
]
for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    print(f"Dropped: {t}")

dbutils.fs.rm("/Volumes/training/prep/landing/raw_json/",    recurse=True)
dbutils.fs.rm("/Volumes/training/prep/landing/raw_csv/",     recurse=True)
dbutils.fs.rm("/Volumes/training/prep/landing/raw_parquet/", recurse=True)
dbutils.fs.rm("/Volumes/training/prep/landing/checkpoints/day2/", recurse=True)
print("Cleanup complete.")
```

---

## Day 2 Summary Checklist

Before moving to Day 3, confirm you can answer YES to all of these:

- [ ] I can write an Auto Loader stream with correct `cloudFiles.*` options from memory
- [ ] I know when to use `addNewColumns` vs `rescue` vs `failOnNewColumns`
- [ ] I understand why `schemaLocation` is mandatory and what it stores
- [ ] I can explain the difference between `trigger(availableNow=True)` and `trigger(once=True)`
- [ ] I know when COPY INTO is preferred over Auto Loader (and vice versa)
- [ ] I can decode and parse a Kafka value payload using `from_json`
- [ ] I know which metadata fields are available in `_metadata` for Auto Loader
- [ ] I scored ≥ 4/5 on Task 7 multiple-choice questions

> 👉 **Next:** [Day 3 — Lakeflow Declarative Pipelines (DLT Advanced)](../day3/study-notes.md)
