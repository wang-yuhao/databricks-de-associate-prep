# Day 2 — Auto Loader & Advanced Ingestion Patterns (7% of exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day2_autoloader.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review ingestion pattern questions

---

## 2.1 Auto Loader Overview

Auto Loader (`cloudFiles`) incrementally ingests new files from cloud storage into Delta Lake.

### How It Works
1. Lists new files in source path (or uses file notification service)
2. Tracks which files have been processed in a **checkpoint**
3. Processes only **new** files on each trigger
4. Infers or enforces schema

```python
# Basic Auto Loader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")         # source format
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/schema")
    .load("/Volumes/training/prep/landing/raw/")
)

df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/training/prep/landing/checkpoints/autoloader")
  .trigger(availableNow=True)  # process all available, then stop
  .table("training.prep.raw_events")
```

---

## 2.2 Auto Loader File Notification vs Directory Listing

| Mode | Mechanism | Best For |
|------|-----------|----------|
| **Directory Listing** (default) | Lists all files, diffs against checkpoint | Small to medium scale, simple setup |
| **File Notification** | Cloud storage events (Azure Event Grid, AWS SQS) | Large scale, millions of files, low latency |

```python
# File Notification mode (Azure Event Grid)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")      # enable notifications
    .option("cloudFiles.resourceGroup", "my-rg")
    .option("cloudFiles.subscriptionId", "sub-id")
    .option("cloudFiles.tenantId", "tenant-id")
    .option("cloudFiles.clientId", dbutils.secrets.get("kv", "client-id"))
    .option("cloudFiles.clientSecret", dbutils.secrets.get("kv", "client-secret"))
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/schema")
    .load("abfss://container@account.dfs.core.windows.net/raw/")
)
```

---

## 2.3 Schema Inference & Evolution

```python
# Schema inference (first run infers, subsequent runs use cached schema)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/schema")
    .option("cloudFiles.inferColumnTypes", "true")       # infer proper types vs all string
    .load("/Volumes/training/prep/landing/raw/")
)

# Schema hints (override inference for specific columns)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/schema")
    .option("cloudFiles.schemaHints", "event_ts TIMESTAMP, amount DOUBLE")
    .load("/Volumes/training/prep/landing/raw/")
)

# Schema evolution: rescue column captures unexpected fields
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # auto-add new cols
    .load("/Volumes/training/prep/landing/raw/")
)
# Other modes: rescue, failOnNewColumns, none
```

### Schema Evolution Modes
| Mode | Behaviour |
|------|-----------|
| `addNewColumns` | Auto-adds new columns to schema |
| `rescue` | Puts unmatched columns into `_rescued_data` JSON column |
| `failOnNewColumns` | Fails stream on schema mismatch |
| `none` | Ignores new columns |

---

## 2.4 Auto Loader Metadata Columns

Auto Loader adds useful metadata columns automatically:

```python
# Metadata available in cloudFiles source
df.select(
    "*",
    "_metadata.file_path",       # path of source file
    "_metadata.file_name",       # just the filename
    "_metadata.file_size",       # bytes
    "_metadata.file_modification_time"  # last modified timestamp
).display()
```

---

## 2.5 COPY INTO (Batch Alternative to Auto Loader)

`COPY INTO` is idempotent batch ingestion (not streaming):

```sql
COPY INTO training.prep.raw_events
FROM (
  SELECT
    *,
    _metadata.file_path AS source_file,
    current_timestamp() AS ingested_at
  FROM '/Volumes/training/prep/landing/raw/'
)
FILEFORMAT = JSON
PATTERN = '*.json'
COPY_OPTIONS (
  'force' = 'false',          -- skip already-loaded files
  'mergeSchema' = 'true'
);
```

### Auto Loader vs COPY INTO
| Feature | Auto Loader | COPY INTO |
|---------|-------------|----------|
| Streaming | Yes | No (batch) |
| Scale | Millions of files | Thousands of files |
| Idempotent | Yes (checkpoint) | Yes (file tracking) |
| Schema evolution | Yes | Limited |
| DLT integration | Yes | Yes |
| Best for | Continuous ingestion | Periodic batch loads |

---

## 2.6 Ingestion Patterns for Various Formats

```python
# CSV with header
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/csv_schema")
    .load("/Volumes/training/prep/landing/csv/")
)

# Avro
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "avro")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/avro_schema")
    .load("/Volumes/training/prep/landing/avro/")
)

# Parquet
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/parquet_schema")
    .load("/Volumes/training/prep/landing/parquet/")
)
```

---

## 2.7 Kafka Ingestion (Structured Streaming)

```python
# Read from Kafka (Event Hubs compatible in Azure)
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:9092")
    .option("subscribe", "orders-topic")
    .option("startingOffsets", "earliest")
    .load()
)

# Decode value
from pyspark.sql.functions import col, from_json, schema_of_json

schema = "order_id BIGINT, customer STRING, amount DOUBLE"
parsed_df = (
    kafka_df
    .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp")
    )
    .select("key", "data.*", "timestamp")
)

parsed_df.writeStream
  .format("delta")
  .option("checkpointLocation", "/Volumes/training/prep/landing/checkpoints/kafka")
  .table("training.prep.kafka_orders")
```

---

## Key Exam Points ✔️

- Auto Loader uses `cloudFiles` format — always specify `cloudFiles.format`
- `schemaLocation` is **required** for Auto Loader — stores inferred schema
- `addNewColumns` mode auto-evolves schema; `rescue` puts unknowns in `_rescued_data`
- File Notification mode is better for **scale** (millions of files)
- `COPY INTO` is **idempotent batch** ingestion; Auto Loader is **streaming**
- `_metadata.file_path` and `_metadata.file_name` are Auto Loader built-in metadata columns
- `trigger(availableNow=True)` processes all pending files then stops (like a batch run)
- `cloudFiles.inferColumnTypes=true` infers proper types; default is all strings for JSON
