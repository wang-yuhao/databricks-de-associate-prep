# Day 4 — CDC with APPLY CHANGES INTO (~10% of exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day4_cdc_apply_changes.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review CDC pattern questions

---

## 4.1 What is CDC?

**Change Data Capture (CDC)** captures row-level changes (INSERT, UPDATE, DELETE) from source systems.

### CDC Sources
- Database transaction logs (Debezium → Kafka)
- ADLS/S3 landing zone (JSON/Avro files with operation column)
- Kafka topics

### Typical CDC Record
```json
{
  "order_id": 123,
  "customer": "Alice",
  "amount": 99.99,
  "_change_type": "UPDATE",   // INSERT, UPDATE, DELETE
  "_commit_version": 5,
  "_event_timestamp": "2024-01-15T10:30:00Z"
}
```

---

## 4.2 APPLY CHANGES INTO (DLT/Lakeflow)

`APPLY CHANGES INTO` is the Lakeflow Pipelines API for CDC. It automatically handles SCD Type 1 (overwrite) and SCD Type 2 (history).

### Basic Syntax (SQL)
```sql
-- Step 1: Define the bronze streaming table (raw CDC events)
CREATE OR REFRESH STREAMING TABLE orders_cdc_raw
AS SELECT * FROM STREAM read_files(
  '/Volumes/training/prep/landing/cdc/',
  format => 'json'
);

-- Step 2: Apply CDC changes to create a silver table
APPLY CHANGES INTO LIVE.orders_silver
FROM STREAM(LIVE.orders_cdc_raw)
KEYS (order_id)                          -- primary key
APPLY AS DELETE WHEN operation = 'DELETE'
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'
SEQUENCE BY event_timestamp              -- ordering column
COLUMNS * EXCEPT (_rescued_data, operation)  -- exclude metadata cols
STORED AS SCD TYPE 1;                    -- overwrite (latest wins)
```

### SCD Type 2
```sql
-- SCD Type 2: keep full history
APPLY CHANGES INTO LIVE.customers_silver
FROM STREAM(LIVE.customers_cdc_raw)
KEYS (customer_id)
SEQUENCE BY event_timestamp
COLUMNS * EXCEPT (operation, _rescued_data)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (non_tracked_col);  -- track all changes
```

---

## 4.3 APPLY CHANGES Python API

```python
import dlt

@dlt.table(name="orders_cdc_raw", comment="Raw CDC events")
def orders_cdc_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/cdc_schema")
        .load("/Volumes/training/prep/landing/cdc/")
    )

# APPLY CHANGES using Python API
dlt.create_streaming_table("orders_silver")

dlt.apply_changes(
    target="orders_silver",
    source="orders_cdc_raw",
    keys=["order_id"],
    sequence_by="event_timestamp",
    apply_as_deletes="operation = 'DELETE'",
    apply_as_truncates="operation = 'TRUNCATE'",
    except_column_list=["operation", "_rescued_data"],
    stored_as_scd_type=1   # or 2 for history
)
```

---

## 4.4 SCD Type 1 vs Type 2

| Feature | SCD Type 1 | SCD Type 2 |
|---------|------------|------------|
| History | No (overwrite) | Yes (new row per change) |
| Storage | Lower | Higher |
| Complexity | Simple | Needs `__START_AT`, `__END_AT` cols |
| Use case | Current state only | Audit trail, point-in-time queries |

### SCD Type 2 Auto-Generated Columns
When using SCD Type 2, Databricks auto-generates:
- `__START_AT` — sequence value when row became active
- `__END_AT` — sequence value when row was superseded (NULL = current)

```sql
-- Query SCD Type 2 for current records
SELECT * FROM training.prep.customers_silver
WHERE __END_AT IS NULL;  -- current active records

-- Query point-in-time
SELECT * FROM training.prep.customers_silver
WHERE __START_AT <= '2024-01-15' AND (__END_AT > '2024-01-15' OR __END_AT IS NULL);
```

---

## 4.5 CDC with Delta Lake MERGE (without Lakeflow)

For non-pipeline CDC, use `MERGE INTO`:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Read CDC batch
cdc_df = spark.read.json("/Volumes/training/prep/landing/cdc/batch_001.json")

dt = DeltaTable.forName(spark, "training.prep.orders_silver")

(
    dt.alias("target")
    .merge(
        cdc_df.alias("source"),
        "target.order_id = source.order_id"
    )
    .whenMatchedDelete(condition="source.operation = 'DELETE'")
    .whenMatchedUpdateAll(condition="source.operation = 'UPDATE'")
    .whenNotMatchedInsertAll(condition="source.operation = 'INSERT'")
    .execute()
)
```

---

## 4.6 Change Data Feed (CDF)

CDF (Delta Change Data Feed) captures changes made TO a Delta table (not FROM a source):

```sql
-- Enable CDF on a table
ALTER TABLE training.prep.orders
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Read CDF changes
SELECT * FROM table_changes('training.prep.orders', 1, 5)
-- Returns rows with _change_type: insert, update_preimage, update_postimage, delete

-- Stream CDF changes
orders_changes = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("training.prep.orders")
)
```

### CDF Change Types
| `_change_type` | Meaning |
|---------------|---------|
| `insert` | New row inserted |
| `update_preimage` | Row value before update |
| `update_postimage` | Row value after update |
| `delete` | Row deleted |

---

## Key Exam Points ✔️

- `APPLY CHANGES INTO` = Lakeflow Pipelines CDC API; handles INSERT/UPDATE/DELETE automatically
- `KEYS` = primary key columns; `SEQUENCE BY` = ordering (higher = more recent)
- **SCD Type 1** = overwrite (latest wins); **SCD Type 2** = keeps history with `__START_AT`/`__END_AT`
- `APPLY AS DELETE WHEN` and `APPLY AS TRUNCATE WHEN` define which records are deletions
- CDF (`readChangeFeed`) reads changes FROM a Delta table (outbound); APPLY CHANGES handles inbound CDC
- CDF provides: `insert`, `update_preimage`, `update_postimage`, `delete` change types
- Enable CDF: `'delta.enableChangeDataFeed' = 'true'` in table properties
- APPLY CHANGES target table is automatically a **Streaming Table** (cannot be a MV)
