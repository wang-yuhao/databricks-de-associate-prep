# Day 12: Lakeflow Declarative Pipelines (DLT) - Advanced

## Schedule
- Morning: DLT expectations, quality enforcement, CDC
- Afternoon: Pipeline monitoring, event log, advanced patterns
- Evening: DLT with Unity Catalog and practice

---

## 12.1 DLT Expectations & Data Quality

```python
import dlt
from pyspark.sql.functions import col

# Single expectation - warn on violation
@dlt.table(name="silver_orders")
@dlt.expect("valid_amount", "amount > 0")
def silver_orders():
    return dlt.read("bronze_orders").filter(col("amount").isNotNull())

# Drop rows that violate expectation
@dlt.table(name="silver_orders_clean")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_orders_clean():
    return dlt.read("bronze_orders")

# Fail pipeline on violation
@dlt.table(name="silver_critical")
@dlt.expect_or_fail("non_null_id", "order_id IS NOT NULL")
def silver_critical():
    return dlt.read("bronze_orders")

# Multiple expectations
@dlt.table(name="silver_validated")
@dlt.expect_all({
    "valid_amount": "amount > 0",
    "valid_status": "status IN ('pending', 'shipped', 'delivered')",
    "non_null_customer": "customer_id IS NOT NULL"
})
def silver_validated():
    return dlt.read("bronze_orders")

# expect_all_or_drop, expect_all_or_fail also available
```

---

## 12.2 CDC with APPLY CHANGES INTO

```python
# Source CDC stream
@dlt.view
def cdc_source():
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/schema/cdc") \
        .load("/data/cdc/")

# Apply CDC changes to target table
dlt.create_streaming_table("customers_silver")

dlt.apply_changes(
    target="customers_silver",
    source="cdc_source",
    keys=["customer_id"],
    sequence_by=col("update_timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    apply_as_truncates=expr("operation = 'TRUNCATE'"),
    column_list=["customer_id", "name", "email", "update_timestamp"],
    except_column_list=["operation", "_metadata"]
)
```

---

## 12.3 DLT Table Types

| Type | Description | Supports Streaming |
|------|-------------|--------------------|
| `@dlt.table` | Materialized view or streaming table | Yes |
| `@dlt.view` | Temporary view (not stored) | Yes |
| `dlt.create_streaming_table` | Explicit streaming table for APPLY CHANGES | Yes |

```python
# Streaming table - processes new data incrementally
@dlt.table(
    name="bronze_events",
    comment="Raw events from Auto Loader",
    table_properties={
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    partition_cols=["event_date"]
)
def bronze_events():
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaLocation", "/schema/events") \
        .load("/data/raw/events/")

# Materialized view - recomputed on each pipeline run
@dlt.table(name="gold_daily_revenue")
def gold_daily_revenue():
    return dlt.read("silver_orders") \
        .groupBy("order_date") \
        .agg({"amount": "sum"})
```

---

## 12.4 DLT Event Log & Monitoring

```python
# Query DLT event log (stored in pipeline storage location)
event_log_path = "/pipelines/<pipeline_id>/system/events"

events_df = spark.read.format("delta").load(event_log_path)

# Pipeline metrics
from pyspark.sql.functions import from_json, col

metrics = events_df \
    .filter("event_type = 'flow_progress'") \
    .select(
        col("timestamp"),
        col("origin.flow_name").alias("table"),
        col("details:flow_progress:metrics:num_output_rows").cast("long").alias("rows_written"),
        col("details:flow_progress:data_quality:dropped_records").cast("long").alias("dropped")
    )

# Expectation violations
violations = events_df \
    .filter("event_type = 'flow_progress'") \
    .selectExpr(
        "timestamp",
        "origin.flow_name as table",
        "explode(from_json(details:flow_progress:data_quality:expectations, \'array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>\')) as exp"
    ) \
    .select("timestamp", "table", "exp.*")
```

---

## 12.5 DLT with Unity Catalog

```python
# Pipeline settings for Unity Catalog mode
# In pipeline config JSON:
# {
#   "catalog": "my_catalog",
#   "target": "silver",
#   "channel": "CURRENT"
# }

# Tables automatically registered in Unity Catalog
@dlt.table(
    name="orders_silver",
    comment="Cleaned orders - Unity Catalog managed"
)
def orders_silver():
    return dlt.read_stream("orders_bronze") \
        .filter(col("amount") > 0)

# Reference tables across catalogs
@dlt.table
def enriched_orders():
    orders = dlt.read("orders_silver")
    customers = spark.table("prod_catalog.gold.customers")  # external table
    return orders.join(customers, "customer_id")
```

---

## 12.6 Pipeline Execution Modes

| Mode | Description |
|------|-------------|
| Triggered | Runs once, processes available data |
| Continuous | Always running, low-latency |
| Development | Reuses cluster, no auto-retry, faster iteration |
| Production | Auto-scaling, retry on failure |

```python
# Force full refresh (reprocess all data)
# databricks pipeline start --pipeline-id <id> --full-refresh

# Selective table refresh
# databricks pipeline start --pipeline-id <id> --full-refresh-selection orders_silver
```

---

## Key Exam Points

- `@dlt.expect`: warn (keep rows) | `@dlt.expect_or_drop`: remove rows | `@dlt.expect_or_fail`: stop pipeline
- `dlt.apply_changes()` implements CDC (SCD Type 1 by default)
- `APPLY CHANGES INTO` requires `dlt.create_streaming_table()` target
- DLT event log path: `/pipelines/<id>/system/events`
- Unity Catalog mode: set `catalog` and `target` in pipeline config
- `dlt.read()`: batch read from DLT table | `dlt.read_stream()`: streaming read
- Views in DLT are not stored; tables are materialized in Delta format
- Development mode: faster iteration, no retries

---

## Practice Tasks
- [ ] Create DLT pipeline with bronze/silver/gold layers
- [ ] Add expectations: warn on one table, drop on another, fail on critical
- [ ] Implement CDC with apply_changes for customer table
- [ ] Query DLT event log for expectation violations
- [ ] Configure pipeline for Unity Catalog
