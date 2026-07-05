# Day 3 — Lakeflow Declarative Pipelines (Advanced DLT) (~10% of exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day3_lakeflow_pipelines.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review DLT/Lakeflow questions

---

## 3.1 Lakeflow Declarative Pipelines Overview

**Lakeflow Declarative Pipelines** (formerly Delta Live Tables / DLT) is a framework for building reliable, maintainable, and testable data pipelines.

### Key Concepts
- Define data transformations as **SQL or Python** datasets
- Databricks manages orchestration, dependencies, retries, and scaling
- Built-in **data quality** via expectations
- Supports **streaming** and **batch** in the same pipeline

### Dataset Types
| Type | Description | Supports Streaming? |
|------|-------------|--------------------|
| `STREAMING TABLE` | Append-only, processes new data incrementally | Yes |
| `MATERIALIZED VIEW` | Fully refreshed on each pipeline run | Yes (refresh) |
| `VIEW` | Temporary, not stored; used for intermediate transforms | No |

---

## 3.2 Streaming Tables vs Materialized Views

```sql
-- STREAMING TABLE: for append-only sources (Auto Loader, Kafka, CDC)
CREATE OR REFRESH STREAMING TABLE raw_orders
COMMENT 'Raw orders from landing zone'
AS SELECT * FROM STREAM read_files(
  '/Volumes/training/prep/landing/orders/',
  format => 'json',
  schema => 'order_id BIGINT, customer STRING, amount DOUBLE, order_date DATE'
);

-- MATERIALIZED VIEW: for aggregations / lookups (recomputed each run)
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
COMMENT 'Revenue aggregated by day'
AS
SELECT
  order_date,
  SUM(amount) AS total_revenue,
  COUNT(*) AS order_count
FROM raw_orders
GROUP BY order_date;
```

### When to Use Which
| Scenario | Use |
|----------|-----|
| Append-only incremental ingestion | STREAMING TABLE |
| CDC / upserts from source | STREAMING TABLE + APPLY CHANGES |
| Aggregations / metrics | MATERIALIZED VIEW |
| Complex joins across multiple tables | MATERIALIZED VIEW |
| Temp intermediate logic | VIEW |

---

## 3.3 Data Quality: Expectations

```sql
-- WARN: log violation but keep the row
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION WARN,
  CONSTRAINT non_null_customer EXPECT (customer IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT * FROM STREAM(raw_orders);

-- DROP ROW: remove bad rows from output
-- FAIL UPDATE: fail the pipeline on any violation
CREATE OR REFRESH STREAMING TABLE critical_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM(raw_orders);
```

### Expectation Outcomes
| ON VIOLATION | Behaviour | Good For |
|-------------|-----------|----------|
| `WARN` | Log violation count, keep rows | Monitoring, soft alerts |
| `DROP ROW` | Silently drop bad rows | Quarantine pattern |
| `FAIL UPDATE` | Fail entire pipeline update | Critical data validation |

---

## 3.4 Pipeline Modes

```python
# In pipeline settings (JSON config)
{
  "name": "orders_pipeline",
  "continuous": false,    # false = triggered mode
  "development": true,    # development mode: no retries, verbose logging
  "clusters": [{...}],
  "libraries": [{...}]
}
```

| Mode | Behaviour |
|------|-----------|
| **Triggered** | Run once, process new data, then stop |
| **Continuous** | Runs continuously (low-latency streaming) |
| **Development** | No retries on failure; live verbose logging |
| **Production** | Retries on failure; runs as scheduled |

---

## 3.5 Python API for Lakeflow Pipelines

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="raw_orders",
    comment="Raw orders from Auto Loader",
    table_properties={"quality": "bronze"}
)
def raw_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/checkpoints/schema")
        .load("/Volumes/training/prep/landing/orders/")
    )

@dlt.expect("valid_amount", "amount > 0")
@dlt.expect_or_drop("non_null_customer", "customer IS NOT NULL")
@dlt.table(
    name="silver_orders",
    comment="Validated orders"
)
def silver_orders():
    return dlt.read_stream("raw_orders").select(
        col("order_id"),
        col("customer"),
        col("amount"),
        col("order_date"),
        current_timestamp().alias("processed_at")
    )

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregation"
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")  # read() not read_stream() for MV
        .groupBy("order_date")
        .agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("order_count")
        )
    )
```

### Python Expectation Decorators
| Decorator | Behaviour |
|-----------|-----------|
| `@dlt.expect(name, condition)` | WARN — log but keep |
| `@dlt.expect_or_drop(name, condition)` | DROP ROW |
| `@dlt.expect_or_fail(name, condition)` | FAIL UPDATE |
| `@dlt.expect_all(rules_dict)` | Apply multiple WARN expectations |
| `@dlt.expect_all_or_drop(rules_dict)` | Apply multiple DROP ROW expectations |

---

## 3.6 Control Flow in Pipelines

```python
import dlt
from pyspark.sql import functions as F

# Conditional table creation based on config
if spark.conf.get("pipeline.env", "dev") == "prod":
    @dlt.table(name="prod_audit_log")
    def audit_log():
        return dlt.read("silver_orders").select("*", F.current_timestamp().alias("audit_ts"))

# ForEach pattern: create multiple tables from a list
for region in ["EU", "US", "APAC"]:
    @dlt.table(name=f"silver_orders_{region.lower()}")
    def orders_by_region(r=region):  # capture region in closure
        return dlt.read_stream("raw_orders").filter(F.col("region") == r)
```

---

## 3.7 Pipeline Metrics & Monitoring

```python
# Access event log programmatically
event_log = spark.read.format("delta").load("<pipeline-storage>/system/events")

# Expectation metrics
expectations = (
    event_log
    .filter("event_type = 'flow_progress'")
    .selectExpr(
        "details:flow_progress:metrics:num_output_rows AS output_rows",
        "details:flow_progress:data_quality:dropped_records AS dropped_records",
        "details:flow_progress:data_quality:expectations AS expectations"
    )
)

# SQL query on event log
event_log.createOrReplaceTempView("pipeline_events")
spark.sql("""
    SELECT
        timestamp,
        details:flow_progress:metrics:num_output_rows AS output_rows
    FROM pipeline_events
    WHERE event_type = 'flow_progress'
    ORDER BY timestamp DESC
""")
```

---

## Key Exam Points ✔️

- **Streaming Tables** process new data incrementally; **Materialized Views** fully refresh each run
- Expectations: `WARN` keeps rows; `DROP ROW` removes them; `FAIL UPDATE` stops the pipeline
- Use `dlt.read_stream()` for streaming inputs, `dlt.read()` for batch/MV inputs
- **Triggered mode** = run once and stop; **Continuous mode** = always running
- Development mode: no automatic retries, verbose logging
- `@dlt.expect_all(rules_dict)` applies multiple WARN expectations at once
- The pipeline event log is stored in `<storage>/system/events` as a Delta table
- Lakeflow Pipelines (DLT) manages dependencies automatically — no explicit ordering needed
