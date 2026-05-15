# Databricks DE Associate — Cheatsheet
### ☁️ Azure Databricks Edition

---

## ☁️ AZURE DATABRICKS QUICK REFERENCE

### Workspace URL Format
```
https://adb-<workspace-id>.<random>.azuredatabricks.net
```

### Unity Catalog Default Namespace (this repo)
```sql
USE CATALOG training;
USE SCHEMA  prep;
-- Full 3-part name: training.prep.<table>
```

### Volume Paths (replace /tmp/ with this)
```
/Volumes/<catalog>/<schema>/<volume>/
/Volumes/training/prep/landing/          ← this repo's landing zone
/Volumes/training/prep/landing/checkpoints/<query>/   ← streaming checkpoints
```

### ADLS Gen2 Access (abfss:// — for external tables)
```
abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
```

### Azure Key Vault Secret Scope
```python
# Read secret at runtime — NEVER hardcode credentials
dbutils.secrets.get(scope="azure-kv", key="my-secret")
dbutils.secrets.list("azure-kv")  # list keys in scope
```

### Cluster Access Modes
| Mode | Python UDFs | Multi-user | Unity Catalog |
|------|------------|-----------|---------------|
| Single User | ✅ | ❌ | ✅ enforced |
| Shared | ❌ | ✅ | ✅ enforced |
| No Isolation Shared | ✅ | ✅ | ❌ (legacy) |

### Service Principal Auth (ADLS Gen2 via OAuth)
```python
spark.conf.set(f"fs.azure.account.auth.type.{storage}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage}.dfs.core.windows.net",
               dbutils.secrets.get("azure-kv", "sp-client-id"))
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage}.dfs.core.windows.net",
               dbutils.secrets.get("azure-kv", "sp-client-secret"))
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
```

### Cost Controls (Azure-specific)
- Always set **Auto Termination**: 30 min for learning clusters
- Use **Single Node** (`Standard_DS3_v2`) for all practice tasks
- Job clusters auto-terminate → always cheaper than all-purpose for pipelines
- Stop cluster manually after each study session

---

## UNITY CATALOG

### Namespace Hierarchy
```
Metastore  (one per Azure region, managed by account admin)
  └─ Catalog  (training)
       └─ Schema / Database  (prep)
            ├─ Tables  (managed + external)
            ├─ Views
            ├─ Volumes  (/Volumes/training/prep/landing/)
            └─ Functions
```

### DDL Essentials
```sql
CREATE CATALOG IF NOT EXISTS training;
CREATE SCHEMA  IF NOT EXISTS training.prep;
CREATE VOLUME  IF NOT EXISTS training.prep.landing;

-- Managed table (UC controls storage, inside metastore ADLS)
CREATE TABLE training.prep.orders (
  id BIGINT, amount DOUBLE, region STRING
) USING DELTA;

-- External table (you control the ADLS path)
CREATE TABLE training.prep.orders_ext
USING DELTA
LOCATION 'abfss://container@storage.dfs.core.windows.net/orders/';
```

### Privileges
```sql
GRANT USE CATALOG ON CATALOG training TO `analyst@company.com`;
GRANT USE SCHEMA  ON SCHEMA training.prep TO `analyst@company.com`;
GRANT SELECT     ON TABLE training.prep.orders TO `analyst@company.com`;
GRANT MODIFY     ON TABLE training.prep.orders TO `etl_service_principal`;

REVOKE SELECT ON TABLE training.prep.orders FROM `analyst@company.com`;
SHOW GRANTS ON TABLE training.prep.orders;
```

### Row & Column Security
```sql
-- Row filter (only show own region)
CREATE FUNCTION training.prep.region_filter(region STRING)
  RETURN region = current_user() OR is_account_group_member('admin');

ALTER TABLE training.prep.orders
  SET ROW FILTER training.prep.region_filter ON (region);

-- Column mask (hide PII)
CREATE FUNCTION training.prep.mask_email(email STRING)
  RETURN CASE WHEN is_account_group_member('admin') THEN email ELSE '***MASKED***' END;

ALTER TABLE training.prep.orders
  ALTER COLUMN customer_email SET MASK training.prep.mask_email;
```

---

## DELTA LAKE

### Core Operations
```sql
-- MERGE (upsert)
MERGE INTO target AS t
USING source AS s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET t.amount = s.amount
WHEN NOT MATCHED THEN INSERT *;

-- Time travel
SELECT * FROM training.prep.orders VERSION AS OF 3;
SELECT * FROM training.prep.orders TIMESTAMP AS OF '2024-01-15';
RESTORE TABLE training.prep.orders TO VERSION AS OF 2;

-- Maintenance
OPTIMIZE training.prep.orders;                        -- compact files
OPTIMIZE training.prep.orders ZORDER BY (order_date); -- + data skipping
VACUUM  training.prep.orders RETAIN 168 HOURS;        -- 7 days (default)

-- History
DESCRIBE HISTORY training.prep.orders;
DESCRIBE DETAIL  training.prep.orders;
```

### Change Data Feed
```sql
ALTER TABLE training.prep.orders
  SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT * FROM table_changes('training.prep.orders', 1)
ORDER BY _commit_version;
-- _change_type: insert | update_preimage | update_postimage | delete
```

### Liquid Clustering (preferred over ZORDER for new tables)
```sql
CREATE TABLE training.prep.orders_clustered
CLUSTER BY (order_date, region)
AS SELECT * FROM training.prep.orders;

OPTIMIZE training.prep.orders_clustered; -- incremental, no full rewrite
```

### Schema Evolution
```python
# Merge schema on write
df.write.option("mergeSchema", "true").mode("append").saveAsTable("training.prep.orders")

# Auto-evolve schema for streaming
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

---

## STRUCTURED STREAMING

### Read → Write Pattern
```python
# Delta table source → Delta table sink (most common exam pattern)
chk = "/Volumes/training/prep/landing/checkpoints/my_query"

query = (
    spark.readStream
    .format("delta")
    .table("training.prep.source_table")
    .writeStream
    .format("delta")
    .option("checkpointLocation", chk)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("training.prep.sink_table")
)
query.awaitTermination()
```

### Trigger Types
| Trigger | Code | Behavior |
|---------|------|----------|
| Available Now | `trigger(availableNow=True)` | Process all available, stop. Replaces `once`. |
| Processing Time | `trigger(processingTime="30 seconds")` | New micro-batch every 30s |
| Once (deprecated) | `trigger(once=True)` | Deprecated — use `availableNow` |
| Continuous | `trigger(continuous="1 second")` | Sub-ms latency, limited ops |

### Watermark + Window Aggregation
```python
from pyspark.sql.functions import window, col

(
  spark.readStream.format("delta").table("training.prep.events")
  .withWatermark("event_time", "10 minutes")    # tolerate 10 min late data
  .groupBy(window(col("event_time"), "5 minutes"), col("region"))
  .agg({"amount": "sum"})
  .writeStream
  .outputMode("append")                          # required with watermark
  .option("checkpointLocation", chk)
  .trigger(availableNow=True)
  .toTable("training.prep.windowed_agg")
).awaitTermination()
```

### Auto Loader (cloudFiles)
```python
# Best practice for incremental file ingestion from ADLS/Volumes
chk = "/Volumes/training/prep/landing/checkpoints/autoloader"

(
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")             # csv | parquet | avro | json
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaLocation", chk + "/schema")
  .load("/Volumes/training/prep/landing/raw/")
  .writeStream
  .format("delta")
  .option("checkpointLocation", chk + "/data")
  .trigger(availableNow=True)
  .toTable("training.prep.bronze_events")
).awaitTermination()
```

---

## DELTA LIVE TABLES (DLT)

### Python Syntax
```python
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Bronze: raw ingestion")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/training/prep/landing/dlt_schema/")
        .load("/Volumes/training/prep/landing/dlt_source/")
    )

@dlt.table(comment="Silver: validated orders")
@dlt.expect_or_drop("valid_id",     "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_amt", "amount > 0")
@dlt.expect(        "recent_date",  "order_date >= '2020-01-01'")  # warn only
def silver_orders():
    return dlt.read_stream("bronze_orders").withColumn("order_date", col("order_date").cast("date"))

@dlt.table(comment="Gold: daily revenue")
def gold_daily_revenue():
    return dlt.read("silver_orders").groupBy("order_date", "region").agg({"amount": "sum"})
```

### SQL Syntax
```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders AS
SELECT * FROM cloud_files('/Volumes/training/prep/landing/dlt_source/', 'json');

CREATE OR REFRESH MATERIALIZED VIEW silver_orders
  CONSTRAINT valid_id  EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
  CONSTRAINT pos_amt   EXPECT (amount > 0)           ON VIOLATION DROP ROW
AS SELECT *, CAST(order_date AS DATE) AS order_date FROM LIVE.bronze_orders;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue
AS SELECT order_date, region, SUM(amount) total FROM LIVE.silver_orders GROUP BY 1,2;
```

### DLT Expectation Actions
| Decorator | SQL clause | On violation |
|-----------|-----------|-------------|
| `@dlt.expect()` | `EXPECT ... ` | Log warning, keep row |
| `@dlt.expect_or_drop()` | `EXPECT ... ON VIOLATION DROP ROW` | Drop bad row |
| `@dlt.expect_or_fail()` | `EXPECT ... ON VIOLATION FAIL UPDATE` | Stop pipeline |

---

## APACHE SPARK ESSENTIALS

### Transformations vs Actions
```python
# Transformations (lazy — build the plan, do not execute)
df.select(), df.filter(), df.groupBy(), df.join(), df.withColumn(), df.drop()

# Actions (eager — trigger execution)
df.count(), df.show(), df.collect(), df.write.save(), df.first(), df.take(n)
```

### DataFrame Essentials
```python
# Read
df = spark.read.format("delta").table("training.prep.orders")
df = spark.read.option("header", True).csv("/Volumes/training/prep/landing/file.csv")

# Write
df.write.mode("overwrite").saveAsTable("training.prep.orders")
df.write.mode("append").partitionBy("region").saveAsTable("training.prep.orders")

# Common transforms
from pyspark.sql.functions import col, lit, when, coalesce, count, sum, avg, to_date
df.filter(col("amount") > 100)
df.withColumn("vat", col("amount") * 0.2)
df.select("id", "amount", col("region").alias("area"))
df.groupBy("region").agg(count("*").alias("n"), sum("amount").alias("total"))
```

### Window Functions
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, row_number, lag, lead, sum as wsum

w = Window.partitionBy("region").orderBy(col("amount").desc())
df.withColumn("rank",       rank().over(w))
  .withColumn("dense_rank", dense_rank().over(w))
  .withColumn("row_num",    row_number().over(w))

# Rolling sum
w_rolling = Window.partitionBy("region").orderBy("order_date").rowsBetween(-6, 0)
df.withColumn("7day_sum", wsum("amount").over(w_rolling))
```

### Higher-Order Functions
```sql
-- TRANSFORM: apply function to each array element
SELECT TRANSFORM(amounts, x -> x * 1.2) AS inflated FROM orders;

-- FILTER: keep elements matching predicate
SELECT FILTER(tags, t -> t != 'test') AS clean_tags FROM events;

-- EXISTS: true if any element matches
SELECT EXISTS(scores, s -> s > 90) AS has_high_score FROM students;
```

---

## SQL PATTERNS FOR THE EXAM

```sql
-- CTEs
WITH daily AS (
  SELECT order_date, SUM(amount) AS total FROM training.prep.orders GROUP BY 1
)
SELECT * FROM daily WHERE total > 1000;

-- PIVOT
SELECT * FROM (
  SELECT region, product, amount FROM training.prep.orders
) PIVOT (SUM(amount) FOR region IN ('EU', 'US', 'APAC'));

-- LATERAL VIEW EXPLODE
SELECT id, tag FROM training.prep.orders
LATERAL VIEW EXPLODE(tags) t AS tag;

-- COLLECT_LIST / COLLECT_SET
SELECT customer_id, COLLECT_LIST(product) AS products FROM training.prep.orders
GROUP BY customer_id;
```

---

## EXAM TRAPS & COMMON MISTAKES

| ❌ Wrong | ✅ Correct |
|---------|----------|
| Use `/tmp/` for checkpoints | Use `/Volumes/<cat>/<schema>/<vol>/checkpoints/` |
| Share checkpointLocation across queries | Every query needs its own unique path |
| `trigger(once=True)` | `trigger(availableNow=True)` (deprecated in DBR 10.1+) |
| `outputMode("complete")` without aggregation | `complete` requires aggregation |
| VACUUM with 0 HOURS | Minimum 7 days (168h) unless explicitly overridden |
| ZORDER on a Liquid Clustered table | Use `OPTIMIZE` without ZORDER for LC tables |
| Hardcode credentials in notebooks | Always use `dbutils.secrets.get()` |
| External table in UC managed storage | External tables need explicit `LOCATION` outside metastore |
| `@dlt.expect()` drops rows | `expect()` only warns; use `expect_or_drop()` to drop |
