# Databricks Data Engineer Professional - Cheatsheet

> Quick reference for the Databricks Certified Data Engineer Professional exam

---

## Delta Lake

| Command | Description |
|---------|-------------|
| `DESCRIBE HISTORY t` | Show all versions |
| `RESTORE TABLE t TO VERSION AS OF N` | Restore to version N |
| `RESTORE TABLE t TO TIMESTAMP AS OF 'date'` | Restore to timestamp |
| `OPTIMIZE t ZORDER BY (col)` | Compact + Z-order colocation |
| `VACUUM t RETAIN N HOURS` | Remove old files (default 168h) |
| `ALTER TABLE t SET TBLPROPERTIES (...)` | Set table properties |

```sql
-- Change Data Feed
ALTER TABLE orders SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
SELECT * FROM table_changes('orders', 1, 5);  -- version range

-- Liquid Clustering (replaces partitioning)
CREATE TABLE t (...) CLUSTER BY (customer_id, order_date);
ALTER TABLE t CLUSTER BY (customer_id);
OPTIMIZE t;  -- apply clustering

-- Shallow Clone (shares files) vs Deep Clone (full copy)
CREATE TABLE clone SHALLOW CLONE source;
CREATE TABLE clone DEEP CLONE source;
```

---

## Streaming

```python
# Triggers
.trigger(processingTime="1 minute")  # micro-batch
.trigger(availableNow=True)           # batch (replaces once=True)
.trigger(continuous="1 second")       # continuous (limited ops)

# Output modes
# append: new rows only (non-agg, or windowed+watermark)
# complete: full result (aggregation without watermark)
# update: changed rows (aggregation)

# Watermark
.withWatermark("event_time", "10 minutes")

# foreachBatch: custom sink
def process(batch_df, batch_id):
    batch_df.write.format("delta").mode("append").saveAsTable("t")
df.writeStream.foreachBatch(process).start()

# Auto Loader
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/schema/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # default
    .load("/path/")
```

---

## DLT / Lakeflow Pipelines

```python
import dlt

# Expectations
@dlt.expect("name", "condition")           # warn, keep rows
@dlt.expect_or_drop("name", "condition")   # drop violating rows
@dlt.expect_or_fail("name", "condition")   # fail pipeline
@dlt.expect_all({"n1": "c1", "n2": "c2"})  # multiple

# CDC
dlt.create_streaming_table("target")
dlt.apply_changes(
    target="target",
    source="source_view",
    keys=["id"],
    sequence_by=col("ts"),
    apply_as_deletes=expr("op = 'DELETE'")
)

# Read in DLT
dlt.read("table")         # batch
dlt.read_stream("table")  # streaming
```

| Operation | Action |
|-----------|--------|
| expect | warn, keep |
| expect_or_drop | remove row |
| expect_or_fail | stop pipeline |

---

## Unity Catalog

```sql
-- Hierarchy: Metastore > Catalog > Schema > Table

-- Grants (cascade down)
GRANT USE CATALOG ON CATALOG c TO `group`;
GRANT USE SCHEMA ON SCHEMA c.s TO `group`;
GRANT SELECT ON TABLE c.s.t TO `group`;
GRANT MODIFY ON TABLE c.s.t TO `group`;
SHOW GRANTS ON TABLE c.s.t;

-- Column Masking
CREATE FUNCTION mask_fn(v STRING) RETURNS STRING
  RETURN IF(is_member('admin'), v, '***');
ALTER TABLE t ALTER COLUMN col SET MASK mask_fn;

-- Row Filters
CREATE FUNCTION filter_fn(region STRING) RETURNS BOOLEAN
  RETURN is_member('global') OR region = current_user();
ALTER TABLE t SET ROW FILTER filter_fn ON (region);

-- External Locations
CREATE STORAGE CREDENTIAL cred WITH AZURE_MANAGED_IDENTITY = (...);
CREATE EXTERNAL LOCATION loc URL 'abfss://...' WITH (STORAGE CREDENTIAL cred);

-- Audit
SELECT * FROM system.access.audit WHERE action_name = 'SELECT';
```

---

## DABs (Databricks Asset Bundles)

```yaml
# databricks.yml
bundle:
  name: project_name
variables:
  env: {default: dev}
targets:
  dev:
    mode: development  # username-prefix, no auto-retry
    default: true
  prod:
    mode: production   # strict, no personal clusters
resources:
  jobs:
    my_job:
      tasks:
        - task_key: step1
          notebook_task: {notebook_path: ./nb}
```

```bash
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run my_job --target prod
databricks bundle destroy --target dev
```

---

## Performance

| Topic | Key Facts |
|-------|----------|
| AQE | Auto join strategies, skew handling, partition coalescing |
| Broadcast join | Default 10MB threshold; `broadcast()` hint forces it |
| repartition() | Full shuffle, even distribution |
| coalesce() | No shuffle, reduces partitions only |
| Photon | C++ vectorized, cluster-level, auto SQL/Delta |
| Delta Cache | SSD, raw files | Spark Cache: executor RAM, DataFrames |
| Shuffle partitions | Default 200; tune via AQE or `spark.sql.shuffle.partitions` |
| EXPLAIN modes | logical, extended, codegen, cost, formatted |

---

## Security

```python
# Secrets
dbutils.secrets.get(scope="my-scope", key="password")  # returns [REDACTED]

# Cluster modes
# Single User: isolated, DBFS mount allowed
# Shared: UC enforced, no root, no init scripts
```

```bash
# Secret scopes
databricks secrets create-scope my-scope
databricks secrets put-secret my-scope key --string-value val

# Service principals (for CI/CD)
databricks service-principals create --application-id <id>
```

---

## CLI Quick Reference

```bash
databricks clusters list
databricks jobs run-now --job-id 123
databricks runs get --run-id 456
databricks pipelines start --pipeline-id abc
databricks fs ls dbfs:/
databricks secrets list-scopes
```

---

## Common Exam Traps

- `once` trigger is **deprecated** - use `availableNow=True`
- `APPLY CHANGES INTO` needs `dlt.create_streaming_table()` not `@dlt.table`
- Checkpoint = never reuse across different queries
- `mode: development` = username prefix, no auto-retry
- Unity Catalog: one metastore per region
- VACUUM default = 168 hours (7 days) - cannot go below retention config
- `coalesce()` can only reduce (not increase) partitions
- `broadcast()` hint **ignores** size threshold
- Secrets always show as **[REDACTED]** in output
- DLT views: **not stored**; DLT tables: **Delta files**
