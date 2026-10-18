# Databricks notebook source
# MAGIC %md
# MAGIC # 1.2 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads (Databricks)
# MAGIC Companion notebook for the `multiplex` pipeline. Guide: `FREE_EDITION_GUIDE.md` → Course 1 → Lesson 1.2.
# MAGIC
# MAGIC | Pipeline setting | Value |
# MAGIC |---|---|
# MAGIC | Source code | `01-advanced-sdp/pipelines/multiplex/transformations` |
# MAGIC | Default catalog / schema | `workspace` / `sdp_1_2` |
# MAGIC | Configuration | `multiplex_source` = `/Volumes/workspace/sdp_course/landing/multiplex` |
# MAGIC | | `clickstream_sink_path` = `/Volumes/workspace/sdp_course/landing/exports/clickstream` |
# MAGIC | | `enable_uniform` = `false` (Databricks doesn't allow Iceberg reads on pipeline tables, see step 4) |
# MAGIC | Event log | Publish to `workspace.sdp_1_2.event_log` |

# COMMAND ----------

# MAGIC %pip install -q "pyiceberg[pyarrow]"

# COMMAND ----------

# MAGIC %run ./_event_log_helpers

# COMMAND ----------

dbutils.widgets.text("schema", "workspace.sdp_1_2")
dbutils.widgets.text("event_log", "workspace.sdp_1_2.event_log")
# locate() finds the pipeline's schema if it differs from the widget, then runs USE <schema>
SCHEMA, EVENT_LOG = locate(dbutils.widgets.get("schema"), "bronze_multiplex", dbutils.widgets.get("event_log"))
SINK_PATH = "/Volumes/workspace/sdp_course/landing/exports/clickstream"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - The raw multiplexed feed
# MAGIC One stream, three topics, each with a **different payload schema** inside the `value` string (like Kafka).
# MAGIC About 3% of payloads are corrupt JSON.

# COMMAND ----------

raw = spark.read.json("/Volumes/workspace/sdp_course/landing/multiplex")
display(raw.groupBy("topic").count())
display(raw.select("topic", "key", "value").limit(6))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - After the first update: fan-out results
# MAGIC In the graph: `bronze_multiplex` → `silver_orders`, `silver_customers`, `silver_clickstream` (generated in a
# MAGIC Python `for` loop through a function, so each table binds its own `topic`) + `bronze_multiplex_quarantine`.

# COMMAND ----------

display(spark.sql("""
SELECT 'bronze' AS t, count(*) AS rows FROM bronze_multiplex
UNION ALL SELECT 'silver_orders', count(*) FROM silver_orders
UNION ALL SELECT 'silver_customers', count(*) FROM silver_customers
UNION ALL SELECT 'silver_clickstream', count(*) FROM silver_clickstream
UNION ALL SELECT 'quarantine', count(*) FROM bronze_multiplex_quarantine"""))
display(spark.sql("DESCRIBE DETAIL bronze_multiplex").select("name", "clusteringColumns"))
display(expectation_metrics(EVENT_LOG))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK:** bronze = orders + customers + clickstream + quarantine. This holds exactly because the quarantine
# MAGIC predicate is the complement of the silver one: a record is quarantined when its topic is not in `TOPICS` or
# MAGIC `from_json(value)[key]` is NULL (corrupt JSON or missing key), which is precisely what `expect_or_drop` drops.

# COMMAND ----------

r = spark.sql("""SELECT (SELECT count(*) FROM bronze_multiplex) b,
  (SELECT count(*) FROM silver_orders) + (SELECT count(*) FROM silver_customers)
  + (SELECT count(*) FROM silver_clickstream) + (SELECT count(*) FROM bronze_multiplex_quarantine) s""").first()
assert r.b == r.s, f"bronze={r.b}, fan-out total={r.s}"
print("OK - every multiplex record was routed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Delta sink: streaming out of the pipeline
# MAGIC `dp.create_sink(..., format="delta", options={"path": ...})` + an `@dp.append_flow` writes checkout clicks to
# MAGIC a Delta location **outside** the pipeline's tables. The sink does not appear as a pipeline table in the catalog;
# MAGIC it is only a node in the graph. (`options={"tableName": "workspace.sdp_1_2.clickstream_export_tbl"}` writes to a
# MAGIC Unity Catalog table instead; the name must be fully qualified.)

# COMMAND ----------

sink = spark.read.format("delta").load(SINK_PATH)
print("rows in sink:", sink.count())
display(sink.groupBy("page").count())
try:
    display(spark.sql(f"DESCRIBE HISTORY delta.`{SINK_PATH}`").select("version", "operation", "operationMetrics"))
except Exception as e:
    print("Path-based DESCRIBE HISTORY not allowed here:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Iceberg reads
# MAGIC **Databricks rule:** Iceberg reads (UniForm) can't be enabled on streaming tables / materialized views that a pipeline
# MAGIC owns with `IcebergCompatV2`, which is why the pipeline runs with `enable_uniform=false`. For pipeline tables
# MAGIC Databricks offers `IcebergCompatV3` / *compatibility mode* (preview). For any other Delta table, UniForm works
# MAGIC as shown here: publish an Iceberg-readable copy of `silver_orders`. Deletion vectors are disabled explicitly
# MAGIC because they conflict with IcebergCompatV2 (and new serverless tables may enable them by default).

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE orders_iceberg_readable
TBLPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.enableDeletionVectors' = 'false')
AS SELECT * FROM silver_orders""")
display(spark.sql("SHOW TBLPROPERTIES orders_iceberg_readable")
        .filter("key LIKE 'delta.universalFormat%' OR key LIKE '%iceberg%' OR key LIKE 'delta.columnMapping%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC `DESCRIBE EXTENDED` shows a *Delta Uniform Iceberg* section with the Iceberg metadata location and the
# MAGIC converted Delta version. Column mapping was switched on automatically (an IcebergCompatV2 requirement).
# MAGIC Iceberg metadata is generated **asynchronously** after each commit: if the section is empty, rerun the cell.

# COMMAND ----------

display(spark.sql("DESCRIBE EXTENDED orders_iceberg_readable")
        .filter("col_name ILIKE '%iceberg%' OR col_name ILIKE '%uniform%' OR col_name ILIKE '%converted%' "
                "OR col_name ILIKE '%metadata%' OR data_type ILIKE '%.metadata.json%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC Alternative since 2025: a **managed Iceberg table** (Unity Catalog writes native Iceberg, no Delta at all).

# COMMAND ----------

try:
    spark.sql("CREATE OR REPLACE TABLE orders_managed_iceberg USING ICEBERG AS SELECT * FROM silver_orders")
    # DESCRIBE DETAIL is a Delta command; DESCRIBE EXTENDED works for any table format.
    display(spark.sql("DESCRIBE EXTENDED orders_managed_iceberg")
            .filter("col_name IN ('Provider', 'Type', 'Location')"))
except Exception as e:
    print("Managed Iceberg tables not available in this workspace:", str(e)[:300])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: read it like an external engine (Iceberg REST catalog)
# MAGIC External engines (Snowflake, Trino, Spark OSS, PyIceberg) connect to
# MAGIC `https://<workspace>/api/2.1/unity-catalog/iceberg-rest`. This needs *External data access* enabled on the
# MAGIC metastore and `EXTERNAL USE SCHEMA` on the schema, which Free Edition may not allow. The cell uses this
# MAGIC notebook's own short-lived token; nothing is stored. (`pyiceberg` was installed in the first cell.)

# COMMAND ----------

try:
    from databricks.sdk import WorkspaceClient
    from pyiceberg.catalog.rest import RestCatalog

    # The SDK's notebook-native auth works on serverless, where the py4j context token getters are blocked.
    cfg = WorkspaceClient().config
    token = cfg.authenticate()["Authorization"].split(" ", 1)[1]
    catalog = RestCatalog("uc", uri=f"{cfg.host.rstrip('/')}/api/2.1/unity-catalog/iceberg-rest",
                          warehouse="workspace", token=token)
    tbl = catalog.load_table("sdp_1_2.orders_iceberg_readable")
    print("Iceberg snapshot:", tbl.current_snapshot().snapshot_id)
    print(tbl.scan(limit=5).to_pandas())
except Exception as e:
    print("Iceberg REST read not available here (expected on Free Edition):", str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - New data arrives
# MAGIC In `00-land-course-data`, run the **multiplex** cell again (the next 6 hours of the stream), then **Start**.
# MAGIC Each streaming table (and the sink flow) only processes the new records; the materialized view is refreshed.

# COMMAND ----------

display(flow_rows(EVENT_LOG))
print("rows in sink:", spark.read.format("delta").load(SINK_PATH).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - A full refresh does NOT clean up a sink
# MAGIC Note the sink row count, run **Full refresh all**, and count again: the pipeline tables are rebuilt with the same
# MAGIC counts, but the sink **doubles**, because its flow restarts from the beginning and appends everything again.

# COMMAND ----------

n_sink = spark.read.format("delta").load(SINK_PATH).count()
n_checkout = spark.table("silver_clickstream").filter("page IN ('/cart', '/checkout')").count()
print("rows in sink:", n_sink)
print("checkout clicks in silver_clickstream:", n_checkout)
print(f"sink / source ratio: {n_sink / max(n_checkout, 1):.1f}  (1.0 before the full refresh, 2.0 after)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key takeaways
# MAGIC - Multiplex Bronze clustered by `topic`; fan-out tables generated in a loop.
# MAGIC - Unparseable payloads go to a quarantine instead of disappearing.
# MAGIC - Sinks: append flows only, no expectations, exactly-once through the checkpoint, not cleaned by a full refresh.
# MAGIC - Iceberg interoperability: UniForm or managed Iceberg tables, served through the UC Iceberg REST catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - Multiplex = one Bronze table for many topics; fan out with `filter(topic = ...)` + `from_json(value, schema)`.
# MAGIC   Tables created in a loop must bind the loop variable (function argument), or every table sees the last value.
# MAGIC - `expect_or_drop` drops rows whose constraint is false **or NULL**; dropped counts appear in the event log
# MAGIC   (`flow_progress.data_quality`), not in any table. Keep them with an inverse-predicate quarantine table.
# MAGIC - `dp.create_sink` supports `format="delta"` (`path` or `tableName`) and `"kafka"`; only `@dp.append_flow` can write
# MAGIC   to a sink, and expectations are not supported on sinks.
# MAGIC - A full refresh resets streaming checkpoints and rebuilds pipeline tables, but never truncates a sink:
# MAGIC   reprocessed data is appended again (duplicates).
# MAGIC - UniForm: `delta.enableIcebergCompatV2 = true` + `delta.universalFormat.enabledFormats = iceberg`; needs column
# MAGIC   mapping (auto-enabled at creation) and no deletion vectors. Iceberg metadata is generated asynchronously and
# MAGIC   UniForm tables are read-only for Iceberg clients.
# MAGIC - Managed Iceberg (`USING ICEBERG`) tables are read/write for external engines via the UC Iceberg REST catalog
# MAGIC   (`/api/2.1/unity-catalog/iceberg-rest`, warehouse = catalog), which requires external data access on the
# MAGIC   metastore and `EXTERNAL USE SCHEMA`.
