# Course 1: Advanced Techniques with Spark Declarative Pipelines

Production patterns for Lakeflow **Spark Declarative Pipelines (SDP)**: multi-source ingestion, data quality, Liquid Clustering, multiplex streaming, sinks, Iceberg interoperability, and automated CDC.

**Prerequisites:** SDP basics (streaming tables, materialized views, the pipeline UI), Delta Lake, Structured Streaming (micro-batches, checkpoints).

## Lesson map

| # | Official lesson | Practice here |
|---|---|---|
| 1 | Introduction to Multi Flows, Expectations and Liquid Clustering in SDP | [Key concepts](#1-multi-flows-expectations-liquid-clustering) |
| 2 | **Demo:** Multi Flow SDP with Liquid Clustering and Data Quality | [`notebooks/1.1-demo-...`](notebooks/1.1-demo-multi-flow-sdp-liquid-clustering-data-quality.ipynb) · [`pipelines/multi_flow`](pipelines/multi_flow/transformations) |
| 3 | Introduction to Multiplex Streaming, Delta Sinks and Iceberg Reads | [Key concepts](#2-multiplex-sinks-iceberg) |
| 4 | **Demo:** Multiplex Streaming SDP with Delta Sinks and Iceberg Reads | [`notebooks/1.2-demo-...`](notebooks/1.2-demo-multiplex-streaming-delta-sinks-iceberg-reads.ipynb) · [`pipelines/multiplex`](pipelines/multiplex/transformations) |
| 5 | Change Data Capture (CDC) Review | [Key concepts](#3-cdc-and-auto-cdc) |
| 6 | **Demo:** Automating SCD Type 2 with AUTO CDC in SDP | [`notebooks/1.3-demo-...`](notebooks/1.3-demo-auto-cdc-scd-type-2.ipynb) · [`pipelines/auto_cdc`](pipelines/auto_cdc/transformations) |
| 7 | Advanced Data Quality Checks and Expectations in SDP | [Key concepts](#4-advanced-data-quality) |
| 8 | **Demo:** Advanced Data Quality Checks and Expectations in SDP | [`notebooks/1.4-demo-...`](notebooks/1.4-demo-advanced-data-quality-expectations.ipynb) · [`pipelines/advanced_dq`](pipelines/advanced_dq/transformations) |
| 9 | **Lab:** Building a Multi-Source E-commerce Pipeline with SDP | [`notebooks/1.5-lab-...`](notebooks/1.5-lab-multi-source-ecommerce-pipeline.ipynb) · starter [`pipelines/lab_ecommerce`](pipelines/lab_ecommerce/transformations) · [`solutions/lab_ecommerce`](solutions/lab_ecommerce/transformations) |
| + | Bonus: the open-source SDP engine (Apache Spark 4.1+) | [`notebooks/1.6-bonus-...`](notebooks/1.6-bonus-open-source-spark-declarative-pipelines.ipynb) · [`oss-spark-pipelines`](oss-spark-pipelines/transformations) |

## Folder layout

```
01-advanced-sdp/
├── notebooks/        run these (they drive the pipelines, like the pipeline UI does)
├── pipelines/        pipeline SOURCE files: what you'd put in a Databricks pipeline, unchanged
│   ├── multi_flow/transformations/       01_bronze.py · 02_silver.py · 03_gold.py
│   ├── multi_flow_extras/flow_c.py       adding a source without a full refresh
│   ├── multiplex/transformations/        bronze multiplex · per-topic silver (UniForm) · sink + gold
│   ├── auto_cdc/transformations/         CDC feed · SCD1 + SCD2 targets
│   ├── advanced_dq/transformations/      rules table · quarantine · reconciliation checks
│   └── lab_ecommerce/transformations/    LAB starter (TODOs)
├── solutions/lab_ecommerce/              lab solution
├── oss-spark-pipelines/                  project for the open-source spark-pipelines CLI
└── databricks/                           run the same pipelines on a Databricks workspace (+ SQL versions)
```

The pipeline files import `from pyspark import pipelines as dp`, exactly as on Databricks. The local engine (`adelab.sdp`) resolves that import while it runs a pipeline. Settings are read with `spark.conf.get("key")` and passed as `configuration={...}`, which is *Pipeline settings → Configuration* in the UI.

## Key concepts

### 1. Multi flows, expectations, Liquid Clustering
- **Flow**: one streaming query that writes to a target. `@dp.table` has an implicit flow. **`@dp.append_flow(target=...)`** adds more flows to the same streaming table (`dp.create_streaming_table`).
- Why multi-flow instead of `union`: each flow has its **own checkpoint**. You can add a source, remove one, or backfill one (`once=True`) without a full refresh, and the sources can have different schemas or formats.
- **Expectations**: `expect` (warn: keep the row, count the violation), `expect_or_drop`, `expect_or_fail` (stop the update; the failed micro-batch is not committed), plus the `expect_all*` variants with a dict of rules. Metrics land in the **event log** (`flow_progress` → `details.flow_progress.data_quality`).
- Expectations belong to **datasets**, not flows: you can't put them on an `@dp.append_flow`. For a multi-flow table, pass `expect_all*={...}` to `dp.create_streaming_table()`; they apply to every flow into it.
- **Quarantine**: don't let dropped rows vanish. Write the **inverse** of the drop rules to a quarantine table.
- **Liquid Clustering** (`cluster_by=[...]`, or `cluster_by_auto=True` on Databricks): incremental clustering, keys can be changed without a rewrite, replaces partitioning and Z-ORDER. Pipeline tables are optimized automatically on Databricks.

### 2. Multiplex, sinks, Iceberg
- **Multiplex**: one Bronze table for many event types (topic, key, value), then a fan-out to one Silver table per topic. Cluster Bronze by `topic`. Generate the per-topic tables in a Python **loop** (wrap the definition in a function to bind the loop variable).
- **Sinks** (`dp.create_sink(name, format, options)` + `@dp.append_flow(target=sink)`): stream data **out** of a pipeline to Delta paths or tables (`options={"path": ...}` or `{"tableName": ...}`), or to Kafka / Event Hubs. Exactly-once through the flow checkpoint. Only append flows can write to a sink, sinks don't support expectations, and a **full refresh doesn't clean up a sink**: the reprocessed records are appended again.
- **UniForm / Iceberg reads**: `delta.enableIcebergCompatV2` + `delta.universalFormat.enabledFormats = 'iceberg'` makes Delta also write Iceberg metadata. Iceberg engines (Snowflake, Trino, BigQuery, PyIceberg) read the same files. On Databricks they connect through the **Unity Catalog Iceberg REST catalog**. Requirements: deletion vectors off, column mapping (enabled automatically).

### 3. CDC and AUTO CDC
- A CDC feed carries `INSERT/UPDATE/DELETE` events plus an ordering column. Problems: **out-of-order** events, several changes per key in one batch, deletes followed by late updates.
- **`AUTO CDC INTO`** (Python `dp.create_auto_cdc_flow`, formerly `APPLY CHANGES`): `keys`, `sequence_by`, `apply_as_deletes`, `column_list` / `except_column_list`, `stored_as_scd_type` 1 or 2, `track_history_column_list` / `track_history_except_column_list`.
- **SCD1** keeps the current state, with tombstones for deletes so late updates are ignored. **SCD2** keeps history with `__START_AT` / `__END_AT`. A late event is placed in sequence order, and changes to untracked columns update the current version in place.

### 4. Advanced data quality
- **Rules as data**: keep expectations in a governed Delta table and load them by tag (`expect_all_or_drop(get_rules("orders"))`).
- **Zero-data-loss quarantine**: `is_quarantined = NOT(all rules)` on a flagged table, then valid and quarantine datasets.
- **Null-safe rules**: a constraint must evaluate to true or false. `qty > 0` is NULL for a missing `qty`, and Databricks doesn't document how a NULL result is counted, so write `qty IS NOT NULL AND qty > 0`. (The local engine counts NULL as a failure.)
- **Schema drift**: Auto Loader `schemaHints`, `schemaEvolutionMode` (`addNewColumns` / `rescue` / `none`), and `_rescued_data`, monitored with a warn expectation.
- **Table-level checks**: aggregate expectations on a materialized view (row-count reconciliation across layers, non-negative totals, referential integrity through joins).

## Run it on Databricks Free Edition
Step by step: [`../FREE_EDITION_GUIDE.md`](../FREE_EDITION_GUIDE.md) → Course 1. You create one pipeline per lesson from these source files and inspect it with the companion notebooks in [`databricks/`](databricks/README.md). SQL versions of the demo pipelines are in `databricks/sql/`.
