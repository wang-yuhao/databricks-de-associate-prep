# Running Course 1 on Databricks (Free Edition works)

Step-by-step instructions: [`../../FREE_EDITION_GUIDE.md`](../../FREE_EDITION_GUIDE.md) → Course 1.

The pipeline source files in `../pipelines/` use the Databricks API unchanged; you create one Lakeflow pipeline per lesson from them and inspect it with the companion notebook.

| File | Purpose |
|---|---|
| `00-land-course-data` | creates `workspace.sdp_course` (landing volume, `dq_rules`) and one schema per pipeline; each cell lands the next file of a lesson |
| `_event_log_helpers` | `%run` helpers: updates, rows per flow, expectation metrics, errors (from the published event log) |
| `1.1-multi-flow-companion` | lesson 1.1: multi-flow, expectations, quarantine, LC, incremental, add a flow, fail + recover, full refresh |
| `1.2-multiplex-companion` | lesson 1.2: fan-out, Delta sink, Iceberg reads (UniForm copy, managed Iceberg, Iceberg REST) |
| `1.3-auto-cdc-companion` | lesson 1.3: SCD1 / SCD2 batch by batch, late data, point-in-time, MERGE comparison |
| `1.4-advanced-dq-companion` | lesson 1.4: rules table, quarantine, reconciliation, schema drift, DQ trend |
| `1.5-lab-ecommerce-companion` | lab 1.5: CHECK cells for the TODOs in `../pipelines/lab_ecommerce` |
| `sql/` | SQL versions of the multi-flow and AUTO CDC pipelines |

| Pipeline | Schema | Configuration keys (values: folders in `/Volumes/workspace/sdp_course/landing/...`) |
|---|---|---|
| `multi_flow` | `sdp_1_1` | `source_flow_a`, `source_flow_b` (later `source_flow_c`, `event_id_policy` = `fail`/`drop`) |
| `multiplex` | `sdp_1_2` | `multiplex_source`, `clickstream_sink_path` = `.../exports/clickstream`, `enable_uniform` = `false` |
| `auto_cdc` | `sdp_1_3` | `cdc_source` = `.../customers_cdc` |
| `advanced_dq` | `sdp_1_4` | `orders_source` = `.../ecommerce_dq/orders_us`, `products_source` = `.../ecommerce_dq/products`, `rules_table` = `workspace.sdp_course.dq_rules` |
| `lab_ecommerce` | `sdp_1_5` | `orders_eu_source`, `orders_us_source`, `products_source`, `customers_source` = `.../ecommerce/` + `orders_eu`, `orders_us`, `products`, `customers_cdc` |

One schema per pipeline: two pipelines can't publish tables with the same name into one schema. Every pipeline publishes its event log to `<schema>.event_log`.

Differences from the local runtime:
- Auto Loader schema location, Liquid Clustering and OPTIMIZE are managed by Databricks.
- Iceberg reads (UniForm) are not allowed on pipeline-owned tables, hence `enable_uniform=false`; the companion shows UniForm on a regular table instead.
- Free Edition runs one active pipeline at a time.

Deploy as code: Course 4 deploys a pipeline with a bundle (`04-automated-deployment-dabs/bundles/02_multi_env_dab`); the same approach works for these pipelines (`libraries.glob.include` pointing at a `transformations` folder).
