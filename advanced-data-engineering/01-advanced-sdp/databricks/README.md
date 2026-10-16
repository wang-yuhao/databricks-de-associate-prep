# Running Course 1 on a Databricks workspace

The pipeline source files in `../pipelines/` use the Databricks API unchanged. To run them for real (Databricks Free Edition works):

## 1. Get the repository into the workspace
*Workspace → Create → Git folder* → paste this repository's GitHub URL. The whole repo, including `adelab/datagen.py`, is now available in the workspace.

## 2. Land the course data in a Unity Catalog volume
Open [`00-land-course-data.py`](00-land-course-data.py) from the Git folder and run it on serverless compute. It creates `workspace.sdp_course` with a `landing` volume and writes the same synthetic files the local notebooks use.

## 3. Create a pipeline
*Jobs & Pipelines → Create → ETL pipeline*:

| Setting | Value |
|---|---|
| Name | `multi_flow` (or `multiplex`, `auto_cdc`, ...) |
| Default catalog / schema | `workspace` / `sdp_course` |
| Source code | *Add existing assets*: the `transformations` folder of the pipeline, e.g. `advanced-data-engineering/01-advanced-sdp/pipelines/multi_flow/transformations` |
| Configuration | see the table below |
| Compute | Serverless |

| Pipeline | Configuration keys (values = folders in `/Volumes/workspace/sdp_course/landing/...`) |
|---|---|
| `multi_flow` | `source_flow_a`, `source_flow_b` (optional: `event_id_policy` = `fail`/`drop`) |
| `multiplex` | `multiplex_source`, `clickstream_sink_path` (e.g. `/Volumes/workspace/sdp_course/landing/exports/clickstream`) |
| `auto_cdc` | `cdc_source` |
| `advanced_dq` | `orders_source` = `.../landing/ecommerce/orders_us`, `products_source` = `.../landing/ecommerce/products`, `rules_table` = `workspace.sdp_course.dq_rules` (create and fill it as in notebook 1.4, section B) |
| `lab_ecommerce` | `orders_eu_source`, `orders_us_source`, `products_source`, `customers_source` = `.../landing/ecommerce/` + `orders_eu`, `orders_us`, `products`, `customers_cdc` |

Click **Start**. Compare the pipeline graph, the *Data quality* tab, and the event log with what the local notebooks showed. To land more data, rerun the matching cell of `00-land-course-data.py`, then start another update.

Local-only behavior to be aware of:
- On Databricks the Auto Loader schema location is managed for you, and Liquid Clustering / OPTIMIZE run automatically.
- Event log queries use `event_log(TABLE(...))` or the pipeline's event log table instead of `pipe.event_log()`.

## 4. SQL versions
[`sql/`](sql) has SQL-language versions of the multi-flow and AUTO CDC pipelines. Create a pipeline with those files as source code to compare the two syntaxes.

## 5. Deploy as code
Course 4 deploys a pipeline with a Declarative Automation Bundle (`04-automated-deployment-dabs/bundles/02_multi_env_dab`). The same approach works for these pipelines: point `libraries.glob.include` at a `transformations` folder.
