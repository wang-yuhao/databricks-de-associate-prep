# Practice the Advanced Data Engineering series on Databricks Free Edition

Step-by-step guide for the four Databricks Academy courses, on a **Databricks Free Edition** workspace (serverless, Unity Catalog):

| # | Course | Where you practice |
|---|---|---|
| 1 | Advanced Techniques with Spark Declarative Pipelines | Lakeflow pipelines in the UI + companion notebooks in [`01-advanced-sdp/databricks/`](01-advanced-sdp/databricks) |
| 2 | Databricks Data Privacy | notebooks in [`02-data-privacy/databricks/`](02-data-privacy/databricks) |
| 3 | Databricks Performance Optimization | notebooks in [`03-performance-optimization/databricks/`](03-performance-optimization/databricks) + query profiles |
| 4 | Automated Deployment with Declarative Automation Bundles | Databricks CLI on your laptop + [`04-automated-deployment-dabs/databricks/`](04-automated-deployment-dabs/databricks) |

Every lesson of the official outlines is mapped in the [coverage matrix](#7-coverage-matrix). The local Jupyter track (`*/notebooks/*.ipynb`) stays available for offline practice; this guide is the workspace track.

Conventions: **▶** = do this in the workspace UI, **`$`** = run in a terminal on your laptop, **CHECK** = a notebook cell that asserts the expected result. Tick the boxes as you go.

---

## 1. One-time setup (about 20 minutes)

### 1.1 Workspace
- [ ] Sign up at **databricks.com/learn/free-edition** (email OTP, Google or Microsoft login). Note the workspace URL (`https://dbc-....cloud.databricks.com`).
- [ ] ▶ **Catalog** (left bar): you have a catalog called `workspace` with a `default` schema. All course schemas go into `workspace`.

### 1.2 Get this repository into the workspace
- [ ] ▶ **Workspace → Home → Create → Git folder** → URL `https://github.com/wang-yuhao/databricks-de-associate-prep` → *Create Git folder*.
  The repo now lives at `/Workspace/Users/<you>/databricks-de-associate-prep`. Below, `<repo>` means `/Workspace/Users/<you>/databricks-de-associate-prep/advanced-data-engineering`.
- [ ] To pull later changes: open the Git folder → branch name button → **Pull**. Your lab edits are local to that Git folder until you commit them.

### 1.3 Run a notebook on serverless
- [ ] ▶ Open `<repo>/01-advanced-sdp/databricks/00-land-course-data` → compute selector (top right) → **Serverless** → run **section 0**. It should print the landing path. If it fails on `from adelab import datagen`, the notebook is not inside the Git folder.

### 1.4 Databricks CLI on your laptop (Courses 2 and 4)
```
$ databricks --version                         # v0.2xx or newer; install: winget install Databricks.DatabricksCLI
$ databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile DEFAULT
$ databricks current-user me                   # prints your user: authentication works
```
OAuth (U2M) opens a browser and stores a refreshable token in `~/.databrickscfg`/token cache. No personal access token needed.

### 1.5 Free Edition limits that shape this guide
| Limit | Consequence for the labs |
|---|---|
| Serverless compute only | no Spark UI → use the **query profile** (*See performance* under a cell). `cache()` / `persist()` unsupported. Only 6 Spark confs can be set (`spark.sql.shuffle.partitions`, `spark.sql.files.maxPartitionBytes`, `spark.sql.session.timeZone`, `spark.sql.ansi.enabled`, `spark.sql.legacy.timeParserPolicy`, `spark.databricks.execution.timeout`). |
| One active pipeline per type | run Course 1 pipelines **one at a time**; wait until an update finishes before starting another pipeline. |
| 5 concurrent job tasks | Course 4 jobs are small; don't run several bundles' jobs at once. |
| Daily usage quota | if compute stops for the day, your data is kept; continue tomorrow. Keep data sizes as in the notebooks. |
| Single user, no account console / SCIM | Course 2 personas are simulated with a mapping table (row filters and masks apply to you too, so the test is real). |
| One workspace | "dev / stage / prod" = different schemas and folders in the same workspace. |

---

## 2. What differs from the course videos

| Video shows | Free Edition way | Where |
|---|---|---|
| Classroom setup `%run ./Includes/Classroom-Setup` | `00-land-course-data` / `%run ./_setup` | each `databricks/` folder |
| Pipeline event log via `event_log(TABLE(...))` | publish the event log to `<schema>.event_log`, then query the table (helpers in `_event_log_helpers`) | Course 1 |
| UniForm on pipeline tables | not allowed on pipeline-owned tables: pipelines run with `enable_uniform=false`; Iceberg reads are done on a UniForm copy / managed Iceberg table | 1.2 |
| Groups `eu_analysts`, `admins` | `user_access` mapping table + `use_persona()`; group version shown for reference | 2.1 |
| `VACUUM ... RETAIN 0 HOURS` with the safety check disabled | lower the table's `delta.deletedFileRetentionDuration`, `VACUUM`, restore 7 days | 2.3, 2.4 |
| Spark UI tabs, `spark.conf.set(autoBroadcastJoinThreshold / AQE ...)` | query profile + `explain()`; join **hints**; `shuffle.partitions` is settable | Course 3 |
| Choosing instance types | decision exercise + serverless notebook vs SQL warehouse comparison | 3.7 |
| Instructor's workspace with service principals | your user for dev; optional service principal for stage/prod and GitHub Actions | Course 4 |

---

## 3. Course 1 - Advanced Techniques with Spark Declarative Pipelines

### 3.0 How every Course 1 lesson works
1. Land data with a cell of `01-advanced-sdp/databricks/00-land-course-data`.
2. ▶ Create / start the pipeline in **Jobs & Pipelines**.
3. Inspect it with the lesson's companion notebook (`1.x-...-companion`), which runs `%run ./_event_log_helpers`.

**Creating a pipeline** (same for every lesson; the per-lesson table gives the values):
1. ▶ **Jobs & Pipelines → Create → ETL pipeline**.
2. Name it, set **default catalog** `workspace` and **default schema** from the table.
3. Choose **Add existing assets** → *Pipeline root folder* = the lesson's pipeline folder in `<repo>/01-advanced-sdp/pipelines/...`, *Source code paths* = its `transformations` folder (or single files where noted).
4. ▶ **Settings** (gear) → **Configuration** → add the key/value pairs.
5. ▶ **Settings → Advanced → Event logs → Publish event log to metastore**: catalog `workspace`, schema = the pipeline's schema, name `event_log`.
6. Compute stays **Serverless**; pipeline mode stays **Triggered**; keep **Development** mode on while practicing.
7. ▶ **Start** (or **Dry run** first: validates the graph without processing data).

Alternative to steps 2-5: ▶ *Settings → JSON* and paste (replace `<you>`):
```json
{
  "name": "multi_flow",
  "catalog": "workspace",
  "schema": "sdp_1_1",
  "serverless": true,
  "development": true,
  "root_path": "/Workspace/Users/<you>/databricks-de-associate-prep/advanced-data-engineering/01-advanced-sdp/pipelines/multi_flow",
  "libraries": [{"glob": {"include": "/Workspace/Users/<you>/databricks-de-associate-prep/advanced-data-engineering/01-advanced-sdp/pipelines/multi_flow/transformations/**"}}],
  "configuration": {
    "source_flow_a": "/Volumes/workspace/sdp_course/landing/source_flow_a",
    "source_flow_b": "/Volumes/workspace/sdp_course/landing/source_flow_b"
  },
  "event_log": {"catalog": "workspace", "schema": "sdp_1_1", "name": "event_log"}
}
```

### 3.1 Lesson 1.1 - Multi Flow SDP with Liquid Clustering and Data Quality
Concepts first: [Course 1 README §1](01-advanced-sdp/README.md#1-multi-flows-expectations-liquid-clustering).

| Setting | Value |
|---|---|
| Pipeline / schema | `multi_flow` / `sdp_1_1` |
| Source | `pipelines/multi_flow/transformations` |
| Configuration | `source_flow_a` = `/Volumes/workspace/sdp_course/landing/source_flow_a`, `source_flow_b` = `.../source_flow_b` |
| Companion | `01-advanced-sdp/databricks/1.1-multi-flow-companion` |

- [ ] Land: `00-land` → section 0, then cell **"1.1 step 1"**.
- [ ] Read the code: `01_bronze.py` (`dp.create_streaming_table` + two `@dp.append_flow`), `02_silver.py` (warn / drop / fail expectations, `cluster_by`, quarantine), `03_gold.py` (materialized view).
- [ ] Create the pipeline, click **Dry run**, then **Start**.
- [ ] ▶ In the graph: `bronze_events` has 2 incoming flows. Click `silver_events` → **Data quality** tab.
- [ ] Companion steps 1-5: raw data, rows per flow, expectation metrics, `bronze = silver + quarantine` (CHECK), `clusteringColumns`, Gold MV.
- [ ] Optional: change `silver_events` to `cluster_by_auto=True`, run, inspect, revert.
- [ ] **Incremental**: `00-land` cell "new file in flow B only" → **Start** → companion step 6 (flow_a 0 rows, flow_b ~50).
- [ ] **Add a source without full refresh**: `00-land` cell "flow C" → *Settings → Source code* add `pipelines/multi_flow_extras/flow_c.py` → add config `source_flow_c` = `.../source_flow_c` → **Start** → companion step 7.
- [ ] **expect_or_fail**: `00-land` cell "event_id = NULL" → **Start** → update fails; read the error (companion step 8). Recover: config `event_id_policy` = `drop` → **Start**.
- [ ] **Full refresh**: ▶ arrow next to *Start* → **Full refresh all** → companion step 9.
- [ ] Optional SQL syntax: second pipeline `multi_flow_sql`, schema `sdp_1_1_sql`, source `01-advanced-sdp/databricks/sql/multi_flow.sql`, same configuration.

### 3.2 Lesson 1.2 - Multiplex Streaming with Delta Sinks and Iceberg Reads
| Setting | Value |
|---|---|
| Pipeline / schema | `multiplex` / `sdp_1_2` |
| Source | `pipelines/multiplex/transformations` |
| Configuration | `multiplex_source` = `/Volumes/workspace/sdp_course/landing/multiplex`, `clickstream_sink_path` = `/Volumes/workspace/sdp_course/landing/exports/clickstream`, **`enable_uniform` = `false`** |
| Companion | `1.2-multiplex-companion` |

- [ ] Land: `00-land` → multiplex cell. Wait until the `multi_flow` pipeline is idle (one active pipeline).
- [ ] Read the code: per-topic tables generated in a **loop through a function** (binds the loop variable), quarantine for corrupt payloads, `dp.create_sink` + `@dp.append_flow(target=sink)`.
- [ ] Create + **Start**. ▶ In the graph, the sink is a node but not a catalog table.
- [ ] Companion steps 1-3: fan-out CHECK, `clusteringColumns = [topic]`, read the sink by path.
- [ ] Companion step 4: Iceberg. UniForm copy (`SHOW TBLPROPERTIES`, `DESCRIBE EXTENDED`), managed Iceberg table, optional Iceberg REST read with PyIceberg.
- [ ] Step 5: land another multiplex file → **Start** → only new records processed.
- [ ] Step 6: **Full refresh all** → the sink row count **doubles** (sinks aren't cleaned by a full refresh).

### 3.3 Lesson 1.3 - Automating SCD Type 2 with AUTO CDC
| Setting | Value |
|---|---|
| Pipeline / schema | `auto_cdc` / `sdp_1_3` |
| Source | `pipelines/auto_cdc/transformations` |
| Configuration | `cdc_source` = `/Volumes/workspace/sdp_course/landing/customers_cdc` |
| Companion | `1.3-auto-cdc-companion` |

- [ ] Concepts: [README §3](01-advanced-sdp/README.md#3-cdc-and-auto-cdc): out-of-order events, `keys`, `sequence_by`, `apply_as_deletes`, `except_column_list`, `stored_as_scd_type`, `track_history_except_column_list`.
- [ ] Land batch **0** (widget `cdc_batch`) → **Start** → companion step 1 (4 / 4 rows).
- [ ] Land batch **1** → **Start** → step 2 (SCD1 4 rows, SCD2 7 rows; email-only change updates in place).
- [ ] Land batch **2** → **Start** → step 3 (late events placed in sequence order, customer 3 stays deleted; CHECK).
- [ ] Step 4: point-in-time query (`as_of_sequence` widget), `customers_current` MV.
- [ ] Step 5: hand-written MERGE and why batch-by-batch it resurrects deleted rows.
- [ ] Optional: SQL version `databricks/sql/auto_cdc.sql` in schema `sdp_1_3_sql`.

### 3.4 Lesson 1.4 - Advanced Data Quality Checks and Expectations
| Setting | Value |
|---|---|
| Pipeline / schema | `advanced_dq` / `sdp_1_4` |
| Source | `pipelines/advanced_dq/transformations` |
| Configuration | `orders_source` = `/Volumes/workspace/sdp_course/landing/ecommerce_dq/orders_us`, `products_source` = `.../ecommerce_dq/products`, `rules_table` = `workspace.sdp_course.dq_rules` |
| Companion | `1.4-advanced-dq-companion` |

- [ ] `00-land`: target `ecommerce_dq (demo 1.4)`, batch 0 → land; run the **rules table** cell.
- [ ] Read the code: rules loaded **by tag** from a table, `is_quarantined` flag + `partition_cols`, `@dp.temporary_view`, stream-static join for referential integrity, `dq_reconciliation` MV with aggregate expectations, `_rescued_data` warn rule.
- [ ] **Start** → companion steps 1-3 (metrics, quarantine with `failed_rules`, reconciliation CHECK).
- [ ] Schema drift: land batch 1 into `ecommerce_dq` → **Start** → step 4 (`_rescued_data`, `no_rescued_data` metric).
- [ ] Change a rule without code: step 5 inserts `plausible_price` → land batch 2 → **Start** → new rule in metrics.
- [ ] Step 6: DQ trend over all updates; ▶ add a line-chart visualization. Optional: put it on an AI/BI dashboard or a SQL alert.
- [ ] Step 7: choose warn / drop / fail; null-safe rules experiment.

### 3.5 Lesson 1.5 - Lab: Multi-Source E-commerce Pipeline
| Setting | Value |
|---|---|
| Pipeline / schema | `lab_ecommerce` / `sdp_1_5` |
| Source | **files, one per step**: `pipelines/lab_ecommerce/transformations/01_bronze_orders.py`, later `02_dimensions.py`, `03_silver_gold.py` |
| Configuration | `orders_eu_source`, `orders_us_source`, `products_source`, `customers_source` = `/Volumes/workspace/sdp_course/landing/ecommerce/` + `orders_eu`, `orders_us`, `products`, `customers_cdc` |
| Companion | `1.5-lab-ecommerce-companion` |

- [ ] `00-land`: target `ecommerce (lab 1.5)`, batch 0.
- [ ] Step 1: complete TODO 1.1-1.3 (edit the file in the Git folder or in the pipeline editor) → pipeline with file 01 only → **Start** → CHECK.
- [ ] Step 2: TODO 2.1-2.3 → add file 02 → **Start** → CHECK.
- [ ] Step 3: TODO 3.1-3.5 → add file 03 → **Start** → CHECK.
- [ ] Step 4: land batch 1 → **Start** (no full refresh) → CHECK (240 EU / 200 US, SCD2 history).
- [ ] Step 5 (stretch): production mode, a schedule (remove it afterwards), lineage graph.
- [ ] Answer the reflection questions. Stuck? Compare with `solutions/lab_ecommerce/transformations/`, or point the pipeline at the solution folder.

### 3.6 Bonus 1.6 - open-source SDP engine
Local only (`spark-pipelines` CLI of Apache Spark 4.1+): notebook `01-advanced-sdp/notebooks/1.6-bonus-...ipynb` on your laptop. On Databricks the same API runs in Lakeflow pipelines.

### 3.7 Course 1 clean-up
- [ ] ▶ Delete the pipelines (*Jobs & Pipelines* → ⋮ → Delete). Then the last cell of `00-land-course-data` drops all `sdp_*` schemas.

---

## 4. Course 2 - Databricks Data Privacy

Concepts: [Course 2 README](02-data-privacy/README.md#key-concepts) (GDPR / CCPA, PII and quasi-identifiers, UC securables, isolation, pseudonymization vs anonymization, physical deletion). All notebooks `%run ./_setup` (schema `workspace.privacy_course`).

### 4.1 Demo 2.1 - Securing Data in Unity Catalog (+ Audit Your Data, Data Isolation)
Notebook `02-data-privacy/databricks/2.1-securing-data-in-unity-catalog`.
- [ ] Steps 1-2: PII table; `GRANT` / `SHOW GRANTS` / `information_schema.table_privileges`; `REVOKE`.
- [ ] Step 3: column and table **tags**; `information_schema.column_tags`. ▶ Catalog Explorer → table → tags; search `tag:pii`. ▶ Optional: *Data classification* on the catalog, if offered.
- [ ] Steps 4-5: personas in `user_access`; row filter + column mask SQL UDFs; `is_member` / `is_account_group_member` check.
- [ ] Steps 6-7: `SET ROW FILTER`, `SET MASK`; same query as 4 personas; CHECK.
- [ ] Step 8: dynamic view alternative; optional **ABAC** policy (*Policies* tab, if available).
- [ ] Step 9: data isolation: regional catalog (or schema), workspace-catalog binding (▶ catalog → *Workspaces* tab).
- [ ] Step 10: `DESCRIBE HISTORY`, `system.access.audit`, `system.access.table_lineage` (if enabled), ▶ **Lineage** tab with column lineage.
- [ ] Optional real two-user test: ▶ *Settings → Identity and access → Users → Add* a second email you own, `GRANT` it `USE SCHEMA` + `SELECT`, log in as that user and query `customers`.

### 4.2 Demo 2.2 - PII Data Security
One-time, laptop:
```
$ databricks secrets create-scope privacy_course
$ databricks secrets put-secret privacy_course hash_salt     # paste a long random string at the prompt
$ databricks secrets put-secret privacy_course aes_key       # exactly 32 characters
$ databricks secrets list-secrets privacy_course             # names only, values are never shown
```
Notebook `2.2-pii-data-security`:
- [ ] Step 1: plain hash re-identified by a dictionary join vs secret salt; SQL `secret()`.
- [ ] Step 2: tokenization vault (restricted table, detokenize by join, delete = forget).
- [ ] Step 3: `aes_encrypt` / `aes_decrypt` with the key read by `secret()` inside the query.
- [ ] Step 4: masking with `mask()` and partial masks.
- [ ] Step 5: anonymization: generalization, suppression, perturbation, aggregation.
- [ ] Step 6: k-anonymity and l-diversity; **task**: find a generalization with k ≥ 5, publish an aggregate with small-cell suppression.

### 4.3 Demo 2.3 - Processing Records from CDF and Propagating Changes
Notebook `2.3-cdf-propagating-changes`:
- [ ] A: Bronze with `delta.enableChangeDataFeed` + deletion vectors.
- [ ] B: streaming CDF reader → `foreachBatch` MERGE into pseudonymized Silver (+ delete propagation to events), `availableNow` trigger, checkpoint in the volume.
- [ ] C-D: MERGE a CDC batch; `table_changes()`; propagate; deleted IDs gone downstream.
- [ ] E: formal deletion-request table.
- [ ] F: data still in time travel and the change feed.
- [ ] G: `REORG ... APPLY (PURGE)`, `VACUUM ... DRY RUN`, `VACUUM`, time travel fails.
- [ ] H: production checklist (`skipChangeCommits`, lineage, predictive optimization).

### 4.4 Lab 2.4 - Propagating Changes with CDF
- [ ] Notebook `2.4-lab-propagating-changes-with-cdf`: complete TODO 1-6, run each CHECK. Solution: `solutions/2.4-lab-...-solution`.

### 4.5 Course 2 clean-up
- [ ] `DROP SCHEMA workspace.privacy_course CASCADE`; `DROP CATALOG privacy_eu CASCADE` (if created); `$ databricks secrets delete-scope privacy_course` (optional).

---

## 5. Course 3 - Databricks Performance Optimization

Concepts: [Course 3 README](03-performance-optimization/README.md#key-concepts) (the 5 S's: small files, skipping, shuffle, skew, spill + serialization). All notebooks `%run ./_setup` (schema `workspace.perf_course`, predictive optimization disabled on it so the bad layouts stay bad).

**How to read a query profile:** run a cell → **See performance** below the output → click the query → **query profile**. Toggle the *Graph* / *Tree* views, click an operator for its metrics (rows, time, files read/pruned, bytes, spill), and use the *Top operators* list to find the bottleneck.

- [ ] **3.1** `3.1-query-profile-and-spark-ui`: narrow vs wide (Exchange), `explain` modes, logical → physical plan, `cache()` on serverless → materialize instead, settable confs, optional `system.query.history`, checklist.
- [ ] **3.2** `3.2-file-explosion`: tiny files and over-partitioning vs baseline (`DESCRIBE DETAIL`), measure, `OPTIMIZE`, rewrite with clustering, optimized writes / auto compaction / `delta.targetFileSize` / predictive optimization, small appends. Fill in the results table.
- [ ] **3.3 Lab** `3.3-lab-data-skipping-and-liquid-clustering`: random → Z-ORDER → Liquid Clustering → partitioning (wrong tool) → change keys + `OPTIMIZE FULL` → `delta.dataSkippingStatsColumns`. Fill in *files read / pruned* from each profile. Solution in `solutions/`.
- [ ] **3.4** `3.4-shuffle`: Exchanges in plans, sort-merge vs broadcast (hints), `shuffle.partitions` 8 / 200 / 2000 / auto and AQE coalescing, pre-aggregate and prune before a join.
- [ ] **3.5 Lab** `3.5-lab-exploding-join`: diagnose explosion from key counts, dedup the dimension, skew (task time, AQE), salting, broadcast, provoke and read spill. Solution in `solutions/`.
- [ ] **3.6** `3.6-user-defined-functions`: built-in vs SQL UDF vs pandas UDF vs Arrow UDF vs Python UDF timings, `BatchEvalPython` / `ArrowEvalPython`, UDF blocks pushdown.
- [ ] **3.7** `3.7-choosing-compute`: decision guide, serverless notebook vs SQL warehouse exercise, cluster sizing scenarios.
- [ ] Clean-up: `ALTER SCHEMA workspace.perf_course INHERIT PREDICTIVE OPTIMIZATION`, then `DROP SCHEMA workspace.perf_course CASCADE`.

---

## 6. Course 4 - Automated Deployment with Declarative Automation Bundles

Concepts: [Course 4 README](04-automated-deployment-dabs/README.md#key-concepts). You run the CLI **on your laptop** from the repo checkout (`F:\projects\databricks-de-associate-prep\advanced-data-engineering\04-automated-deployment-dabs`), against your Free Edition workspace. After each deploy, check the result in ▶ *Jobs & Pipelines* and with notebook `04-automated-deployment-dabs/databricks/4.x-inspect-deployments`.

### 6.1 Lesson 4.1 - Setup, authentication, deploying projects, DevOps / CI-CD review
- [ ] Section 1.4 above (CLI + `databricks auth login`). Then:
```
$ databricks auth profiles            # profile valid?
$ databricks catalogs list            # you see "workspace"
```
- [ ] Compare the deployment options once: ▶ create a job by hand in the UI; `$ databricks jobs list`; the REST API (`$ databricks api get /api/2.2/jobs/list`); a bundle (next lesson). Delete the hand-made job.
- [ ] Read local notebook 4.1 section C (DevOps loop, CI vs CD) or the README key concepts.

### 6.2 Demo 4.2 - Deploying a simple DAB
```
$ cd bundles/01_simple_dab
$ databricks bundle validate          # resolved config, warnings
$ databricks bundle deploy            # uploads files to /Workspace/Users/<you>/.bundle/simple_dab/dev, creates the job
$ databricks bundle summary           # resources + URLs
$ databricks bundle run hello_job     # runs on serverless jobs compute, streams the result
```
- [ ] ▶ Open the job: name `[dev <you>] hello_job`, *"managed by a bundle"* banner. Run `4.x-inspect-deployments`.
- [ ] Edit `src/hello.py`, `deploy` again: only the difference is applied (idempotent).
- [ ] `$ databricks bundle destroy --auto-approve` → job and folder are gone.

### 6.3 Lab 4.3 - Deploying a simple DAB
```
$ cd labs/4.3_simple_dab              # complete TODO 1-3 in databricks.yml and resources/etl_job.yml
$ databricks bundle validate
$ databricks bundle deploy
$ databricks bundle run etl_job --params run_date=2026-10-15
$ databricks bundle destroy --auto-approve
```
- [ ] ▶ In the run, `transform` started only after `ingest` succeeded and both received `run_date`. Solution: `solutions/4.3_simple_dab`.

### 6.4 Demo 4.4 - Variable substitutions and multiple environments
```
$ cd bundles/02_multi_env_dab
$ databricks bundle validate -t dev
$ databricks bundle validate -t stage                 # compare names, root_path, schema, mode
$ databricks bundle validate -t dev --var="orders_per_run=5"
$ databricks bundle validate -t dev -o json           # the fully resolved configuration
$ databricks bundle deploy -t dev
$ databricks bundle run -t dev orders_job             # land → pipeline → report
$ databricks bundle deploy -t stage                   # "promote": same code, other variables, production mode
$ databricks bundle summary -t stage
```
- [ ] Also try the environment variable form: PowerShell `$env:BUNDLE_VAR_orders_per_run = "7"` then `validate -t dev`.
- [ ] ▶ Compare dev (`[dev you]` prefix, schedule **paused**, pipeline in development mode) and stage (no prefix, schedule **active**, shared root path). Tables in `orders_dev` vs `orders_stage` (`4.x-inspect-deployments`).
- [ ] Pause or destroy stage right away so its daily schedule doesn't eat your quota: `$ databricks bundle destroy -t stage --auto-approve`, then dev.

### 6.5 Lab 4.5 - Deploy a DAB to multiple environments
```
$ cd labs/4.5_multi_env_dab           # TODO 1-4: required variable, per-target values, prod target, permissions, email on failure
$ databricks bundle validate -t dev ; databricks bundle validate -t stage ; databricks bundle validate -t prod
$ databricks bundle deploy -t prod
$ databricks bundle summary -t prod
$ databricks bundle destroy -t prod --auto-approve
```
- [ ] ▶ The prod job has your `alert_email` under *Job notifications* and `users` CAN_VIEW under *Permissions*. Solution: `solutions/4.5_multi_env_dab`.

### 6.6 Lab 4.6 - Use a default DAB template
```
$ cd $env:TEMP                                         # outside the repo (PowerShell)
$ databricks bundle init default-python                # answer the prompts (serverless: yes)
$ cd my_project; Get-ChildItem -Recurse -Name          # databricks.yml, resources/, src/, tests/, pyproject.toml
$ databricks bundle validate ; databricks bundle deploy ; databricks bundle run <job-key> ; databricks bundle destroy --auto-approve
```
- [ ] Also look at `databricks bundle init` with no argument (the template list: `default-python`, `default-sql`, `default-minimal`, `dbt-sql`, `pydabs`, `mlops-stacks`, a custom template URL).
- [ ] Optional: `databricks bundle init pydabs` (resources defined in Python).

### 6.7 Demo 4.7 - CI/CD with DABs
Local CI steps (laptop, uses the `.venv` of this repo or any Python with PySpark + Java):
```
$ cd bundles/03_cicd_dab
$ pip install -e . pytest pyspark ; pytest -q          # CI step 1: unit tests on local Spark
$ python -m pip wheel --no-deps -w dist .              # CI step 2: build the artifact
$ databricks bundle validate -t stage                  # CI step 3: validate each target
$ databricks bundle deploy -t stage                    # CD: deploy (builds + uploads the wheel)
$ databricks bundle run -t stage orders_cicd_job       # integration test
$ databricks bundle destroy -t stage --auto-approve
```
**GitHub Actions** (the Next Steps lesson), on your own repo `wang-yuhao/databricks-de-associate-prep`:
1. ▶ Optional but recommended: *Settings → Identity and access → Service principals → Add new* (`cicd-deployer`), then its *Secrets* tab → **Generate secret**. Grant it `USE CATALOG` on `workspace` and `CREATE SCHEMA` on `workspace` (Catalog Explorer → *Permissions*). If Free Edition doesn't offer this, use a personal access token instead (*Settings → Developer → Access tokens*) and name the GitHub secret `DATABRICKS_TOKEN`.
2. GitHub → *Settings → Secrets and variables → Actions*: create `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (or `DATABRICKS_TOKEN`). Values go only into GitHub's secret store.
3. GitHub → *Settings → Environments*: create `stage` and `prod`; add yourself as required reviewer on `prod`.
4. Copy `bundles/03_cicd_dab/ci/github-actions/bundle-ci-cd.yml` to `.github/workflows/bundle-ci-cd.yml` at the repo root (paths are already repo-root relative). With a token instead of OAuth M2M, replace the two `DATABRICKS_CLIENT_*` env lines by `DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}`.
5. Open a PR that changes `src/orders_lib/transforms.py` → **ci** job runs tests + validate. Merge → **deploy-stage** deploys and runs. Publish a release → **deploy-prod** waits for your approval.
6. Optional: set `run_as: service_principal_name: <application-id>` in the prod target (the variable `service_principal` is prepared).
- [ ] After practicing, destroy stage/prod (`databricks bundle destroy -t prod --auto-approve`) and disable the workflow if you don't want it to run on every push.

### 6.8 Lab 4.8 - Adding ML to engineering workflows
```
$ cd labs/4.8_ml_workflow             # TODO 1-2 in resources/ml_job.yml: 2 tasks, 2 environments
$ databricks bundle validate
$ databricks bundle deploy
$ databricks bundle run orders_ml_job
$ databricks bundle destroy --auto-approve
```
- [ ] ▶ *Experiments*: the MLflow run with `r2_train`; ▶ *Catalog → workspace.orders_cicd_dev → Models*: `revenue_model` version 1 (also listed by `4.x-inspect-deployments`). Solution: `solutions/4.8_ml_workflow`.

### 6.9 Lesson 4.9 - Using VS Code with Databricks
Follow [`docs/4.9-using-vscode-with-databricks.md`](04-automated-deployment-dabs/docs/4.9-using-vscode-with-databricks.md): Databricks extension → *Configure* with your `DEFAULT` profile → open `bundles/02_multi_env_dab` → deploy and run from the *Bundle Resource Explorer* → Databricks Connect run of a file on serverless.
- [ ] Bonus: ▶ open `databricks.yml` of a bundle inside the workspace Git folder: the workspace can also validate and deploy bundles from the UI (*Deploy* button / *Bundle* panel), which is handy when the CLI is not at hand.

---

## 7. Coverage matrix
Every lesson of the four official outlines → the task that practices it on Free Edition.

### Course 1 - Advanced Techniques with Spark Declarative Pipelines
| Official lesson | Knowledge points practiced | Task |
|---|---|---|
| Intro to Multi Flows, Expectations and Liquid Clustering | flows, `append_flow`, per-flow checkpoints, warn/drop/fail, `expect_all*`, expectations on targets, quarantine, `cluster_by`, `cluster_by_auto` | 3.1 |
| Demo: Multi Flow SDP with LC and DQ | dry run, event log metrics, incremental update, add a source without refresh, fail + recover via config, full refresh, SQL syntax | 3.1 |
| Intro to Multiplex Streaming, Delta Sinks and Iceberg Reads | multiplex Bronze, fan-out in a loop, sinks (path/tableName, append only, no expectations, not cleaned on refresh), UniForm / Iceberg REST / managed Iceberg | 3.2 |
| Demo: Multiplex Streaming with Delta Sinks and Iceberg Reads | same, hands-on | 3.2 |
| CDC Review | CDC feed, ordering, late data, deletes, MERGE limitations | 3.3 step 5 |
| Demo: Automating SCD Type 2 with AUTO CDC | `create_auto_cdc_flow` / `AUTO CDC INTO`, SCD1 vs SCD2, `track_history_*`, tombstones, point-in-time queries | 3.3 |
| Advanced DQ Checks and Expectations | rules as data, null-safe rules, zero-loss quarantine, schema drift / `_rescued_data`, table-level checks, DQ trend | 3.4 |
| Demo: Advanced DQ | same, hands-on | 3.4 |
| Lab: Multi-Source E-commerce Pipeline | multi-flow ingest of CSV + JSON, AUTO CDC dimension, DQ + quarantine, LC, Gold MVs, incremental day 2 | 3.5 |

### Course 2 - Databricks Data Privacy
| Official lesson | Knowledge points practiced | Task |
|---|---|---|
| Regulatory Compliance · Data Privacy · Key Concepts and Components | GDPR/CCPA principles, PII vs quasi-identifiers, UC securables & inheritance | README + 4.1 steps 1-3 |
| Audit Your Data | `DESCRIBE HISTORY`, audit + lineage system tables, lineage UI, information_schema | 4.1 step 10 |
| Data Isolation | catalog per region/env, managed storage, workspace-catalog binding | 4.1 step 9 |
| Demo: Securing Data in UC | grants, tags, classification, row filters, column masks, dynamic views, ABAC | 4.1 |
| Pseudonymization & Anonymization · Summary & Best Practices | salted hashing, tokenization, encryption, masking, generalization, suppression, perturbation, aggregation, k-anonymity, l-diversity, secrets | 4.2 |
| Demo: PII Data Security | same, hands-on | 4.2 |
| Capturing Changed Data · Deleting Data in Databricks | CDF, `table_changes`, streaming CDF, delete propagation, deletion vectors, REORG PURGE, VACUUM, retention | 4.3 |
| Demo: Processing Records from CDF and Propagating Changes | same, hands-on + deletion requests | 4.3 |
| Lab: Propagating Changes with CDF | MERGE CDC, CDF batch + streaming, foreachBatch, physical delete | 4.4 |

### Course 3 - Databricks Performance Optimization
| Official lesson | Knowledge points practiced | Task |
|---|---|---|
| Spark UI Introduction | jobs/stages/tasks, narrow vs wide, plans, query profile, caching | §5, 3.1 |
| Introduction to Designing the Foundation · Demo: File Explosion | file sizing, partitioning rules, OPTIMIZE, optimized writes, auto compaction, predictive optimization | 3.2 |
| Data Skipping and Liquid Clustering · Lab | min/max stats, stats columns, Z-ORDER, LC, changing keys, CLUSTER BY AUTO | 3.3 |
| Shuffles · Demo: Shuffle | exchanges, SMJ vs BHJ, hints, shuffle partitions, AQE | 3.4 |
| Skew · Spill · Lab: Exploding Join | key distribution, explosion, AQE skew join, salting, broadcast, spill | 3.5 |
| Serialization · Demo: UDFs | Python vs Arrow vs pandas vs SQL UDF vs built-ins, pushdown | 3.6 |
| Choosing the Right Cluster · Pick the Best Instance Types | serverless vs classic, instance families, sizing from metrics | 3.7 |

### Course 4 - Automated Deployment with Declarative Automation Bundles
| Official lesson | Knowledge points practiced | Task |
|---|---|---|
| DevOps Review · CI/CD Review | CI vs CD, environments, testing | 6.1, 6.7 |
| Demo: Course Setup and Authentication · Deploying Databricks Projects | OAuth U2M, profiles, UI vs API vs CLI vs bundles | 6.1 |
| Intro to DABs · Demo: Deploying a Simple DAB | anatomy, validate / deploy / run / summary / destroy, state, idempotency | 6.2 |
| Lab: Deploying a Simple DAB | tasks, `depends_on`, job parameters | 6.3 |
| Variable Substitutions · Demo: Multiple Environments | variables, `--var`, `BUNDLE_VAR_*`, `${bundle.target}`, resource references, modes, root_path | 6.4 |
| Lab: Deploy a DAB to Multiple Environments | required variables, prod target, permissions, notifications | 6.5 |
| DAB Project Templates Overview · Lab: Default Template | `bundle init`, template catalog, pydabs | 6.6 |
| CI/CD Project Overview · Demo: CI/CD with DABs · CI/CD Best Practices | unit tests, wheel artifacts, serverless environments, stage integration run, service principal, approvals | 6.7 |
| Lab: Adding ML to Engineering Workflows | multi-task job, `spark_python_task`, environments, MLflow + UC model registry | 6.8 |
| Developing Locally with VS Code · Demo | extension, bundle explorer, Databricks Connect | 6.9 |
| Next Steps: Automated Deployment with GitHub Actions | workflow, secrets, environments, release-gated prod | 6.7 |

---

## 8. Suggested schedule (about 6 sessions)
| Session | Work |
|---|---|
| 1 | Setup (§1) + Course 1 lessons 1.1-1.2 |
| 2 | Course 1 lessons 1.3-1.5 |
| 3 | Course 2 (all) |
| 4 | Course 3 lessons 3.1-3.3 |
| 5 | Course 3 lessons 3.4-3.7 |
| 6 | Course 4 (all, CLI) + GitHub Actions |

## 9. Troubleshooting
| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: adelab` | run the notebook from the Git folder (not an imported copy) |
| Pipeline won't start: another update is running | Free Edition runs one pipeline at a time; wait or stop the other one |
| `Table or view not found: event_log` | publish the event log (§3.0 step 5) and run one update; until then the companions fall back to `event_log(TABLE(...))` |
| `TABLE_OR_VIEW_NOT_FOUND: bronze_events` (or another pipeline table) in a companion | the pipeline hasn't finished an update, or it publishes to another schema. The companion's first cell prints where it found the table (or that it found none); fix the pipeline's *Default schema* (e.g. `sdp_1_1`) or set the `schema` widget to the printed one |
| Pipeline fails on `UNIFORM` / Iceberg properties | set `enable_uniform` = `false` in the multiplex configuration |
| Lab pipeline fails with `NotImplementedError` | a file with an open TODO is in the source list; add files one step at a time |
| Counts in a CHECK are higher than expected | data was landed twice: reset the folder in `00-land` (*reset_folder* widget) and **Full refresh** the pipeline |
| `[CONFIG_NOT_AVAILABLE]` on `spark.conf.set` | that conf is not settable on serverless (see §1.5) |
| `VACUUM` with short retention refused | see the message in 2.3 step G; the files disappear after the default retention instead |
| `bundle deploy -t stage` complains about production mode | keep `workspace.root_path` set to `/Workspace/Shared/.bundle/...` (already in the bundles) |
| Compute stopped for the day | Free Edition quota; data and settings are kept |
