# Multi-Flow SDP with Liquid Clustering and Data Quality — Local Practice Lab

A self-contained, **free** local replica of the Databricks Academy demo
*"Multi Flow SDP with Liquid Clustering and Data Quality"* (from *Advanced Techniques with
Apache Spark Declarative Pipelines*). It runs on open-source **PySpark 3.5** and
**Delta Lake 3.2**, so you can practice without a Databricks workspace or cloud credits.

> This is a learning emulation, not the Databricks runtime. The Lakeflow / SDP orchestrator
> (`@dp.table`, `@dp.append_flow`, `@dp.expect…`) is reproduced with plain Python classes so the
> *concepts and behavior* match. The APIs themselves are different.

---

## Contents

1. [What you will learn](#1-what-you-will-learn)
2. [Architecture](#2-architecture)
3. [Concept mapping: Databricks → local](#3-concept-mapping-databricks--local)
4. [Project structure](#4-project-structure)
5. [Prerequisites](#5-prerequisites)
6. [Installation](#6-installation)
7. [Running the demo](#7-running-the-demo)
8. [Understanding the output](#8-understanding-the-output)
9. [Running the tests](#9-running-the-tests)
10. [Practice exercises](#10-practice-exercises)
11. [Troubleshooting](#11-troubleshooting)
12. [Known differences from Databricks](#12-known-differences-from-databricks)

---

## 1. What you will learn

| Topic | Where you practice it |
|---|---|
| **Multi-flow ingestion**: several independent sources appending into one streaming table | `MultiFlowPipeline.run_bronze()` |
| **Data quality expectations**: `expect` (warn), `expect_or_drop`, `expect_or_fail` | `DataQualityManager` |
| **Quarantine pattern**: keeping rejected rows with the reason they failed | `silver_quarantine` table |
| **Liquid Clustering**: `CLUSTER BY` instead of partitioning or Z-ORDER | `MultiFlowPipeline._create_silver_table()` |
| **Incremental processing**: checkpoints and `availableNow` triggers | Bronze and Silver streams |
| **Gold aggregation**: materialized-view style business metrics | `MultiFlowPipeline.run_gold()` |
| **Verifying table metadata** with `DeltaTable.detail()` and `DESCRIBE HISTORY` | `run_demo.py` |

---

## 2. Architecture

```
 source_flow_a/*.json ──(append flow A)──┐
                                         ├──► BRONZE  bronze_events        (streaming Delta table)
 source_flow_b/*.json ──(append flow B)──┘          │
                                                    │  DataQualityManager
                                                    │   • expect          amount >= 0            → warn, row kept
                                                    │   • expect_or_drop  event_ts IS NOT NULL   → row quarantined
                                                    │   • expect_or_drop  status IN (...)        → row quarantined
                                                    │   • expect_or_fail  event_id IS NOT NULL   → pipeline aborts
                                                    ▼
                         SILVER  silver_events      (Liquid Clustering on status, ingest_date)
                                 silver_quarantine  (rejected rows + failed_expectations[])
                                                    │
                                                    ▼
                         GOLD    gold_daily_metrics (count, distinct devices, sum/avg amount
                                                     per ingest_date × status)
```

---

## 3. Concept mapping: Databricks → local

| Databricks SDP / DLT | Local equivalent in this repo |
|---|---|
| `@dp.table` streaming table with multiple `@dp.append_flow` | One `readStream … writeStream` per source, all writing to the same Delta path, each with **its own checkpoint** |
| Auto Loader (`cloudFiles`) | `spark.readStream.json(dir)` with an explicit schema |
| `@dp.expect("name", "cond")` | `DataQualityManager.expect(...)` |
| `@dp.expect_or_drop(...)` | `DataQualityManager.expect_or_drop(...)` (rows sent to quarantine) |
| `@dp.expect_or_fail(...)` | `DataQualityManager.expect_or_fail(...)` (raises `ExpectationFailed`) |
| Pipeline event log: data quality metrics | `pipeline.dq.metrics` (one record per rule per micro-batch) |
| `CREATE … CLUSTER BY (status, ingest_date)` / `cluster_by=[…]` | `DeltaTable.createIfNotExists(...).clusterBy("status", "ingest_date")` |
| Automatic / predictive optimization | Explicit `OPTIMIZE` (`optimize().executeCompaction()`) after the Silver load |
| `@dp.materialized_view` | `run_gold()`: full recompute and overwrite |
| Pipeline update / full refresh | `python run_demo.py` (always starts from a clean state) |

---

## 4. Project structure

```
databricks-academy/
├── README.md                    ← this guide
├── requirements.txt             ← pinned pyspark / delta-spark / pytest
├── spark_session.py             ← local SparkSession with the Delta extension + catalog
├── pipeline.py                  ← MultiFlowPipeline + DataQualityManager (the core lesson)
├── run_demo.py                  ← end-to-end driver: clean → mock data → Bronze → Silver → Gold → verify
├── start_lab.ps1                ← launches the JupyterLab "Databricks workspace"
├── notebooks/
│   ├── Includes/Classroom-Setup.py   ← like the course's %run ./Includes/Classroom-Setup
│   ├── 1 - Demo - Multi Flow SDP with Liquid Clustering and Data Quality.ipynb
│   └── 2 - Lab - Practice Exercises.ipynb
├── tests/
│   └── test_data_quality.py     ← pytest suite for expectation behavior
└── tmp/                         ← created at runtime (git-ignorable)
    ├── source_flow_a/           ← mock JSON landing zone, flow A
    ├── source_flow_b/           ← mock JSON landing zone, flow B
    ├── lake/                    ← bronze_events, silver_events, silver_quarantine, gold_daily_metrics
    └── checkpoints/             ← streaming checkpoints (one per flow + silver)
```

> **Data location:** on macOS/Linux, data goes to `/tmp/...` as in the course. On Windows it
> goes to `.\tmp\` inside the project. Set the `DEMO_ROOT` environment variable to use another folder.

---

## 5. Prerequisites

| Requirement | Version | Check |
|---|---|---|
| Python | **3.9 – 3.11** (3.12 crashes PySpark 3.5 Python workers on Windows) | `py -3.11 --version` |
| Java JDK | 11 or 17 | `java -version` |
| Internet (first run only) | — | Downloads the Delta Lake JAR via Ivy |
| **Windows only:** `winutils.exe` + `hadoop.dll` | Hadoop 3.3.x | `echo $env:HADOOP_HOME` |

### Install Java 17

```powershell
# Windows
winget install EclipseAdoptium.Temurin.17.JDK
```
```bash
# macOS
brew install openjdk@17
# Ubuntu / Debian
sudo apt-get install -y openjdk-17-jdk
```

Open a **new terminal** afterwards and confirm that `java -version` prints 17.x. If Spark can't find Java, set `JAVA_HOME` to the JDK folder.

### Windows only: Hadoop winutils

Spark needs native Hadoop helpers to write files on Windows.

1. Download `winutils.exe` and `hadoop.dll` for **Hadoop 3.3.x** (for example from the
   `cdarlint/winutils` GitHub repository, folder `hadoop-3.3.5/bin`).
2. Put both files in `C:\hadoop\bin`.
3. Set the environment variables, then open a new terminal:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\hadoop\bin", "User")
```

---

## 6. Installation

```powershell
# Windows (PowerShell)
cd D:\projects\databricks-academy
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
```bash
# macOS / Linux
cd databricks-academy
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Pinned versions (keep PySpark and Delta in lockstep):

| Package | Version | Why |
|---|---|---|
| `pyspark` | 3.5.3 | Spark version that Delta 3.2 is built against |
| `delta-spark` | 3.2.1 | Liquid Clustering + `DeltaTable.clusterBy()` builder |
| `pytest` | 8.3.3 | Data quality test suite |
| `jupyterlab` | 4.2.5 | Databricks-style notebook workspace |
| `pandas` / `numpy` | 2.2.3 / 1.26.4 | Needed by `display()` (`toPandas`) |

Register the notebook kernel once (already done on this laptop):

```powershell
.\.venv\Scripts\python.exe -m ipykernel install --user --name databricks-academy --display-name "Python (databricks-academy)"
```

---

## 7. Running the demo

### 7.0 Notebook workspace (recommended, closest to Databricks)

```powershell
cd D:\projects\databricks-academy
powershell -ExecutionPolicy Bypass -File .\start_lab.ps1
```

JupyterLab opens in your browser on the `notebooks/` folder. Open
**`1 - Demo - Multi Flow SDP with Liquid Clustering and Data Quality.ipynb`**, make sure the
kernel (top right) is **Python (databricks-academy)**, and run the cells from top to bottom.

The first cell, `%run ./Includes/Classroom-Setup.py`, gives you the same tools a Databricks
classroom notebook has:

| Databricks | Local notebook |
|---|---|
| Cluster's preconfigured `spark` | `spark` (Delta-enabled, local) |
| `display(df)` | `display(df)`, which renders an HTML table |
| `%sql` cell | `%%sql` cell magic, which displays the last statement's result |
| `DA` helper (`DA.paths`, `DA.cleanup()`) | `DA.paths`, `DA.reset()`, `DA.land_new_files()`, `DA.register_tables()`, `DA.event_log()` |
| Catalog tables (`catalog.schema.table`) | Tables registered in schema `sdp_demo`, queryable by name |
| Pipeline event log (data quality) | `DA.event_log(pipe)` |
| Spark UI on the cluster | http://localhost:4040 (printed by the setup cell) |

The demo notebook walks through: raw data → Bronze multi-flow → Silver expectations and
quarantine → Liquid Clustering checks (`DESCRIBE DETAIL` / `HISTORY`) → Gold → incremental
file arrival → `ALTER TABLE … CLUSTER BY` → an `expect_or_fail` failure → cleanup.

Then try **`2 - Lab - Practice Exercises.ipynb`**: TODO cells with check cells below them.

> Running a notebook a second time? Restart the kernel first (**Kernel → Restart Kernel**), as you
> would detach and reattach in Databricks.

### 7.1 Script run (warn and drop rules fire)

```bash
python run_demo.py
```

What happens, step by step:

1. **Clean**: removes `source_flow_a`, `source_flow_b`, `lake`, and `checkpoints` from the data root.
2. **Generate dirty data**: 3 JSON files × 50 rows per flow, seeded for reproducible results:
   - ~8 % rows with `event_ts = null` → dropped by `valid_timestamp`
   - ~7 % rows with an invalid `status` (`UNKNOWN`, `""`, `active!`) → dropped by `valid_status`
   - ~5 % rows with a negative `amount` → **warned only**, so they stay in Silver
3. **Bronze**: two append flows stream into the same `bronze_events` table.
4. **Silver**: each micro-batch passes through the expectations. Clean rows go to the
   clustered `silver_events` table and rejected rows go to `silver_quarantine`. `OPTIMIZE` then clusters the data.
5. **Verify**: `DeltaTable.forPath(...).detail()` must show the clustering columns and the
   `clustering` table feature. The script asserts this.
6. **Gold**: daily metrics per status.

### 7.2 Failure run (`expect_or_fail`)

```bash
python run_demo.py --inject-fatal
```

This adds one row with `event_id = null` to flow B. The `valid_event_id` rule raises
`ExpectationFailed` and the Silver update stops with:

```
[FAIL] pipeline update aborted by expect_or_fail:
  ... expect_or_fail 'valid_event_id' violated by 1 row(s): event_id IS NOT NULL
```

This matches Databricks, where an `expect_or_fail` violation fails the whole pipeline update.

### 7.3 Custom data location

```powershell
$env:DEMO_ROOT = "D:\sdp-lab"; python run_demo.py      # PowerShell
```
```bash
DEMO_ROOT=/data/sdp-lab python run_demo.py            # bash
```

---

## 8. Understanding the output

| Section | What to look for |
|---|---|
| **Bronze counts** | Rows for both `flow_a` and `flow_b`, so two flows have written to one table |
| **Expectation metrics** | Per rule: `action` (warn/drop/fail), `sum(total)`, `sum(failed)` |
| **Row counts** | `bronze = silver + quarantine`; warned rows stay in silver |
| **Quarantine sample** | `failed_expectations` lists which drop rules each row broke |
| **`DeltaTable.detail()`** | `clusteringColumns = [status, ingest_date]`, `tableFeatures` contains `clustering` |
| **`DESCRIBE HISTORY`** | `CREATE TABLE` with clustering, then `WRITE`s, then `OPTIMIZE` |
| **Gold table** | One row per `ingest_date × status` with counts and amounts |

Expected verification line:

```
[verify] Liquid Clustering ACTIVE on ['status', 'ingest_date']; table features = [..., 'clustering', ...]
```

### Inspect the tables yourself

```python
from spark_session import build_spark
spark = build_spark()
root = "tmp/lake"   # or /tmp/lake on macOS/Linux

spark.read.format("delta").load(f"{root}/silver_events").show()
spark.read.format("delta").load(f"{root}/silver_quarantine").show(truncate=False)
spark.sql(f"DESCRIBE DETAIL delta.`{root}/silver_events`").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY delta.`{root}/silver_events`").show(truncate=False)
```

---

## 9. Running the tests

```bash
python -m pytest -v tests
```

| Test | Verifies |
|---|---|
| `test_drop_rules_split_clean_and_quarantine` | Drop rules route rows to quarantine and record which rule failed |
| `test_warn_rule_keeps_rows_but_records_metric` | `expect` keeps rows and still counts violations |
| `test_null_rule_result_counts_as_violation` | A rule that evaluates to NULL counts as a failure, as in SDP |
| `test_fail_rule_raises` | `expect_or_fail` raises `ExpectationFailed` |

---

## 10. Practice exercises

Work through these to apply what the demo teaches. Each one only needs changes to `pipeline.py` or `run_demo.py`.

1. **Add a third flow.** Create `source_flow_c` in `generate_mock_data()` and add it to
   `MultiFlowPipeline.flows`. Confirm that Bronze shows three `source_flow` values and that a third checkpoint appears.
2. **Incremental re-run.** Comment out `clean_workspace()`, add one new JSON file, and run again.
   Only the new file should be processed, because checkpoints track progress.
3. **Change cluster keys.** Liquid Clustering keys can be changed without rewriting data:
   ```python
   spark.sql(f"ALTER TABLE delta.`{pipe.silver_path}` CLUSTER BY (device_id)")
   ```
   Re-run `OPTIMIZE` and check `detail()` again.
4. **New expectation.** Add `expect_or_drop("plausible_amount", "amount < 10000")` and
   generate a few outliers. Watch them appear in the quarantine table.
5. **Promote a rule.** Change `non_negative_amount` from `expect` to `expect_or_drop`, then predict
   the new silver and quarantine counts before running.
6. **Data skipping.** Query `silver_events` with `WHERE status = 'ERROR'` and compare
   `numFiles` in `detail()` with the files read in the Spark UI (`http://localhost:4040`) during the query.
7. **Gold extension.** Add a second gold table that shows rejection rate per `source_flow`, using
   both `silver_events` and `silver_quarantine`.

---

## 11. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `java : command not found` / `JAVA_HOME is not set` | Install JDK 11/17 and open a new terminal |
| `HADOOP_HOME and hadoop.home.dir are unset` / `UnsatisfiedLinkError … NativeIO$Windows` | Windows winutils missing. See [Prerequisites](#windows-only-hadoop-winutils) |
| `Python worker failed to connect back` / `Python worker exited unexpectedly (crashed)` | Rebuild the venv with Python 3.11 (`py -3.11 -m venv .venv`) |
| `DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE` | A lake path was not in `file:/D:/...` form. `pipeline.py` already builds paths this way, so keep it if you add tables |
| `ERROR ShutdownHookManager … Failed to delete … spark-*` at exit | Harmless on Windows: Spark can't delete its own temp JARs. Ignore it |
| `ClassNotFoundException: io.delta…` | The first run must download the Delta JAR. Check internet or proxy settings |
| `AttributeError: … clusterBy` | `delta-spark` is too old. Reinstall from `requirements.txt` |
| `clusteringColumns` assertion fails | PySpark and Delta versions don't match. Use the pinned versions |
| Stale or odd results after editing code | Just re-run `run_demo.py`; it always cleans data and checkpoints first |

---

## 12. Known differences from Databricks

- **No declarative dependency graph.** Steps run in explicit order (`run_bronze → run_silver → run_gold`) instead of being inferred from decorators.
- **`.option("clusterBy", …)` is not used.** On Spark 3.5 the DataFrame writer ignores it.
  The supported open-source route is the `DeltaTable` builder or SQL `CLUSTER BY`, which is what this project uses.
- **Clustering is triggered manually** with `OPTIMIZE`. Databricks can do this automatically (predictive optimization, automatic clustering keys).
- **No Auto Loader** features such as schema inference/evolution or rescued data columns. A fixed schema is used instead.
- **Expectation metrics** are kept in memory (`pipeline.dq.metrics`) instead of the pipeline event log.
- **Not a full refresh engine.** `run_gold()` always recomputes; there is no incremental materialized-view refresh.

---

*For educational practice only. Databricks, Delta Lake, and Apache Spark are trademarks of their respective owners.*
