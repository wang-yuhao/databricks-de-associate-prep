# Advanced Data Engineering with Databricks: Local Practice Labs

Runnable demos and labs for the four Databricks Academy courses of the **Advanced Data Engineering with Databricks** series. They follow the official lesson outline, so you can watch a video and then do the same steps here, either for free on your laptop or on a Databricks workspace.

| # | Course | Folder | Notebooks |
|---|---|---|---|
| 1 | Advanced Techniques with Spark Declarative Pipelines | [`01-advanced-sdp`](01-advanced-sdp/README.md) | 4 demos, 1 lab, 1 bonus |
| 2 | Databricks Data Privacy | [`02-data-privacy`](02-data-privacy/README.md) | 3 demos, 1 lab |
| 3 | Databricks Performance Optimization | [`03-performance-optimization`](03-performance-optimization/README.md) | 4 demos, 2 labs |
| 4 | Automated Deployment with Declarative Automation Bundles | [`04-automated-deployment-dabs`](04-automated-deployment-dabs/README.md) | 4 demos, 4 labs |

Each course README maps every lesson of the official outline to its notebook, pipeline, or bundle, and lists the key concepts of the lecture-only lessons.

---

## 1. How it works

Each notebook starts with the equivalent of the course's `%run ./Includes/Classroom-Setup-X.Y`:

```python
from adelab.classroom import setup
DA = setup("1.1", reset=True)      # creates spark, display(), dbutils.fs, DA, and the %sql / %%sql magics
```

`adelab` is the local runtime in this folder. It provides:

| Databricks feature | Local equivalent | Fidelity |
|---|---|---|
| Cluster `spark` session | Spark **4.2** + Delta Lake **4.4** (`adelab.spark`) | Real engine (DBR 17+ is Spark 4.x) |
| Delta: Liquid Clustering, CDF, deletion vectors, UniForm, OPTIMIZE / VACUUM / REORG | Delta Lake OSS 4.4 | Real |
| **Spark Declarative Pipelines** (`from pyspark import pipelines as dp`) | `adelab.sdp` engine, `DA.pipeline(...).run()` | Same source code as on Databricks. Expectations, AUTO CDC (SCD1/2), sinks, multi-flow, event log, and full refresh are emulated |
| Open-source SDP (`spark-pipelines` CLI) | `adelab.oss_sdp` (bonus lesson 1.6) | Real Apache Spark engine |
| Auto Loader (`cloudFiles`) | Emulated over the file streaming source, incl. schema hints, schema location, `_rescued_data` | Close |
| Iceberg reads of UniForm tables | PyIceberg reads the Iceberg metadata written by Delta | Real |
| Unity Catalog row filters / column masks / `is_account_group_member` | SQL UDFs + dynamic views, simulated personas (`adelab.privacy.use_identity`) | Same SQL functions. Policies attached via views |
| Spark UI | http://127.0.0.1:24040, plus `adelab.perf` reading the same metrics via its REST API | Real |
| `display()`, `%sql`, `dbutils.fs` | `adelab.notebook`, `adelab.classroom` | Close |
| Declarative Automation Bundles | Real Databricks CLI. Offline: `adelab.bundles.preview()` and schema validation | Real with a workspace |

Not available locally: GRANTs, system tables (`system.access.audit`), serverless, Photon, predictive optimization. The notebooks show the Databricks SQL for these, and `databricks/` folders contain workspace-ready versions.

---

## 2. Setup (one time, about 10 minutes)

### Prerequisites

| Requirement | Check | Install |
|---|---|---|
| Python **3.11** | `py -3.11 --version` | python.org or `winget install Python.Python.3.11` |
| Java **17** (or 21) | `java -version` | `winget install EclipseAdoptium.Temurin.17.JDK`, then set `JAVA_HOME` |
| Windows only: `winutils.exe` + `hadoop.dll` for Hadoop 3.3.x | `echo $env:HADOOP_HOME` | Put both files in `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`, and add `%HADOOP_HOME%\bin` to `Path` |
| Databricks CLI (Course 4) | `databricks --version` | `winget install Databricks.DatabricksCLI` (or `brew install databricks`) |
| Internet on first run | | Downloads the Delta Lake jars once (about 40 MB, cached) |

### Install

```powershell
cd advanced-data-engineering
powershell -ExecutionPolicy Bypass -File .\setup\setup_env.ps1       # Windows
```
```bash
cd advanced-data-engineering && bash setup/setup_env.sh               # macOS / Linux
```

The script creates `.venv`, installs the pinned packages (`requirements.txt`) and `adelab` (editable), registers the **Python (adelab)** Jupyter kernel, downloads the Delta jars, and ends with the environment check (`python -m adelab.doctor`):

```
[OK  ] Python - 3.11.x
[OK  ] Java - openjdk version "17.0.x"
[OK  ] HADOOP_HOME - C:\hadoop
[OK  ] Spark UI port - 24040 is free (4040, Spark's default, is reserved by Windows on this machine)
[OK  ] Databricks CLI - Databricks CLI v1.21.0
[OK  ] Spark + Delta + pandas UDF - Spark 4.2.0, Delta write/read 10 rows, Liquid Clustering OK, ...
```

### Start the lab

```powershell
powershell -ExecutionPolicy Bypass -File .\setup\start_lab.ps1
```

JupyterLab opens on this folder. Open a notebook (e.g. `01-advanced-sdp/notebooks/1.1-...ipynb`), check that the kernel (top right) is **Python (adelab)**, and run the cells top to bottom. VS Code works too: open the folder and select the `.venv` interpreter or the *Python (adelab)* kernel.

### Databricks Free Edition track

Every course also has a workspace version for **Databricks Free Edition** (free, serverless): native notebooks in each course's `databricks/` folder, the Course 1 pipelines run as real Lakeflow pipelines, and Course 4 deploys with the CLI. Follow **[`FREE_EDITION_GUIDE.md`](FREE_EDITION_GUIDE.md)**: setup, the step-by-step task list for every lesson, what differs from the videos on Free Edition, and a coverage matrix of all official lessons.

---

## 3. Working through a course

1. Read the course README: lesson map plus key concepts of the lecture-only lessons.
2. Watch the lecture or demo video.
3. Open the matching notebook and run it cell by cell. Read the markdown cells, they explain what the video shows.
4. **Labs**: notebooks named `*-lab-*`. Complete `TODO` / `<FILL_IN>` items and run the `CHECK` cells. Solutions are in each course's `solutions/` folder.
5. Every notebook ends with `DA.cleanup()`. `setup(..., reset=True)` at the top gives you a clean start on every run.
6. **Diagrams**: the notebooks include original diagrams for each step (in each course's `images/` folder). To keep a slide from the course video next to them, save your own screenshot in `<course>/slides/` (git-ignored, because course material is copyrighted) and show it in any cell with `DA.show_slide("1.1-multi-flow.png")`.

Data lives in `.lab-data/` (git-ignored): one schema per lesson (`sdp_1_1`, `privacy_2_3`, ...) under `.lab-data/warehouse/`, and landing files and checkpoints under `.lab-data/courses/`. Delete the folder to start completely fresh.

---

## 4. `adelab` quick reference

| Call | Purpose |
|---|---|
| `DA = setup("X.Y", reset=False)` | Classroom setup. `DA.schema_name`, `DA.paths.landing`, `DA.paths.checkpoints`, `DA.course_dir` |
| `DA.pipeline(name, source, configuration)` | A pipeline over source files or folders (relative to the course folder) |
| `pipe.validate()` / `pipe.run()` / `pipe.run(full_refresh=True)` | Dry run / start an update / full refresh |
| `pipe.event_log()`, `pipe.expectation_metrics()` | Event log and data-quality metrics |
| `pipe.maintenance()` | `OPTIMIZE` all pipeline tables (clusters Liquid Clustering tables) |
| `DA.show_source(path)` | Show pipeline or bundle files with syntax highlighting |
| `adelab.datagen.*` | Deterministic course datasets |
| `adelab.perf.run(df, label)`, `compare(...)`, `stage_summary(spark)`, `table_files(spark, t)` | Time a query and read Spark UI metrics: files read, shuffle, spill, join output, skew |
| `adelab.privacy.use_identity(spark, "eu_analyst")`, `scan_pii(df)`, `k_anonymity(df, cols)` | Course 2 helpers |
| `adelab.iceberg.load_table(spark, t)` | Read a UniForm table as Iceberg (PyIceberg) |
| `adelab.bundles.cli(...)`, `preview(dir, target)`, `validate_schema(dir)`, `workspace_status()` | Course 4 helpers |
| `adelab.oss_sdp.run(project, name, conf)` | Run the open-source `spark-pipelines` engine |

---

## 5. Troubleshooting

| Symptom | Fix |
|---|---|
| `JAVA_GATEWAY_EXITED`, or `java` not found | Install JDK 17, set `JAVA_HOME`, open a **new** terminal / restart JupyterLab |
| `HADOOP_HOME and hadoop.home.dir are unset`, `UnsatisfiedLinkError ... NativeIO$Windows` | `winutils.exe` / `hadoop.dll` missing. See Prerequisites |
| `Failed to bind SparkUI ... 4056` | Windows (Hyper-V/WSL/Docker) reserves port ranges. `adelab` uses port 24040. If that range is also reserved (`netsh int ipv4 show excludedportrange protocol=tcp`), set `ADELAB_SPARK_UI_PORT` to a free port |
| Kernel hangs at setup, log full of `BlockManagerId ... is null` | A stray Spark JVM is still running: `.\setup\stop_spark.ps1`, then restart the kernel |
| `Python worker exited unexpectedly (crashed)` in pandas UDFs | Another project's interpreter in `PYSPARK_PYTHON` (machine-wide on this laptop: `stock-quant-project/.venv311`). `adelab` overrides it for its own sessions; if you build a SparkSession yourself, set `os.environ["PYSPARK_PYTHON"] = sys.executable` first |
| A row-at-a-time Python UDF (`@udf`, also `useArrow=True`) on a Parquet/Delta table runs until `TimeoutError: timed out` | PySpark 4.2 on Windows: these UDFs stall when fed directly by a parallel vectorized scan. Use a pandas UDF or built-ins (better anyway), materialize the input first (`df.localCheckpoint()`), or set `spark.sql.parquet.enableVectorizedReader=false` for that query |
| `ClassNotFoundException: io.delta...` / jar resolution fails | First run needs internet (Maven Central). Delete `.lab-data/jars/manifest-*.txt` to resolve again |
| `ERROR ShutdownHookManager ... Failed to delete ...spark-*` when a process exits | Harmless on Windows (Spark can't delete jars it still has open) |
| Strange state after editing pipeline code | `pipe.run(full_refresh=True)`, or `setup("X.Y", reset=True)` |
| Pipeline update is slow (30-90 s) | Normal locally: every flow is a separate streaming query with Delta commits |
| Notebooks get slow or the kernel dies when several are open | Every running kernel starts its own Spark JVM (4 GB driver). Shut down the kernels you're not using (*Kernel → Shut Down Kernel*, or the *Running* tab in JupyterLab). A second session's Spark UI moves to the next free port (24041, ...) |
| The kernel *Python (adelab)* is missing in JupyterLab / VS Code | `.\.venv\Scripts\python.exe -m ipykernel install --user --name adelab --display-name "Python (adelab)"`, then reload the page or VS Code window |
| `py -3.11` not found during setup | Install Python 3.11 (see Prerequisites) and open a new terminal. The script stops with the failing step's name |
| A pipeline update fails with `expectations cannot be defined on @dp.append_flow` | Same rule as on Databricks: declare the expectations on the target (`dp.create_streaming_table(..., expect_all_or_drop={...})`) |

Run `.\.venv\Scripts\python.exe -m adelab.doctor` at any time to recheck the environment.

---

## 6. Tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q            # adelab runtime: engine, expectations, AUTO CDC, Auto Loader, helpers
```
