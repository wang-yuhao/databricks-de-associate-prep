# Databricks notebook source
# MAGIC %md
# MAGIC # 3.7 Fine-Tuning: Choosing the Right Cluster and Instance Types (Databricks Free Edition)
# MAGIC **Lessons:** Fine-Tuning - Choosing the Right Cluster · Pick the Best Instance Types
# MAGIC
# MAGIC Lecture-only in the course. Free Edition is serverless-only, so you can't pick instance types; you can still
# MAGIC practice the **decision** and the **diagnosis**, and compare the two serverless compute types you do have.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Decision guide
# MAGIC | Workload symptom (query profile / Spark UI) | Bottleneck | Choose |
# MAGIC |---|---|---|
# MAGIC | task time ≈ executor CPU time, Python UDFs, heavy parsing | CPU | **compute optimized**, more cores |
# MAGIC | spill, large joins / aggregations, GC time high | memory | **memory optimized** (more GB of RAM per core) |
# MAGIC | shuffle-heavy, repeated reads of the same data | local disk / I/O | **storage optimized** (NVMe, disk cache) |
# MAGIC | balanced ETL | — | **general purpose** |
# MAGIC | deep learning | — | **GPU** |
# MAGIC | many tiny files, no skipping | layout, not compute | fix the layout first (3.2, 3.3) |
# MAGIC
# MAGIC | Choice | Prefer when |
# MAGIC |---|---|
# MAGIC | Serverless (jobs, notebooks, pipelines, SQL) | default: instant start, autoscaling, Photon, no tuning, pay per use |
# MAGIC | Latest **LTS** runtime + **Photon** | classic compute for SQL / DataFrame work |
# MAGIC | Job clusters (not all-purpose) | every scheduled production job |
# MAGIC | Fewer, larger workers | shuffle-heavy work (less network) |
# MAGIC | More, smaller workers | embarrassingly parallel work, fault isolation |
# MAGIC | Autoscaling | variable batch load (classic autoscaling scales down poorly for Structured Streaming: use fixed size, or pipelines with enhanced autoscaling) |
# MAGIC | Spot / preemptible workers | fault-tolerant batch; keep the driver on-demand (the driver is always the first, on-demand node) |
# MAGIC | Cluster policies + pools | governance (limits, tags) and faster start for classic clusters |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exercise: same query, two serverless engines
# MAGIC 1. Run the cell below on **serverless notebook compute** and note the time.
# MAGIC 2. Open **SQL Editor**, select your SQL warehouse (Free Edition: one 2X-Small serverless warehouse, usually named
# MAGIC    **Serverless Starter Warehouse**) and run the same SQL (printed below). Open its query profile from *Query history*.
# MAGIC 3. Compare: start-up time, Photon usage (warehouse profile shows Photon operators), result caching on the second run.
# MAGIC    The notebook timing below is wall-clock only (no Spark UI on serverless) and includes `collect()` to the client.

# COMMAND ----------

import time

SQL = """SELECT o.customer_id % 100 AS bucket, count(*) AS orders, round(sum(o.amount), 2) AS revenue
FROM workspace.perf_course.orders_31 o GROUP BY 1 ORDER BY 1"""
print(SQL)
try:
    t0 = time.time()
    spark.sql(SQL).collect()
    print(f"serverless notebook: {time.time() - t0:.2f} s")
except Exception as e:
    print("Run notebook 3.1 first to create orders_31:", str(e).splitlines()[0][:120])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exercise: size a classic cluster on paper
# MAGIC For each scenario, choose instance family, worker count/size, autoscaling and runtime. Answers in the next cell.
# MAGIC 1. Nightly 2 TB join + aggregation, spill seen in the profile with 8 × 4-core workers.
# MAGIC 2. Hourly job running a Python UDF over 50 GB of JSON; tasks are CPU-bound, no spill.
# MAGIC 3. Interactive BI dashboards over 300 GB of Gold tables, many repeated queries.
# MAGIC 4. A streaming pipeline with steady 5k events/s.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spoiler: suggested answers (try the exercise first)
# MAGIC 1. Memory optimized, fewer larger workers (less shuffle over the network), fix skew/explosion first, job cluster, LTS + Photon.
# MAGIC 2. Compute optimized; better: replace the UDF with built-ins or a pandas UDF (3.6), then re-measure. Photon does not speed up Python UDFs.
# MAGIC 3. Serverless SQL warehouse (Photon, result cache, disk cache, intelligent workload management, autoscaling clusters for concurrency).
# MAGIC 4. Serverless pipeline, or classic pipeline with enhanced autoscaling, or fixed-size compute; avoid classic cluster autoscaling for streaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - **Access modes:** *standard* (formerly *shared*: multi-user, UC isolation, Python/SQL/Scala) and *dedicated* (formerly *single user*: one user or group, needed for RDD APIs, R, ML runtime/GPU). Both support Unity Catalog; *no isolation shared* is legacy and not UC.
# MAGIC - **Job compute vs all-purpose:** scheduled production jobs run on job compute (created per run, terminated after, cheaper DBU rate) or serverless jobs; all-purpose is for interactive development.
# MAGIC - **Serverless** (notebooks, jobs, pipelines, SQL warehouses): fast start, automatic scaling and Photon, no instance/runtime tuning; limited Spark conf control.
# MAGIC - **LTS runtimes** get long-term support; pin production classic compute to the latest LTS. **Photon** accelerates SQL/DataFrame operators (not Python UDFs or RDD code).
# MAGIC - **Pools** keep idle, ready instances to cut cluster start and autoscale time; **cluster policies** restrict/default config (node types, max workers, tags, auto-termination) and control who can create compute.
# MAGIC - **Spot** workers cut cost for fault-tolerant batch; the driver stays on-demand. Streaming: prefer pipelines with **enhanced autoscaling** over classic autoscaling.
# MAGIC - **SQL warehouses:** serverless > pro > classic; serverless adds instant start and intelligent workload management; all use Photon; query result cache + local disk cache serve repeated queries.
