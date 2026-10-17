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
# MAGIC | spill, large joins / aggregations, GC time high | memory | **memory optimized**, fewer cores per GB |
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
# MAGIC | Autoscaling | variable load (not for streaming with steady load: use fixed size / enhanced autoscaling for pipelines) |
# MAGIC | Spot / preemptible workers | fault-tolerant batch, with on-demand driver |
# MAGIC | Cluster policies + pools | governance (limits, tags) and faster start for classic clusters |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exercise: same query, two serverless engines
# MAGIC 1. Run the cell below on **serverless notebook compute** and note the time.
# MAGIC 2. Open **SQL Editor**, select the **Serverless Starter Warehouse** (Free Edition: one 2X-Small warehouse) and run
# MAGIC    the same SQL (printed below). Open its query profile from *Query history*.
# MAGIC 3. Compare: start-up time, Photon usage (warehouse profile shows Photon operators), result caching on the second run.

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
# MAGIC For each scenario, choose instance family, worker count/size, autoscaling and runtime. Answers below.
# MAGIC 1. Nightly 2 TB join + aggregation, spill seen in the profile with 8 × 4-core workers.
# MAGIC 2. Hourly job running a Python UDF over 50 GB of JSON; tasks are CPU-bound, no spill.
# MAGIC 3. Interactive BI dashboards over 300 GB of Gold tables, many repeated queries.
# MAGIC 4. A streaming pipeline with steady 5k events/s.
# MAGIC
# MAGIC <details><summary>Suggested answers</summary>
# MAGIC
# MAGIC 1. Memory optimized, fewer larger workers (less shuffle over the network), fix skew/explosion first, job cluster, LTS + Photon.
# MAGIC 2. Compute optimized; better: replace the UDF with built-ins or a pandas UDF (3.6), then re-measure.
# MAGIC 3. Serverless SQL warehouse (Photon, result cache, disk cache, autoscaling for concurrency).
# MAGIC 4. Serverless pipeline / fixed-size compute (or enhanced autoscaling); avoid standard autoscaling flapping.
# MAGIC </details>
