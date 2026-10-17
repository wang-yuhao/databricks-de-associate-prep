# Course 3: Databricks Performance Optimization

Find performance problems in the Spark UI and fix them at three levels: **physical layout** (files, skipping, clustering), **code** (shuffle, skew, spill, UDFs), and **compute** (cluster and instance choice).

**Prerequisites:** PySpark DataFrames and SQL, Delta Lake.

## Lesson map

| Official lesson | Practice here |
|---|---|
| Spark UI Introduction | [`notebooks/3.1-demo-spark-ui-introduction`](notebooks/3.1-demo-spark-ui-introduction.ipynb) |
| Introduction to Designing the Foundation · **Demo:** File Explosion | [`notebooks/3.2-demo-file-explosion`](notebooks/3.2-demo-file-explosion.ipynb) |
| Data Skipping and Liquid Clustering · **Lab:** Data Skipping and Liquid Clustering | [`notebooks/3.3-lab-data-skipping-and-liquid-clustering`](notebooks/3.3-lab-data-skipping-and-liquid-clustering.ipynb) · [`solutions/`](solutions/3.3-lab-data-skipping-and-liquid-clustering-solution.ipynb) |
| Shuffles · **Demo:** Shuffle | [`notebooks/3.4-demo-shuffle`](notebooks/3.4-demo-shuffle.ipynb) |
| Skew · Spill · **Lab:** Exploding Join | [`notebooks/3.5-lab-exploding-join`](notebooks/3.5-lab-exploding-join.ipynb) · [`solutions/`](solutions/3.5-lab-exploding-join-solution.ipynb) |
| Serialization · **Demo:** User-Defined Functions | [`notebooks/3.6-demo-user-defined-functions`](notebooks/3.6-demo-user-defined-functions.ipynb) |
| Fine-Tuning: Choosing the Right Cluster · Pick the Best Instance Types | [Key concepts](#fine-tuning-compute) |

## Measuring locally
The Spark UI runs at **http://127.0.0.1:24040** while a notebook kernel is alive. `adelab.perf` reads the same numbers through the UI's REST API, so every notebook can print a comparison table:

```python
from adelab import perf
r1 = perf.run(df_before, "before")          # executes with a no-op sink, returns time + metrics
r2 = perf.run(df_after, "after")
perf.compare(r1, r2)                         # seconds, files_read, bytes_read, join_output_rows, shuffle, spill, join strategy, skew
perf.stage_summary(spark)                    # per stage: tasks, median vs max task time (skew), spill
perf.table_files(spark, "my_table")          # numFiles, size, average file size, clustering columns
```

Laptop-scale data makes absolute times small and noisy. Compare **files read, shuffle bytes, spill, and skew ratios**, which are the same signals you read on a cluster. Labs lower some thresholds (target file size, skew and spill thresholds) so the effects show up on small data. Each place is marked in the notebook.

## Key concepts

### The 5 S's of Spark performance
| Problem | Symptom in the UI | Typical fixes |
|---|---|---|
| **Small files** (file explosion) | many tiny tasks, high *files read* | OPTIMIZE, optimized writes / auto compaction, predictive optimization, no over-partitioning |
| **Skipping** missing | files read ≈ all files for a selective filter | Liquid Clustering on filter columns, stats on the right columns |
| **Shuffle** | large shuffle read/write, many stages | broadcast joins, filter and prune early, AQE |
| **Skew** | max task time ≫ median | AQE skew join, salting, handle hot keys separately, broadcast |
| **Spill** | *Spill (memory/disk)* > 0 | smaller partitions, more memory per core, fix skew, better logic |
| (Serialization) | `BatchEvalPython`, slow Python stages | built-ins, SQL UDFs, pandas UDFs |

### Designing the foundation
- Target files of roughly 128 MB to 1 GB. Check `DESCRIBE DETAIL` (`numFiles`, `sizeInBytes`).
- **Partitioning**: only for very large tables, with low-cardinality columns that almost every query filters on, and ≥ 1 GB per partition.
- **Liquid Clustering** (`CLUSTER BY`, `CLUSTER BY AUTO`): the default for new tables. Incremental, keys can change, works for high-cardinality columns.
- **Data skipping**: Delta keeps min/max/null counts for the first 32 columns (`delta.dataSkippingNumIndexedCols`) or for the columns in `delta.dataSkippingStatsColumns`.

### Fine-tuning compute
- Prefer **serverless** or the latest **LTS runtime** with **Photon** for SQL/DataFrame workloads.
- Instance families: **general purpose** for balanced ETL, **memory optimized** for large joins, aggregations, caching and spill-heavy work, **compute optimized** for CPU-bound transformations and UDFs, **storage optimized** (local NVMe) for the Delta cache and shuffle-heavy work, **GPU** for deep learning.
- Few large workers vs many small: fewer, larger workers reduce shuffle across the network. Many small workers give more fault isolation. Autoscaling suits variable loads. Use job clusters, not all-purpose clusters, for production jobs.
- Size the cluster from the Spark UI: CPU-bound (task time ≈ executor run time) needs more cores; GC- or spill-bound needs more memory per core; I/O-bound needs better layout first.

## Run it on Databricks Free Edition
Step by step: [`../FREE_EDITION_GUIDE.md`](../FREE_EDITION_GUIDE.md) → Course 3. Serverless has no Spark UI: the notebooks in [`databricks/`](databricks) use the **query profile** (*See performance* under a cell), `explain()`, `DESCRIBE DETAIL` and join hints instead of Spark confs that serverless doesn't allow.

| Notebook | Lesson |
|---|---|
| `3.1-query-profile-and-spark-ui` | Spark UI introduction mapped to the query profile |
| `3.2-file-explosion` | small files, over-partitioning, OPTIMIZE, optimized writes, auto compaction |
| `3.3-lab-data-skipping-and-liquid-clustering` | lab; solution in `databricks/solutions/` |
| `3.4-shuffle` | exchanges, broadcast vs sort-merge, shuffle partitions, AQE |
| `3.5-lab-exploding-join` | lab: explosion, skew, salting, broadcast, spill; solution in `databricks/solutions/` |
| `3.6-user-defined-functions` | Python / Arrow / pandas / SQL UDFs vs built-ins |
| `3.7-choosing-compute` | cluster and instance-type decisions, serverless vs SQL warehouse |
