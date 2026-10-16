# Day 8 Practice Tasks — Cost & Performance Optimisation

> **Exam section:** Cost & Performance Optimisation (13%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last.
Every task has:
- 📖 **Context** — why this matters for the exam
- 🛠️ **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- ⚠️ **Exam trap** — a common wrong-answer pitfall

---

## Task 1 — AQE Performance Comparison

📖 **Context:**
Adaptive Query Execution (AQE) is tested heavily in the Professional exam. You must understand when AQE improves performance and when it doesn't.

🛠️ **Instructions:**

**Step 1 — Create a large dataset for testing:**
```python
# Run in a Databricks notebook cell
from pyspark.sql.functions import col, rand, expr

# Create 10M row dataset with skewed join key
df_large = (
    spark.range(10_000_000)
    .withColumn("join_key", 
        expr("CASE WHEN id % 100 = 0 THEN 1 ELSE id % 1000 END"))
    .withColumn("value", rand() * 1000)
)

df_large.write.format("delta").mode("overwrite").saveAsTable("training.prep.large_table")

df_small = spark.range(1000).withColumn("metadata", expr("CONCAT('info_', id)"))
df_small.write.format("delta").mode("overwrite").saveAsTable("training.prep.small_table")
```

**Step 2 — Run join WITHOUT AQE:**
```python
# Disable AQE
spark.conf.set("spark.sql.adaptive.enabled", "false")

# Run join and time it
import time
start = time.time()

result_no_aqe = spark.sql("""
    SELECT l.id, l.join_key, l.value, s.metadata
    FROM training.prep.large_table l
    JOIN training.prep.small_table s ON l.join_key = s.id
""")

count_no_aqe = result_no_aqe.count()
time_no_aqe = time.time() - start

print(f"Without AQE: {count_no_aqe} rows in {time_no_aqe:.2f}s")
```

**Step 3 — Run join WITH AQE:**
```python
# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

start = time.time()

result_aqe = spark.sql("""
    SELECT l.id, l.join_key, l.value, s.metadata
    FROM training.prep.large_table l
    JOIN training.prep.small_table s ON l.join_key = s.id
""")

count_aqe = result_aqe.count()
time_aqe = time.time() - start

print(f"With AQE: {count_aqe} rows in {time_aqe:.2f}s")
print(f"Improvement: {((time_no_aqe - time_aqe) / time_no_aqe * 100):.1f}%")
```

**Step 4 — Examine the execution plan:**
```python
result_aqe.explain("formatted")
```

✅ **Expected outcome:**
- AQE should be 20-50% faster due to dynamic skew handling
- In the AQE plan, you should see `AdaptiveSparkPlan` and possibly `BroadcastHashJoin` or `ShuffleHashJoin`
- Look for `SkewJoin` in the plan if skew was detected

⚠️ **Exam trap:**
Exam questions may ask "when does AQE NOT help?" Answer: streaming queries, very small datasets, queries without joins/aggregations.

---

## Task 2 — Broadcast Join Threshold Tuning

📖 **Context:**
The Professional exam tests understanding of when broadcast joins are beneficial vs harmful (OOM errors).

🛠️ **Instructions:**

**Step 1 — Check current broadcast threshold:**
```python
threshold_mb = int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")) / (1024 * 1024)
print(f"Current broadcast threshold: {threshold_mb}MB")
```

**Step 2 — Create a medium table (15MB):**
```python
# Create ~15MB table
df_medium = spark.range(1_000_000).selectExpr(
    "id",
    "CONCAT('data_', id) as col1",
    "CONCAT('more_data_', id, '_padding') as col2"
)

df_medium.write.format("delta").mode("overwrite").saveAsTable("training.prep.medium_table")

# Check size
size_bytes = spark.sql("DESCRIBE DETAIL training.prep.medium_table").select("sizeInBytes").first()[0]
size_mb = size_bytes / (1024 * 1024)
print(f"Table size: {size_mb:.2f}MB")
```

**Step 3 — Force broadcast join and observe:**
```python
from pyspark.sql.functions import broadcast

# Explicit broadcast hint
df_result = (
    spark.table("training.prep.large_table")
    .join(broadcast(spark.table("training.prep.medium_table")), "id")
)

df_result.explain()
# Look for "BroadcastHashJoin" in the plan
```

**Step 4 — Test threshold tuning:**
```python
# Set threshold to 20MB (above our 15MB table)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")  # 20MB in bytes

df_auto_broadcast = spark.sql("""
    SELECT l.id, l.value, m.col1
    FROM training.prep.large_table l
    JOIN training.prep.medium_table m ON l.id = m.id
""")

df_auto_broadcast.explain()
# Should see BroadcastHashJoin automatically
```

✅ **Expected outcome:**
- With `broadcast()` hint: always see BroadcastHashJoin
- With threshold = 20MB: automatic broadcast for 15MB table
- With threshold = 10MB: would use SortMergeJoin instead

⚠️ **Exam trap:**
Setting threshold too high causes driver OOM. Exam may ask "what happens if broadcast table is too large?" Answer: Driver OOM error, not executor OOM.

---

## Task 3 — Caching Strategy Comparison

📖 **Context:**
Understand the difference between Spark Cache (executor memory) and Delta Cache (SSD), and when to use each.

🛠️ **Instructions:**

**Step 1 — Test Spark cache:**
```python
# Clear any existing cache
spark.catalog.clearCache()

# Cache a DataFrame
df_to_cache = spark.table("training.prep.large_table")
df_to_cache.cache()

# Trigger caching
df_to_cache.count()

# Second query should be faster
import time
start = time.time()
df_to_cache.filter("value > 500").count()
time_cached = time.time() - start
print(f"Query on cached data: {time_cached:.2f}s")

# Unpersist
df_to_cache.unpersist()
```

**Step 2 — Test Delta Cache (if on Photon cluster):**
```python
# Delta Cache is automatic on Photon-enabled clusters
# Just query the Delta table multiple times

start = time.time()
df_delta = spark.sql("SELECT * FROM training.prep.large_table WHERE value > 500")
count1 = df_delta.count()
time_first = time.time() - start

start = time.time()
df_delta2 = spark.sql("SELECT * FROM training.prep.large_table WHERE value > 500")
count2 = df_delta2.count()
time_second = time.time() - start

print(f"First query: {time_first:.2f}s")
print(f"Second query (Delta Cache): {time_second:.2f}s")
print(f"Speedup: {(time_first / time_second):.1f}x")
```

**Step 3 — Explicit Delta table caching:**
```python
spark.sql("CACHE SELECT * FROM training.prep.large_table")

# Or
spark.catalog.cacheTable("training.prep.large_table")

# Verify
cached_tables = spark.sql("SHOW TABLES IN training.prep").filter("isTemporary = true").collect()
print(f"Cached tables: {[row.tableName for row in cached_tables]}")
```

✅ **Expected outcome:**
- Spark cache: 2x-5x speedup for repeated queries
- Delta cache: automatic speedup on second query (if Photon enabled)
- Cached table appears in Spark UI Storage tab

⚠️ **Exam trap:**
Exam may ask "what happens to Spark cache when executor dies?" Answer: cache is lost. Delta Cache persists across executor restarts.

---

## Task 4 — Partition Tuning with Repartition vs Coalesce

📖 **Context:**
Professional exam tests when to use `repartition()` (full shuffle) vs `coalesce()` (no shuffle).

🛠️ **Instructions:**

**Step 1 — Check current partitioning:**
```python
df = spark.table("training.prep.large_table")
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# Write to see file count
df.write.format("delta").mode("overwrite").save("/tmp/test_partitions")

files = dbutils.fs.ls("/tmp/test_partitions")
parquet_files = [f for f in files if f.name.endswith(".parquet")]
print(f"Parquet files written: {len(parquet_files)}")
```

**Step 2 — Test coalesce (reduce partitions without shuffle):**
```python
df_coalesced = df.coalesce(10)
print(f"After coalesce: {df_coalesced.rdd.getNumPartitions()} partitions")

df_coalesced.write.format("delta").mode("overwrite").save("/tmp/test_coalesce")
files = dbutils.fs.ls("/tmp/test_coalesce")
parquet_files = [f for f in files if f.name.endswith(".parquet")]
print(f"Files written: {len(parquet_files)}")
```

**Step 3 — Test repartition (full shuffle):**
```python
df_repartitioned = df.repartition(20)
print(f"After repartition: {df_repartitioned.rdd.getNumPartitions()} partitions")

df_repartitioned.write.format("delta").mode("overwrite").save("/tmp/test_repartition")
files = dbutils.fs.ls("/tmp/test_repartition")
parquet_files = [f for f in files if f.name.endswith(".parquet")]
print(f"Files written: {len(parquet_files)}")
```

**Step 4 — Partition by column:**
```python
# Repartition by a column for co-located data
df_by_key = df.repartition(50, "join_key")
print(f"Partitions: {df_by_key.rdd.getNumPartitions()}")

df_by_key.write.format("delta").mode("overwrite").partitionBy("join_key").save("/tmp/test_partition_by")
```

✅ **Expected outcome:**
- `coalesce(10)`: reduces partitions to 10, no shuffle
- `repartition(20)`: exactly 20 partitions, full shuffle
- `repartition(50, "join_key")`: 50 partitions, data with same key in same partition

⚠️ **Exam trap:**
`coalesce()` can only REDUCE partitions. Trying `coalesce(100)` when you have 50 partitions does nothing. Use `repartition()` to increase.

---

## Task 5 — Shuffle Partition Tuning

📖 **Context:**
Default shuffle partitions (200) is often wrong. Professional exam tests optimal sizing.

🛠️ **Instructions:**

**Step 1 — Check current shuffle partitions:**
```python
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
```

**Step 2 — Run aggregation with default (200):**
```python
import time

df = spark.table("training.prep.large_table")

start = time.time()
result_default = df.groupBy("join_key").count()
count_default = result_default.count()
time_default = time.time() - start

print(f"With 200 partitions: {time_default:.2f}s")
print(f"Partitions in result: {result_default.rdd.getNumPartitions()}")
```

**Step 3 — Tune to optimal size:**
```python
# Calculate optimal partitions
# Rule: target 128MB per partition
# 10M rows * ~50 bytes = 500MB total
# 500MB / 128MB = ~4 partitions optimal

spark.conf.set("spark.sql.shuffle.partitions", "8")

start = time.time()
result_tuned = df.groupBy("join_key").count()
count_tuned = result_tuned.count()
time_tuned = time.time() - start

print(f"With 8 partitions: {time_tuned:.2f}s")
print(f"Speedup: {(time_default / time_tuned):.1f}x")
```

**Step 4 — Let AQE handle it automatically:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")  # reset
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128MB

start = time.time()
result_aqe = df.groupBy("join_key").count()
count_aqe = result_aqe.count()
time_aqe = time.time() - start

print(f"With AQE auto-tuning: {time_aqe:.2f}s")
print(f"Final partitions: {result_aqe.rdd.getNumPartitions()}")
```

✅ **Expected outcome:**
- Default 200 partitions: slower (too many small tasks)
- Tuned 8 partitions: 1.5-3x faster
- AQE auto-tuning: automatically reduces to ~8-10 partitions

⚠️ **Exam trap:**
Exam may ask "how to calculate optimal shuffle partitions?" Answer: `total_data_size / 128MB`. But with AQE enabled, manual tuning isn't needed.

---

## Task 6 — Query Profiling with EXPLAIN

📖 **Context:**
EXPLAIN output is critical for debugging performance. Exam tests ability to read plans.

🛠️ **Instructions:**

**Step 1 — Run EXPLAIN on a complex query:**
```sql
%sql
EXPLAIN FORMATTED
SELECT 
    l.join_key,
    COUNT(*) as total_count,
    AVG(l.value) as avg_value
FROM training.prep.large_table l
JOIN training.prep.medium_table m ON l.id = m.id
WHERE l.value > 100
GROUP BY l.join_key
ORDER BY total_count DESC
LIMIT 10;
```

**Step 2 — Identify key plan elements:**

Look for:
- **FileScan** with `PushedFilters: [IsNotNull(value), GreaterThan(value,100)]` ← filter pushdown working
- **BroadcastHashJoin** or **SortMergeJoin** ← join strategy
- **Exchange** ← shuffle operations (expensive)
- **Sort** ← sorting overhead
- **TakeOrderedAndProject** ← LIMIT optimization

**Step 3 — Use Python API:**
```python
df_query = spark.sql("""
    SELECT 
        l.join_key,
        COUNT(*) as total_count,
        AVG(l.value) as avg_value
    FROM training.prep.large_table l
    JOIN training.prep.medium_table m ON l.id = m.id
    WHERE l.value > 100
    GROUP BY l.join_key
    ORDER BY total_count DESC
    LIMIT 10
""")

# Different explain modes
df_query.explain()  # physical plan
df_query.explain("extended")  # logical + physical
df_query.explain("cost")  # with cost estimates
df_query.explain("formatted")  # human-readable
```

✅ **Expected outcome:**
- You should see `PushedFilters` showing predicate pushdown to Parquet
- Join strategy should be visible (Broadcast or SortMerge)
- `Exchange` nodes show where shuffles happen

⚠️ **Exam trap:**
Exam may show a plan and ask "why is this slow?" Look for: missing filter pushdown, SortMergeJoin instead of Broadcast, too many Exchange nodes.

---

## Task 7 — Cost Analysis Exercise

📖 **Context:**
Professional exam includes scenario questions about cluster cost optimization.

🛠️ **Instructions:**

**Step 1 — Analyze current cluster configuration:**

Document your cluster:
- Node type: _______________
- Number of workers: _______________
- DBR version: _______________
- Photon enabled: Yes / No
- Auto-termination: _______________ minutes

**Step 2 — Calculate hourly cost:**

Use Azure Pricing Calculator or check cluster details:
- VM cost/hour: €________
- DBU cost/hour: €________ (Premium DBU ≈ €0.60/DBU)
- Total workers: ________
- **Total cost/hour = (VM cost + DBU cost) × workers**

**Step 3 — Optimization recommendations:**

For a batch ETL job running 1 hour/day:

| Strategy | Savings | Trade-off |
|----------|---------|----------|
| Use Spot/Preemptible instances | 70-90% VM cost | Can be evicted |
| Use Job cluster instead of All-Purpose | 50% DBU cost | Not for interactive work |
| Reduce idle time (auto-terminate after 5 min) | Saves idle charges | Slower restart |
| Enable auto-scaling (min 1, max 3) | Scales down when idle | May scale slowly |
| Use instance pools | Faster start (no savings) | Pre-warmed instances |

✅ **Expected outcome:**
You can explain when each cost optimization makes sense for different workload types.

⚠️ **Exam trap:**
Spot instances are NOT recommended for: interactive analysis, critical production workloads with SLAs, jobs that can't tolerate interruption.

---

## Task 8 — Photon Performance Test

📖 **Context:**
Photon is tested in Professional exam. Understand when it helps most.

🛠️ **Instructions:**

**Step 1 — Check if Photon is enabled:**
```python
version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
print(f"Runtime: {version}")

if "photon" in version.lower():
    print("✅ Photon is enabled")
else:
    print("❌ Photon is NOT enabled")
```

**Step 2 — Test on Photon-friendly workload:**
```sql
%sql
-- Aggregations and joins benefit most from Photon
SELECT 
    join_key,
    COUNT(*) as cnt,
    SUM(value) as total,
    AVG(value) as avg,
    MAX(value) as max_val,
    MIN(value) as min_val
FROM training.prep.large_table
WHERE value > 100
GROUP BY join_key
ORDER BY cnt DESC;
```

Check Spark UI for "Photon" stages.

**Step 3 — Identify Photon in execution plan:**
```python
df = spark.sql("""
    SELECT join_key, COUNT(*) as cnt
    FROM training.prep.large_table
    GROUP BY join_key
""")

df.explain("formatted")
# Look for: PhotonGroupingAgg, PhotonProject, PhotonFilter
```

✅ **Expected outcome:**
- On Photon cluster: see `PhotonGroupingAgg`, `PhotonScan` in plan
- 2-10x speedup for SQL/Delta operations
- Non-Photon: standard `HashAggregate`, `FileScan`

⚠️ **Exam trap:**
Photon does NOT help: Python UDFs, RDD operations, custom JVM libraries. It's for SQL/DataFrame operations only.

---

## Concept Quiz (Self-Check)

Answer these without looking at notes:

1. What three things does AQE dynamically optimize at runtime?
2. When does broadcast join cause OOM errors: driver or executor?
3. What is the difference between Spark Cache and Delta Cache?
4. When should you use `coalesce()` vs `repartition()`?
5. What is the default `spark.sql.shuffle.partitions` value?
6. How do you calculate optimal shuffle partitions for a 1GB dataset?
7. What does `PushedFilters` in EXPLAIN output indicate?
8. What cost optimization has no downside for batch jobs?
9. Can Photon accelerate Python UDF execution?
10. What cluster policy setting prevents accidentally leaving clusters running?

**Answers:**
1. Shuffle partition count, join strategy, skew handling
2. Driver OOM (broadcast table is sent to driver first)
3. Spark Cache = executor memory/disk; Delta Cache = SSD raw files, survives restarts
4. `coalesce()` to reduce partitions without shuffle; `repartition()` to increase or rebalance
5. 200
6. 1000MB / 128MB ≈ 8 partitions
7. Filters pushed down to Parquet file scan (predicate pushdown working)
8. Auto-termination after idle time
9. No — Photon is C++ engine for SQL/DataFrames, not Python
10. Auto-termination setting (e.g., 30 minutes)

---

## Key Takeaways for Exam

✅ **AQE is enabled by default in DBR 7.3+** — know the three optimizations
✅ **Broadcast threshold default is 10MB** — configurable via `spark.sql.autoBroadcastJoinThreshold`
✅ **Delta Cache is automatic on Photon** — no code needed, just run queries twice
✅ **`repartition()` = shuffle; `coalesce()` = no shuffle** — exam loves this distinction
✅ **Default 200 shuffle partitions often too high** — tune or enable AQE auto-coalescing
✅ **EXPLAIN FORMATTED shows PushedFilters** — confirms partition/predicate pushdown
✅ **Spot instances save 70-90%** — use for fault-tolerant batch jobs only
✅ **Photon accelerates SQL/Delta** — not UDFs or RDDs
