# Day 8 — Cost & Performance Optimisation (~13% of exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Afternoon (3h):** Run performance profiling tasks
- **Evening (1h):** Review performance optimisation questions

---

## 8.1 Adaptive Query Execution (AQE)

AQE dynamically re-optimizes query plans at runtime based on actual data statistics.

```python
# Enable AQE (default in DBR 7.3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE features:
# 1. Dynamic coalescing of shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")

# 2. Dynamic switching of join strategies (sort-merge -> broadcast)
spark.conf.set("spark.sql.adaptive.join.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")  # threshold

# 3. Dynamic skew join optimization
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

---

## 8.2 Join Strategies

| Strategy | When | Trigger |
|----------|------|---------|
| **Broadcast Hash Join** | Small table < threshold | Auto (AQE) or `broadcast()` hint |
| **Sort Merge Join** | Both large tables | Default for large joins |
| **Shuffle Hash Join** | One side much smaller | AQE chooses at runtime |

```python
from pyspark.sql.functions import broadcast

# Force broadcast join
result = large_df.join(broadcast(small_df), "customer_id")

# Broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20MB")

# Disable broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

---

## 8.3 Caching & Persistence

```python
# Cache in memory (Spark's default)
df.cache()               # equivalent to persist(MEMORY_AND_DISK)
df.persist()             # same

# Specific storage levels
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)        # fail if doesn't fit
df.persist(StorageLevel.MEMORY_AND_DISK)    # spill to disk
df.persist(StorageLevel.DISK_ONLY)          # disk only
df.persist(StorageLevel.OFF_HEAP)           # off-heap memory

# Delta table caching (server-side cache)
# Delta Cache: automatically caches Parquet files in SSD/memory
# Enabled by default on cluster node types with SSDs

# Explicitly cache a Delta table
spark.sql("CACHE SELECT * FROM training.prep.orders")
# Or
spark.catalog.cacheTable("training.prep.orders")

# Clear cache
df.unpersist()
spark.catalog.clearCache()
```

### Delta Cache vs Spark Cache
| Feature | Delta Cache | Spark Cache |
|---------|-------------|-------------|
| What | Raw data files (Parquet) | Spark DataFrames |
| Location | Local SSD/memory | Executor memory/disk |
| Persistence | Survives query restarts | Lost when executor dies |
| Invalidation | Auto (on Delta update) | Manual |

---

## 8.4 Photon Engine

- Vectorized execution engine (C++) replacing JVM-based Spark executor
- 2-10x faster for SQL queries, Delta Lake reads/writes
- Automatically used on Photon-enabled clusters
- Best for: aggregations, joins, sorts, delta reads

```sql
-- Verify Photon is active
SELECT version();
-- Look for 'Photon' suffix

-- Check in Spark UI: look for PhotonProject, PhotonFilter in plan
```

---

## 8.5 Partitioning Strategies

```python
# Repartition: full shuffle, equal-sized partitions
df_repartitioned = df.repartition(200)                    # by count
df_repartitioned = df.repartition("order_date")          # by column
df_repartitioned = df.repartition(200, "region")         # count + column

# Coalesce: no shuffle, reduce partitions only
df_small = df.coalesce(10)  # faster but uneven partitions

# Check partition count
print(df.rdd.getNumPartitions())

# Optimal partition size: ~128MB each
# Too many small partitions = overhead; too few = no parallelism
# Rule: 2-3x partitions per CPU core
```

---

## 8.6 Shuffle Optimizations

```python
# Shuffle partitions (default 200 - often too many for small data)
spark.conf.set("spark.sql.shuffle.partitions", "8")  # set based on data size

# Rule of thumb: target 128MB per shuffle partition
# If total shuffle data = 10GB: 10000MB / 128MB = ~80 partitions

# Local shuffle reader (AQE): avoids network I/O
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Tungsten binary format for shuffle
# (enabled by default)
```

---

## 8.7 Cost-Effective Cluster Configuration

```python
# Spot/preemptible instances (Azure: Spot VMs)
# Use for: non-critical workloads, batch jobs
# Cost: 70-90% cheaper than on-demand
# Risk: can be evicted

# Auto-scaling
# Min workers: 2, Max workers: 10
# Databricks scales based on task queue length

# Cluster policies: enforce cost controls
# Example: max DBUs/hour, max workers, auto-terminate

# Auto-termination: set idle time
# Prevents forgotten clusters from running indefinitely

# Instance pools: pre-warmed instances for faster cluster start
# Pool instances are held in IDLE state (no DBU cost)
```

### Cost Optimisation Checklist
- [ ] Use spot/preemptible instances for batch jobs
- [ ] Enable auto-scaling with appropriate min/max
- [ ] Set auto-termination (30-60 min for interactive, 5-10 min for jobs)
- [ ] Use instance pools to reduce cluster start time
- [ ] Choose right instance type (memory-optimized for ML, compute-optimized for ETL)
- [ ] Use serverless compute for SQL analytics (pay per query)
- [ ] Enable Delta auto-optimize (`autoCompact`, `optimizeWrite`)

---

## 8.8 Query Profiling with EXPLAIN

```python
# View execution plan
df.explain()              # logical plan
df.explain("extended")   # parsed, analyzed, optimized, physical plans
df.explain("codegen")    # generated code
df.explain("cost")       # cost-based optimizer statistics
df.explain("formatted")  # human-readable format

# SQL EXPLAIN
spark.sql("EXPLAIN FORMATTED SELECT * FROM training.prep.orders WHERE amount > 100")

# Look for:
# - FileScan: check filters pushed down (PushedFilters)
# - BroadcastHashJoin vs SortMergeJoin
# - Exchange: shuffle operations (expensive)
# - Sort: sort operations
```

---

## Key Exam Points ✔️

- **AQE** dynamically adjusts: shuffle partitions, join strategies, skew handling
- Broadcast join threshold: `spark.sql.autoBroadcastJoinThreshold` (default 10MB)
- `broadcast()` hint forces broadcast join regardless of size
- `repartition()` = full shuffle (even distribution); `coalesce()` = no shuffle (uneven)
- **Delta Cache** (SSD) caches raw files; **Spark Cache** caches DataFrames in executor memory
- Photon is a C++ vectorized engine; automatic on Photon clusters; best for SQL/Delta
- Shuffle partitions default is 200 — often too high for small data sets
- Spot instances save 70-90% but can be evicted; use for fault-tolerant batch jobs
- `EXPLAIN FORMATTED` shows PushedFilters (partition/predicate pushdown success)
- `autoOptimize.optimizeWrite` writes optimally-sized files without explicit OPTIMIZE
