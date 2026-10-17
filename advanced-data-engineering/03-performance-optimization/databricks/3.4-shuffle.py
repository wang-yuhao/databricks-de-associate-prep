# Databricks notebook source
# MAGIC %md
# MAGIC # 3.4 Demo - Shuffle (Databricks Free Edition)
# MAGIC **Lesson:** Shuffles
# MAGIC
# MAGIC A shuffle redistributes rows across the cluster by key (wide transformations: join, groupBy, distinct, window,
# MAGIC repartition). It costs serialization, disk and network. Serverless doesn't let you change
# MAGIC `spark.sql.autoBroadcastJoinThreshold` or AQE settings, so this demo controls join strategies with **hints**,
# MAGIC which work everywhere and are what you'd use in production code anyway.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

spark.range(20_000_000).select(
    F.col("id").alias("order_id"),
    (F.abs(F.xxhash64("id")) % 50_000).alias("product_id"),
    (F.abs(F.xxhash64("id", F.lit(1))) % 1_000_000).alias("customer_id"),
    (F.rand(5) * 200).alias("amount"),
    F.expr("repeat('x', 40)").alias("notes"),                 # a wide column you don't need
).write.mode("overwrite").saveAsTable("orders_34")
spark.range(50_000).select(
    F.col("id").alias("product_id"),
    F.element_at(F.array(*[F.lit(c) for c in ["Laptops", "Phones", "Audio", "Monitors", "Accessories"]]),
                 (F.col("id") % 5 + 1).cast("int")).alias("category"),
).write.mode("overwrite").saveAsTable("products_34")
table_files("orders_34"); table_files("products_34")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Reading a plan: where are the shuffles?
# MAGIC Look for `Exchange hashpartitioning(...)` (shuffle), `Exchange SinglePartition`, `BroadcastExchange` (the small side
# MAGIC is sent to every executor, no shuffle of the big side), and `AQEShuffleRead` (AQE re-planned after the shuffle).
# MAGIC Serverless runs **Photon**, so operators carry a `Photon` prefix (e.g. `PhotonShuffleExchangeSink/Source`,
# MAGIC `PhotonBroadcastHashJoin`, `PhotonShuffledHashJoin`); `explain()` shows the plan *before* AQE runs
# MAGIC (`isFinalPlan=false`), the query profile shows the final one.

# COMMAND ----------

joined = (spark.table("orders_34").join(spark.table("products_34"), "product_id")
          .groupBy("category").agg(F.sum("amount").alias("revenue")))
joined.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Broadcast hash join vs. sort-merge join
# MAGIC - **Sort-merge join**: both sides are shuffled by the key and sorted. Scales to any size; two exchanges.
# MAGIC - **Broadcast hash join**: the small side is copied to every executor; the big side is **not shuffled**.
# MAGIC   Static planning broadcasts below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB, based on estimated
# MAGIC   size); on Databricks, AQE can also switch to broadcast at runtime below
# MAGIC   `spark.databricks.adaptive.autoBroadcastJoinThreshold` (default 30 MB, based on actual shuffle size).
# MAGIC   Neither is settable on serverless - hints are.
# MAGIC
# MAGIC Force each with a hint and compare the query profiles (number of exchanges, shuffle bytes). Photon replaces
# MAGIC sort-merge joins with hash joins, so the `merge` hint may show up as `PhotonShuffledHashJoin` rather than
# MAGIC `SortMergeJoin`: the point is the same - **both** sides are shuffled (two exchanges) vs. one `BroadcastExchange`.

# COMMAND ----------

orders, products = spark.table("orders_34"), spark.table("products_34")
smj = orders.join(products.hint("merge"), "product_id").groupBy("category").agg(F.sum("amount"))
bhj = orders.join(F.broadcast(products), "product_id").groupBy("category").agg(F.sum("amount"))
smj.explain()
bhj.explain()
r_smj, r_bhj = timed(smj, "sort-merge join (hint merge)"), timed(bhj, "broadcast hash join")

# COMMAND ----------

# MAGIC %md
# MAGIC SQL hints do the same: `/*+ BROADCAST(p) */`, `/*+ MERGE(p) */`, `/*+ SHUFFLE_HASH(p) */`.

# COMMAND ----------

display(spark.sql("""SELECT /*+ BROADCAST(p) */ p.category, round(sum(o.amount), 2) AS revenue
                     FROM orders_34 o JOIN products_34 p USING (product_id) GROUP BY p.category"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Shuffle partitions and AQE
# MAGIC `spark.sql.shuffle.partitions` (settable on serverless) is the number of partitions **after** a shuffle.
# MAGIC Too few → huge tasks and spill; too many → thousands of tiny tasks. **AQE** looks at the real shuffle sizes and
# MAGIC **coalesces** small partitions (in the profile: *AQEShuffleRead*, "coalesced" partition count), switches a join
# MAGIC to broadcast when one side turns out small, and splits skewed partitions (lesson 3.5).

# COMMAND ----------

agg = spark.table("orders_34").groupBy("customer_id").agg(F.sum("amount").alias("revenue"))
original = spark.conf.get("spark.sql.shuffle.partitions")
print("default:", original)
try:
    for n in ["8", "200", "2000", "auto"]:
        try:
            spark.conf.set("spark.sql.shuffle.partitions", n)
            timed(agg, f"groupBy customer_id, shuffle.partitions={n}")
        except Exception as e:
            print(n, "->", str(e).splitlines()[0][:120])
finally:
    spark.conf.set("spark.sql.shuffle.partitions", original)

# COMMAND ----------

# MAGIC %md
# MAGIC Open the query profiles: with `2000` the shuffle-read stage first plans 2000 tasks, and AQE coalesces them;
# MAGIC with `8` each task processes far more data. `auto` (Databricks *auto-optimized shuffle*) picks the partition
# MAGIC count from the query plan and input size instead of the fixed open-source default of `200`. The `default:` line
# MAGIC above shows what your serverless session starts with.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Reduce what you shuffle
# MAGIC 1. **Prune columns early** (the `notes` column is shuffled for nothing if you select everything).
# MAGIC 2. **Filter early** (before the join, not after).
# MAGIC 3. **Pre-aggregate** before a join when the result is aggregated anyway.

# COMMAND ----------

wasteful = (spark.table("orders_34").join(spark.table("products_34").hint("merge"), "product_id")
            .filter("amount > 150").select("category", "amount", "notes")
            .groupBy("category").agg(F.sum("amount"), F.max("notes")))
lean = (spark.table("orders_34").filter("amount > 150").groupBy("product_id").agg(F.sum("amount").alias("amount"))
        .join(spark.table("products_34").hint("merge"), "product_id")
        .groupBy("category").agg(F.sum("amount")))
timed(wasteful, "join everything, then filter + aggregate")
timed(lean, "filter + pre-aggregate, then join")

# COMMAND ----------

# MAGIC %md
# MAGIC Compare **shuffle bytes written** in the two query profiles: the lean version's join shuffles 50,000
# MAGIC pre-aggregated, narrow rows instead of millions of rows that still carry `notes`. (Catalyst pushes simple filters down by itself; pre-aggregation and dropping
# MAGIC columns you still reference are your job.)
# MAGIC
# MAGIC ## E. Key takeaways
# MAGIC - Every `Exchange` is a shuffle; count them in the plan and look at their bytes in the profile.
# MAGIC - Broadcast small dimensions; let AQE coalesce partitions; set `shuffle.partitions` only when the profile says so.
# MAGIC - Shuffle less data: select, filter and aggregate before wide operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - Wide transformations shuffle: `join` (non-broadcast), `groupBy`/aggregations, `distinct`, window `partitionBy`,
# MAGIC   `repartition`, `orderBy`. Narrow ones (`filter`, `select`, `withColumn`, `coalesce`) do not.
# MAGIC - Broadcast join: no shuffle of the large side; small side must fit in driver and executor memory; not possible
# MAGIC   for the preserved side of an outer join (e.g. right side of a LEFT OUTER join can be broadcast, left cannot).
# MAGIC - `spark.sql.autoBroadcastJoinThreshold` default 10 MB (static, `-1` disables);
# MAGIC   `spark.databricks.adaptive.autoBroadcastJoinThreshold` default 30 MB (AQE runtime switch).
# MAGIC - Hints: `BROADCAST`/`BROADCASTJOIN`/`MAPJOIN`, `MERGE`/`SHUFFLE_MERGE`/`MERGEJOIN`, `SHUFFLE_HASH`,
# MAGIC   `SHUFFLE_REPLICATE_NL`; priority BROADCAST > MERGE > SHUFFLE_HASH > SHUFFLE_REPLICATE_NL.
# MAGIC - AQE (on by default): coalesce post-shuffle partitions, convert sort-merge to broadcast join at runtime,
# MAGIC   split skewed partitions in joins, and detect/propagate empty relations.
# MAGIC - `spark.sql.shuffle.partitions` default `200`; on Databricks `auto` enables auto-optimized shuffle.
# MAGIC   With AQE coalescing, a too-high value is far cheaper than a too-low one (spill, huge tasks).
