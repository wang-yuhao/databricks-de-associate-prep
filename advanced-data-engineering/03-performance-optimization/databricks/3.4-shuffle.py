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

# COMMAND ----------

joined = (spark.table("orders_34").join(spark.table("products_34"), "product_id")
          .groupBy("category").agg(F.sum("amount").alias("revenue")))
joined.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Broadcast hash join vs. sort-merge join
# MAGIC - **Sort-merge join**: both sides are shuffled by the key and sorted. Scales to any size; two exchanges.
# MAGIC - **Broadcast hash join**: the small side (default ≤ ~10 MB, more with AQE / Photon) is copied to every executor.
# MAGIC   The big side is **not shuffled**.
# MAGIC Force each with a hint and compare the query profiles (number of exchanges, shuffle bytes).

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
for n in ["8", "200", "2000", "auto"]:
    try:
        spark.conf.set("spark.sql.shuffle.partitions", n)
        timed(agg, f"groupBy customer_id, shuffle.partitions={n}")
    except Exception as e:
        print(n, "->", str(e).splitlines()[0][:120])
spark.conf.set("spark.sql.shuffle.partitions", original)

# COMMAND ----------

# MAGIC %md
# MAGIC Open the query profiles: with `2000` the shuffle-read stage first plans 2000 tasks, and AQE coalesces them;
# MAGIC with `8` each task processes far more data. `auto` (Databricks) lets AQE pick the initial number.

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
# MAGIC Compare **shuffle bytes written** in the two query profiles: the lean version shuffles 50,000 pre-aggregated rows
# MAGIC instead of millions of wide rows. (Catalyst pushes simple filters down by itself; pre-aggregation and dropping
# MAGIC columns you still reference are your job.)
# MAGIC
# MAGIC ## E. Key takeaways
# MAGIC - Every `Exchange` is a shuffle; count them in the plan and look at their bytes in the profile.
# MAGIC - Broadcast small dimensions; let AQE coalesce partitions; set `shuffle.partitions` only when the profile says so.
# MAGIC - Shuffle less data: select, filter and aggregate before wide operations.
