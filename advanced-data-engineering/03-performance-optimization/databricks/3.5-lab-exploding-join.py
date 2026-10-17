# Databricks notebook source
# MAGIC %md
# MAGIC # 3.5 Lab - Exploding Join: Skew, Explosion, Spill (Databricks Free Edition)
# MAGIC Replace every `<FILL_IN>`, run each step, then run its **CHECK** cell. Solution: `solutions/3.5-lab-exploding-join-solution`.
# MAGIC **Lessons:** Skew · Spill · Lab: Exploding Join
# MAGIC
# MAGIC A daily job joins 20M transactions to a customer table and reports revenue per region. It is slow. Find out why in
# MAGIC the query profile, then fix it step by step.
# MAGIC - **Explosion:** the customer table has several rows per customer (history), so the join multiplies rows.
# MAGIC - **Skew:** one customer (id 0, a marketplace account) owns 30% of all transactions.
# MAGIC - **Spill:** too little memory per task → data written to disk during sort / aggregation / join.
# MAGIC
# MAGIC Serverless note: AQE (with skew-join handling) is on by default and its confs can't be set on serverless, so you
# MAGIC see skew mostly in the task-time distribution of the profile, and AQE may already split the hot partition. Salting
# MAGIC is still taught, because it is the fix when AQE can't help (e.g. skewed aggregations, or a window over one key).
# MAGIC The `merge` hint below only rules out a broadcast (forces a shuffle join); Photon may still execute it as a
# MAGIC shuffled **hash** join instead of sort-merge.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

spark.range(20_000_000).select(
    F.col("id").alias("txn_id"),
    F.when(F.rand(11) < 0.30, F.lit(0)).otherwise(F.abs(F.xxhash64("id")) % 200_000 + 1).alias("customer_id"),
    (F.rand(12) * 100).alias("amount"),
).write.mode("overwrite").saveAsTable("transactions_35")

# 200k customers, 1-4 history versions each; customer 0 has 5. Only one version per customer is current.
spark.range(0, 200_001).select(F.col("id").alias("customer_id")) \
    .withColumn("versions", F.when(F.col("customer_id") == 0, 5).otherwise(F.abs(F.xxhash64("customer_id")) % 4 + 1)) \
    .withColumn("v", F.explode(F.sequence(F.lit(1), F.col("versions").cast("int")))) \
    .select("customer_id", "v",
            F.element_at(F.array(F.lit("EU"), F.lit("US"), F.lit("APAC")), (F.col("customer_id") % 3 + 1).cast("int")).alias("region"),
            (F.col("v") == F.col("versions")).alias("is_current")) \
    .write.mode("overwrite").saveAsTable("customers_history_35")
print(spark.table("transactions_35").count(), "transactions |", spark.table("customers_history_35").count(), "customer rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Run the job as written
# MAGIC Run it and open the query profile. Note: **rows out of the join** vs. rows in, the longest task vs. the median.

# COMMAND ----------

def report(customers):
    return (spark.table("transactions_35").join(customers, "customer_id")
            .groupBy("region").agg(F.count("*").alias("txns"), F.round(F.sum("amount"), 0).alias("revenue")))


slow = report(spark.table("customers_history_35").hint("merge"))
t_slow = timed(slow, "as written")
display(slow)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Diagnose: key distribution on both sides
# MAGIC Output rows of an equi-join = Σ over keys of (left rows × right rows). Compute it **before** running a join.

# COMMAND ----------

left = spark.table("transactions_35").groupBy("customer_id").agg(F.count("*").alias("left_rows"))
right = spark.table("customers_history_35").groupBy("customer_id").agg(F.count("*").alias("right_rows"))
keys = left.join(right, "customer_id")
display(keys.orderBy(F.desc("left_rows")).limit(5))
# TODO 2: expected join output rows (a Python int) = sum over keys of left_rows * right_rows
expected_output = <FILL_IN>
print(f"transactions: {spark.table('transactions_35').count():,} -> join output: {expected_output:,}")

# COMMAND ----------

# CHECK step 2
assert expected_output > 2 * spark.table("transactions_35").count(), "the join should more than double the rows"
print("Step 2 passed: explosion confirmed; customer 0 is the hot key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Fix the explosion: join to exactly one row per customer
# MAGIC The report wants the **current** region, so filter the dimension to `is_current` (or deduplicate). This also fixes
# MAGIC the wrong numbers: the slow report counted each transaction once per history row.

# COMMAND ----------

# TODO 3: exactly one row per customer (the current version), columns customer_id, region
current = <FILL_IN>
fixed = report(current.hint("merge"))
t_fixed = timed(fixed, "one row per customer")

# COMMAND ----------

# CHECK step 3
assert current.groupBy("customer_id").count().filter("count > 1").count() == 0, "dimension must be unique per key"
assert fixed.agg(F.sum("txns")).first()[0] == spark.table("transactions_35").count(), "every transaction exactly once"
print("Step 3 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Fix the skew
# MAGIC Still a shuffle join: all 6M rows of customer 0 go to **one** task. In the profile, compare max vs. median task
# MAGIC time of the join stage (AQE may report *skewed partitions split*).
# MAGIC
# MAGIC ### Manual alternative: salting
# MAGIC Spread the hot key over N sub-keys: random `salt` in [0, N) on the big side, and replicate each dimension row N times
# MAGIC (one per salt) on the small side. Join on (customer_id, salt).

# COMMAND ----------

N = 16
# TODO 4a: random integer salt in [0, N) on every transaction
salted_txn = <FILL_IN>
# TODO 4b: every current customer row replicated once per salt value 0..N-1 (hint: explode(sequence(...)))
salted_dim = <FILL_IN>
salted = (salted_txn.join(salted_dim.hint("merge"), ["customer_id", "salt"])
          .groupBy("region").agg(F.count("*").alias("txns"), F.round(F.sum("amount"), 0).alias("revenue")))
t_salted = timed(salted, f"salted join (N={N})")

# COMMAND ----------

# CHECK step 4
a = {r.region: r.txns for r in fixed.collect()}
b = {r.region: r.txns for r in salted.collect()}
assert a == b, f"salting must not change the result: {a} vs {b}"
print("Step 4 passed - same result, hot key spread over", N, "tasks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - The best fix: don't shuffle the big side at all
# MAGIC The current dimension is 200k rows × 2 columns: small enough to **broadcast**. No shuffle of transactions → no
# MAGIC skewed shuffle partition. (Without the hint, AQE / the optimizer would likely choose this by itself.)

# COMMAND ----------

# TODO 5: the report with the current dimension broadcast (F.broadcast)
broadcast = <FILL_IN>
t_bcast = timed(broadcast, "broadcast hash join")
broadcast.explain()

# COMMAND ----------

# CHECK step 5
assert {r.region: r.txns for r in broadcast.collect()} == a
print("Step 5 passed")
print(f"as written {t_slow}s | dedup {t_fixed}s | salted {t_salted}s | broadcast {t_bcast}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Spill
# MAGIC Spill = a task's data doesn't fit in its execution memory and is written to local disk (and read back).
# MAGIC Causes: too few shuffle partitions, skew, explosion, wide rows. Provoke it with very few partitions and a
# MAGIC large sort, then look for **spill** in the query profile. Expect the 1-partition run to take noticeably longer
# MAGIC (up to a few minutes). Serverless sizes memory generously, so you may see little or no spill; that is fine. Don't
# MAGIC raise the `txn_id` limit beyond ~6M on Free Edition: everything goes through one task.

# COMMAND ----------

original = spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.shuffle.partitions", "1")
big_sort = (spark.table("transactions_35").filter("txn_id < 3000000")   # ~10M joined rows, ~1.4 GB through one task
            .join(spark.table("customers_history_35").hint("merge"), "customer_id")
            .withColumn("pad", F.expr("repeat('x', 100)")).orderBy("amount"))
try:
    timed(big_sort, "1 shuffle partition + exploding join + sort")
finally:
    spark.conf.set("spark.sql.shuffle.partitions", original)
timed(big_sort, f"shuffle.partitions={original}")

# COMMAND ----------

# MAGIC %md
# MAGIC Fixes for spill, in order: fix explosion and skew first; more partitions (smaller tasks); on classic compute,
# MAGIC memory-optimized instances / fewer cores per executor memory; avoid huge rows and unnecessary sorts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - **Skew** shows as one task (or a few) far slower than the median in the same stage; spot it with max vs. median task duration / rows per task in the query profile (Spark UI on classic).
# MAGIC - **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled`) splits oversized shuffle partitions of shuffle joins (sort-merge; shuffled hash since Spark 3.3) at runtime; it does not fix skewed aggregations or windows.
# MAGIC - **Salting**: add a random salt in [0, N) on the big side, replicate the small side N times, join on (key, salt). Result is unchanged; the hot key is spread over N tasks.
# MAGIC - **Broadcast hash join** (`F.broadcast` / `BROADCAST` hint, or `spark.sql.autoBroadcastJoinThreshold`) avoids shuffling the big side, so skew in the join key no longer matters.
# MAGIC - **Exploding join**: duplicate keys on both sides multiply rows (output = Σ left × right per key). Fix by deduplicating / filtering the dimension (e.g. current SCD2 row) before joining.
# MAGIC - **Spill** (memory → disk during shuffle, sort, aggregation, join) is caused by too few/too large partitions, skew, explosion, wide rows; fix the data problem first, then raise `spark.sql.shuffle.partitions` or use larger-memory workers.
