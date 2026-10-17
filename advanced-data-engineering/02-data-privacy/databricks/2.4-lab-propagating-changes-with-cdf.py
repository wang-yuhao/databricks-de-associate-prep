# Databricks notebook source
# MAGIC %md
# MAGIC # 2.4 Lab - Propagating Changes with CDF (Databricks Free Edition)
# MAGIC Complete every `TODO` / `<FILL_IN>`, then run the **CHECK** cell after it. Solution: `solutions/2.4-lab-...-solution`.
# MAGIC A cell that still contains `<FILL_IN>` is a `SyntaxError`, and an unfilled `raise NotImplementedError` stops the cell - replace both with your code.
# MAGIC Guide: `FREE_EDITION_GUIDE.md` → Course 2 → Lab 2.4.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

USERS, CLICKS, CDC = (f"{LANDING}/lab24/{d}" for d in ("users", "clicks", "cdc"))
CKPT = f"{CHECKPOINTS}/lab24_customers_silver"
for d in ("users", "clicks", "cdc"):
    datagen.clear(f"{LANDING}/lab24/{d}")
dbutils.fs.rm(CKPT, True)
for t in ("customers_bronze", "customers_silver", "clicks_silver"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
datagen.land_users(USERS)
datagen.land_user_events(CLICKS)
spark.read.json(CLICKS).withColumn("event_ts", F.to_timestamp("event_ts")).write.saveAsTable("clicks_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Create the Bronze table with Change Data Feed enabled

# COMMAND ----------

# TODO 1: create customers_bronze with Change Data Feed enabled
spark.sql("""
CREATE TABLE customers_bronze (
  user_id BIGINT, first_name STRING, last_name STRING, email STRING, city STRING, region STRING,
  updated_ts TIMESTAMP)
TBLPROPERTIES (<FILL_IN>)""")

(spark.read.json(USERS)
 .select("user_id", "first_name", "last_name", "email", "city", "region",
         F.to_timestamp("signup_ts").alias("updated_ts"))
 .write.mode("append").saveAsTable("customers_bronze"))

# COMMAND ----------

# CHECK step 1
props = {r.key: r.value for r in spark.sql("SHOW TBLPROPERTIES customers_bronze").collect()}
assert props.get("delta.enableChangeDataFeed") == "true", "Change Data Feed is not enabled"
assert spark.table("customers_bronze").count() == 200, "expected the 200 users of the initial load"
print("Step 1 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Apply a CDC batch to Bronze
# MAGIC One `MERGE` that **deletes** rows where `operation = 'DELETE'` and **updates** `email`, `city`, `region`,
# MAGIC `updated_ts` where `operation = 'UPDATE'`.

# COMMAND ----------

def apply_cdc_file(path: str) -> None:
    cdc = spark.read.json(path).withColumn("updated_ts", F.to_timestamp("updated_ts"))
    # TODO 2: MERGE cdc into customers_bronze on user_id: delete on DELETE, update 4 columns on UPDATE
    raise NotImplementedError("TODO 2")


apply_cdc_file(datagen.land_user_updates(CDC, batch=1))

# COMMAND ----------

# CHECK step 2
changes = spark.sql("SELECT _change_type, count(*) AS n FROM table_changes('customers_bronze', 2) GROUP BY 1")
counts = {r._change_type: r.n for r in changes.collect()}
assert counts.get("delete", 0) > 0 and counts.get("update_postimage", 0) > 0, \
    f"the MERGE must both delete and update rows, change feed of version 2: {counts}"
assert counts.get("update_preimage") == counts.get("update_postimage"), "every update has a pre- and a post-image"
print("Step 2 passed", counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read the change feed
# MAGIC A DataFrame of the changes **from version 2 on** (batch read), without the pre-images.

# COMMAND ----------

# TODO 3: batch-read the change feed of customers_bronze from version 2, without update_preimage rows
cdf = <FILL_IN>
display(cdf.select("_change_type", "_commit_version", "user_id", "email", "city"))

# COMMAND ----------

# CHECK step 3
assert {"_change_type", "_commit_version", "_commit_timestamp"} <= set(cdf.columns), "read with readChangeFeed = true"
assert cdf.filter("_change_type = 'update_preimage'").count() == 0, "filter out the update_preimage rows"
assert cdf.agg(F.min("_commit_version")).first()[0] >= 2, "start at version 2 (startingVersion)"
print("Step 3 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Incremental propagation with a streaming CDF reader
# MAGIC `customers_silver` stores the email **domain** only. `propagate()`:
# MAGIC - keeps the **latest** change per `user_id` in each micro-batch (highest `_commit_version`),
# MAGIC - MERGEs into `customers_silver`: delete on `delete`, otherwise upsert,
# MAGIC - deletes the clicks of deleted users from `clicks_silver`.
# MAGIC
# MAGIC Silver gets the initial snapshot (version 1) directly, so the stream starts at **version 2**.

# COMMAND ----------

spark.sql("CREATE TABLE customers_silver (user_id BIGINT, email_domain STRING, city STRING, region STRING, updated_ts TIMESTAMP)")
spark.sql("""INSERT INTO customers_silver
             SELECT user_id, substring_index(email, '@', -1), city, region, updated_ts
             FROM customers_bronze VERSION AS OF 1""")


def upsert_batch(batch_df, batch_id):
    # serverless (Spark Connect) runs this function on the server: use s, not the notebook's global spark
    s = batch_df.sparkSession
    # TODO 4a: latest change per user_id (highest _commit_version), no pre-images, columns:
    #          user_id, _change_type, email_domain (substring_index(email, '@', -1)), city, region, updated_ts
    latest = <FILL_IN>
    # TODO 4b: MERGE latest into f"{FQ}.customers_silver" (use DeltaTable.forName(s, ...)):
    #          delete on 'delete', update all otherwise, insert when not matched and not a delete
    # TODO 4c: delete the clicks of deleted users from f"{FQ}.clicks_silver"
    raise NotImplementedError("TODO 4b / 4c")


def propagate():
    # TODO 4d: stream the change feed of customers_bronze starting at version 2
    (spark.readStream.<FILL_IN>
     .table(f"{FQ}.customers_bronze")
     .writeStream.foreachBatch(upsert_batch)
     .option("checkpointLocation", CKPT)
     .trigger(availableNow=True)
     .start().awaitTermination())


propagate()

# COMMAND ----------

# CHECK step 4
bronze_ids = {r.user_id for r in spark.table("customers_bronze").select("user_id").collect()}
silver_ids = {r.user_id for r in spark.table("customers_silver").select("user_id").collect()}
assert bronze_ids == silver_ids, f"silver out of sync: {len(bronze_ids ^ silver_ids)} ids differ"
orphans = spark.table("clicks_silver").join(spark.table("customers_bronze"), "user_id", "left_anti").count()
assert orphans == 0, f"{orphans} clicks still belong to deleted users"
mismatch = spark.sql("""SELECT count(*) FROM customers_silver s JOIN customers_bronze b USING (user_id)
                        WHERE s.city <> b.city OR s.email_domain <> substring_index(b.email, '@', -1)""").first()[0]
assert mismatch == 0, f"{mismatch} rows not updated"
print("Step 4 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - The next day
# MAGIC Apply the next CDC file and run **the same** `propagate()`. Only the new changes are processed.

# COMMAND ----------

apply_cdc_file(datagen.land_user_updates(CDC, batch=2))
propagate()

# COMMAND ----------

# CHECK step 5
bronze_ids = {r.user_id for r in spark.table("customers_bronze").select("user_id").collect()}
assert bronze_ids == {r.user_id for r in spark.table("customers_silver").select("user_id").collect()}, \
    "customers_silver is out of sync after the second CDC file"
assert spark.table("clicks_silver").join(spark.table("customers_bronze"), "user_id", "left_anti").count() == 0, \
    "clicks of the newly deleted users are still in clicks_silver"
print("Step 5 passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Make the deletes physical
# MAGIC Deleted customers are still readable with time travel (`VERSION AS OF 1`). Remove them for good from
# MAGIC `customers_bronze`, `customers_silver` and `clicks_silver`. Serverless can't disable the retention check, so lower
# MAGIC each table's `delta.deletedFileRetentionDuration` for the lab, VACUUM, and restore 7 days. If the compute refuses the
# MAGIC 0-hour VACUUM, the CHECK accepts a recorded VACUUM and explains that the files go once the retention window passes.

# COMMAND ----------

for t in ("customers_bronze", "clicks_silver", "customers_silver"):
    # TODO 6: REORG TABLE ... APPLY (PURGE) (rewrite files that carry deletion vectors), set
    #         delta.deletedFileRetentionDuration to 'interval 0 hours', VACUUM, set it back to 'interval 7 days'
    raise NotImplementedError("TODO 6")

# COMMAND ----------

# CHECK step 6
# SELECT * (not count(*)): Delta can answer count(*) from log statistics without reading any data file
for table, version in (("customers_bronze", 1), ("clicks_silver", 0)):
    assert spark.catalog.tableExists(table), f"{table} is missing - re-run the notebook from the top"
    try:
        spark.sql(f"SELECT * FROM {table} VERSION AS OF {version}").collect()
    except Exception:
        print(f"{table} VERSION AS OF {version}: no longer readable (files physically removed)")
        continue
    vacuums = spark.sql(f"DESCRIBE HISTORY {table}").filter("operation LIKE 'VACUUM%'").count()
    assert vacuums > 0, f"{table} VERSION AS OF {version} is still readable - did you REORG ... PURGE and VACUUM it?"
    print(f"NOTE: {table} was vacuumed but VERSION AS OF {version} is still readable - this compute kept the old "
          "files (0-hour retention refused). The code is correct; the files go once the retention window passes.")
print("Step 6 done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - CDF is off by default; enable with `delta.enableChangeDataFeed = true`. Only changes committed **after** enabling are recorded.
# MAGIC - `_change_type` is `insert`, `update_preimage`, `update_postimage` or `delete`; every change row also has `_commit_version` and `_commit_timestamp`.
# MAGIC - Batch read: `spark.read.option("readChangeFeed", "true").option("startingVersion", n)` (or `startingTimestamp`, optional `endingVersion` / `endingTimestamp`, bounds inclusive); SQL: `table_changes('tbl', start [, end])`.
# MAGIC - Streaming read without a starting version first emits the current snapshot as `insert` rows, then the changes; the checkpoint makes each run process only new commits.
# MAGIC - One micro-batch can hold several changes for the same key: keep the latest (`row_number` over `_commit_version` desc) and drop `update_preimage` before a MERGE, or the MERGE fails on multiple matching source rows.
# MAGIC - Deletes do not propagate by themselves: each downstream table needs its own delete (here, the `delete` rows drive the Silver MERGE and the clicks delete).
# MAGIC - `DELETE` / `MERGE` are logical: old files (and change-data files) stay readable by time travel until `VACUUM` removes files older than `delta.deletedFileRetentionDuration` (default 7 days). With deletion vectors, run `REORG TABLE t APPLY (PURGE)` first so the rows are rewritten out of the live files.
