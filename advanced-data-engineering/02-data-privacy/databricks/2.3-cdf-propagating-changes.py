# Databricks notebook source
# MAGIC %md
# MAGIC # 2.3 Demo - Processing Records from CDF and Propagating Changes (Databricks Free Edition)
# MAGIC **Lessons:** Capturing Changed Data · Deleting Data in Databricks
# MAGIC
# MAGIC The right to be forgotten (GDPR Art. 17, CCPA) in a lakehouse:
# MAGIC 1. delete the person from the source-of-truth table,
# MAGIC 2. **propagate** the delete to every downstream table, incrementally, with **Change Data Feed**,
# MAGIC 3. make it **physical**: time travel, deletion vectors and CDF files still hold the data until purged and vacuumed.
# MAGIC
# MAGIC Serverless notes: streams use `trigger(availableNow=True)`; checkpoints live in the course volume.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

USERS, EVENTS, UPDATES = (f"{LANDING}/demo23/{d}" for d in ("users", "events", "updates"))
CKPT = f"{CHECKPOINTS}/demo23_users_silver"


# Look versions up instead of hard-coding them: an extra commit (auto compaction, predictive optimization)
# would shift every number after it.
def last_version(operation):
    return (spark.sql("DESCRIBE HISTORY users_bronze").filter(F.col("operation") == operation)
            .agg(F.max("version")).first()[0])


# Fresh start on every run of this cell
for d in ("users", "events", "updates"):
    datagen.clear(f"{LANDING}/demo23/{d}")
dbutils.fs.rm(CKPT, True)
for t in ("users_bronze", "users_silver", "events_silver", "delete_requests"):
    spark.sql(f"DROP TABLE IF EXISTS {t}")
datagen.land_users(USERS)
datagen.land_user_events(EVENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Bronze with Change Data Feed
# MAGIC With `delta.enableChangeDataFeed = true`, every commit also records row-level changes: `_change_type`
# MAGIC (`insert`, `update_preimage`, `update_postimage`, `delete`), `_commit_version`, `_commit_timestamp`.

# COMMAND ----------

spark.sql("""
CREATE TABLE users_bronze (
  user_id BIGINT, first_name STRING, last_name STRING, email STRING, phone STRING, ssn STRING,
  dob DATE, gender STRING, street STRING, city STRING, zip STRING, region STRING, salary BIGINT,
  signup_ts TIMESTAMP, updated_ts TIMESTAMP)
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableDeletionVectors = true)""")

(spark.read.json(USERS)
 .withColumn("dob", F.to_date("dob")).withColumn("signup_ts", F.to_timestamp("signup_ts"))
 .withColumn("updated_ts", F.col("signup_ts"))
 .select(*spark.table("users_bronze").columns)
 .write.mode("append").saveAsTable("users_bronze"))
spark.read.json(EVENTS).withColumn("event_ts", F.to_timestamp("event_ts")).write.saveAsTable("events_silver")
V_INSERT = last_version("WRITE")   # normally 1 (CREATE TABLE is version 0)
print(spark.table("users_bronze").count(), "users |", spark.table("events_silver").count(), "events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Silver: pseudonymized users, maintained from the change feed
# MAGIC A streaming CDF reader + `foreachBatch` MERGE. The checkpoint remembers the last processed table version, so each
# MAGIC run processes **only new changes**. Per micro-batch keep only the **latest** change per user (deletes win ties).

# COMMAND ----------

try:
    SALT = dbutils.secrets.get("privacy_course", "hash_salt")
except Exception:
    SALT = "demo-only-salt"   # create the secret scope in lesson 2.2 to use a real secret

spark.sql("""CREATE TABLE IF NOT EXISTS users_silver (
  user_id BIGINT, user_key STRING, age_band STRING, city STRING, region STRING, gender STRING,
  signup_ts TIMESTAMP, updated_ts TIMESTAMP)""")


def apply_changes(batch_df, batch_id):
    latest = (batch_df.filter("_change_type != 'update_preimage'")
              .withColumn("rn", F.row_number().over(Window.partitionBy("user_id").orderBy(
                  F.col("_commit_version").desc(), (F.col("_change_type") == "delete").desc())))
              .filter("rn = 1")
              .select("user_id", "_change_type",
                      F.sha2(F.concat(F.lit(SALT), F.lower("email")), 256).alias("user_key"),
                      F.concat((F.floor(F.months_between(F.current_date(), "dob") / 120) * 10).cast("int"),
                               F.lit("s")).alias("age_band"),
                      "city", "region", "gender", "signup_ts", "updated_ts"))
    (DeltaTable.forName(batch_df.sparkSession, f"{FQ}.users_silver").alias("t")
     .merge(latest.alias("s"), "t.user_id = s.user_id")
     .whenMatchedDelete(condition="s._change_type = 'delete'")
     .whenMatchedUpdateAll(condition="s._change_type != 'delete'")
     .whenNotMatchedInsertAll(condition="s._change_type != 'delete'")
     .execute())
    # Propagate deletes to every other table that holds the person's data
    (DeltaTable.forName(batch_df.sparkSession, f"{FQ}.events_silver").alias("e")
     .merge(latest.filter("_change_type = 'delete'").alias("d"), "e.user_id = d.user_id")
     .whenMatchedDelete().execute())


def propagate_user_changes():
    q = (spark.readStream.option("readChangeFeed", "true").option("startingVersion", V_INSERT)
         .table(f"{FQ}.users_bronze")
         .writeStream.foreachBatch(apply_changes)
         .option("checkpointLocation", CKPT)
         .trigger(availableNow=True).start())
    q.awaitTermination()
    print("silver:", spark.table("users_silver").count(), "| events:", spark.table("events_silver").count())


propagate_user_changes()
display(spark.table("users_silver").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Apply a batch of updates and deletes from the operational system

# COMMAND ----------

datagen.land_user_updates(UPDATES, batch=0)
updates = (spark.read.json(UPDATES)
           .withColumn("dob", F.to_date("dob")).withColumn("signup_ts", F.to_timestamp("signup_ts"))
           .withColumn("updated_ts", F.to_timestamp("updated_ts")))
display(updates.groupBy("operation").count())

(DeltaTable.forName(spark, "users_bronze").alias("t")
 .merge(updates.alias("s"), "t.user_id = s.user_id")
 .whenMatchedDelete(condition="s.operation = 'DELETE'")
 .whenMatchedUpdate(condition="s.operation = 'UPDATE'",
                    set={c: f"s.{c}" for c in spark.table("users_bronze").columns if c != "user_id"})
 .execute())
V_MERGE = last_version("MERGE")   # normally 2
print("MERGE committed as version", V_MERGE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Look at the change feed
# MAGIC The MERGE (normally version 2): each update produces an `update_preimage` and an `update_postimage`, each delete a `delete` row.

# COMMAND ----------

display(spark.sql(f"""SELECT _change_type, _commit_version, user_id, email, city
                     FROM table_changes('users_bronze', {V_MERGE}, {V_MERGE}) ORDER BY user_id, _change_type"""))

# COMMAND ----------

propagate_user_changes()
deleted_ids = [r.user_id for r in updates.filter("operation = 'DELETE'").select("user_id").collect()]
print("deleted user_ids:", deleted_ids)
print("still in silver :", spark.table("users_silver").filter(F.col("user_id").isin(deleted_ids)).count())
print("still in events :", spark.table("events_silver").filter(F.col("user_id").isin(deleted_ids)).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. A formal "right to be forgotten" request
# MAGIC Track requests in a table so you can prove when each one was completed.

# COMMAND ----------

req_ids = [r.user_id for r in spark.table("users_bronze").orderBy("user_id").limit(2).collect()]
spark.sql("CREATE OR REPLACE TABLE delete_requests (user_id BIGINT, requested_at TIMESTAMP, status STRING, completed_at TIMESTAMP)")
spark.sql(f"""INSERT INTO delete_requests VALUES ({req_ids[0]}, current_timestamp(), 'pending', NULL),
                                                  ({req_ids[1]}, current_timestamp(), 'pending', NULL)""")
spark.sql("""MERGE INTO users_bronze t
             USING (SELECT user_id FROM delete_requests WHERE status = 'pending') r ON t.user_id = r.user_id
             WHEN MATCHED THEN DELETE""")
propagate_user_changes()
spark.sql("UPDATE delete_requests SET status = 'completed', completed_at = current_timestamp() WHERE status = 'pending'")
display(spark.table("delete_requests"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. ...but the data is NOT gone yet
# MAGIC Time travel still returns the person, and so does the change feed.

# COMMAND ----------

ids = ", ".join(map(str, req_ids))
display(spark.sql(f"""
  SELECT 'time travel' AS source, user_id, email FROM users_bronze VERSION AS OF {V_INSERT} WHERE user_id IN ({ids})
  UNION ALL
  SELECT 'change feed', user_id, email FROM table_changes('users_bronze', {V_INSERT})
  WHERE user_id IN ({ids}) AND _change_type = 'delete'"""))
display(spark.sql("DESCRIBE HISTORY users_bronze").select("version", "timestamp", "operation", "operationMetrics"))
display(spark.sql("DESCRIBE DETAIL users_bronze").select("numFiles", "properties"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Make the delete physical
# MAGIC 1. `REORG TABLE ... APPLY (PURGE)` rewrites files with deletion vectors, so deleted rows leave the current files.
# MAGIC 2. `VACUUM` removes files (incl. CDF `_change_data` files) not referenced by versions inside the retention window.
# MAGIC
# MAGIC Default retention (`delta.deletedFileRetentionDuration`) is 7 days (fine for GDPR's one month, and it protects
# MAGIC concurrent readers). Serverless doesn't let you disable `spark.databricks.delta.retentionDurationCheck.enabled`, so
# MAGIC **for the demo only** lower the table's own retention: the safety check only compares an explicit `RETAIN` against
# MAGIC that property, so a plain `VACUUM` then uses 0 hours. On current runtimes UC managed tables also **block time travel**
# MAGIC (and CDF reads) older than `deletedFileRetentionDuration`, and `delta.logRetentionDuration` must stay >= it.
# MAGIC On UC managed tables, **predictive optimization** also runs VACUUM for you automatically.

# COMMAND ----------

display(spark.sql("REORG TABLE users_bronze APPLY (PURGE)"))
spark.sql("ALTER TABLE users_bronze SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours')")
display(spark.sql("VACUUM users_bronze DRY RUN"))     # what would be deleted

# COMMAND ----------

try:
    display(spark.sql("VACUUM users_bronze"))
except Exception as e:
    print("VACUUM with a short retention was refused here:", str(e).splitlines()[0][:250])
    print("Real-world answer: files become unreadable once the 7-day retention passes and VACUUM runs.")
spark.sql("ALTER TABLE users_bronze SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days')")

# COMMAND ----------

try:
    spark.sql(f"SELECT * FROM users_bronze VERSION AS OF {V_INSERT} WHERE user_id = {req_ids[0]}").collect()
    print("Old version still readable (VACUUM did not remove the files yet).")
except Exception as e:
    print("Time travel to the old version now fails:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Production checklist
# MAGIC - Enable CDF on every table that feeds others, and propagate deletes **incrementally** (as here, or with SDP
# MAGIC   `AUTO CDC` + `apply_as_deletes`, lesson 1.3).
# MAGIC - Use **lineage** (lesson 2.1) to find every downstream copy of a PII column, incl. ML features and exports.
# MAGIC - Schedule `REORG ... PURGE` and `VACUUM`, or rely on predictive optimization, so physical deletion meets your SLA.
# MAGIC - A streaming reader of a table that receives updates/deletes fails on non-append commits: use
# MAGIC   `skipChangeCommits` (ignore them) or read the change feed (process them).
# MAGIC - Keep evidence: the request table and `DESCRIBE HISTORY`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - CDF only records changes made **after** `delta.enableChangeDataFeed = true`; read it with `table_changes('t', start[, end])`
# MAGIC   (versions or timestamps, both inclusive) or `readChangeFeed` + `startingVersion` / `startingTimestamp`.
# MAGIC - `_change_type` values: `insert`, `update_preimage`, `update_postimage`, `delete`. For upserts downstream drop the
# MAGIC   pre-images and keep the latest change per key (`_commit_version`), or MERGE fails on multiple matching source rows.
# MAGIC - `DELETE` / `MERGE ... DELETE` are logical: the rows stay readable through time travel, CDF and (with deletion vectors)
# MAGIC   in the old data files until `REORG TABLE ... APPLY (PURGE)` rewrites them and `VACUUM` removes the unreferenced files.
# MAGIC - `VACUUM` default retention is 7 days; `RETAIN` below `delta.deletedFileRetentionDuration` fails unless
# MAGIC   `spark.databricks.delta.retentionDurationCheck.enabled = false` (newer runtimes ignore `RETAIN` on UC managed
# MAGIC   tables and use the table property). `VACUUM ... DRY RUN` lists up to 1000 files.
# MAGIC - After VACUUM you can no longer time travel to versions whose files were removed; `delta.logRetentionDuration`
# MAGIC   (default 30 days) governs how long the history itself is kept.
# MAGIC - A plain streaming read fails on a source that receives updates/deletes; use `skipChangeCommits` to ignore them or
# MAGIC   the change feed to process them. `foreachBatch` + `MERGE` is the standard pattern to apply CDF changes downstream.
