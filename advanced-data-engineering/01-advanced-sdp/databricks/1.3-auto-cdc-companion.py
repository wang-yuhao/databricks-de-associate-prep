# Databricks notebook source
# MAGIC %md
# MAGIC # 1.3 Demo - Automating SCD Type 2 with AUTO CDC (Databricks)
# MAGIC Companion notebook for the `auto_cdc` pipeline. Guide: `FREE_EDITION_GUIDE.md` → Course 1 → Lesson 1.3.
# MAGIC
# MAGIC | Pipeline setting | Value |
# MAGIC |---|---|
# MAGIC | Source code | `01-advanced-sdp/pipelines/auto_cdc/transformations` (or the SQL version `databricks/sql/auto_cdc.sql`) |
# MAGIC | Default catalog / schema | `workspace` / `sdp_1_3` |
# MAGIC | Configuration | `cdc_source` = `/Volumes/workspace/sdp_course/landing/customers_cdc` |
# MAGIC | Event log | Publish to `workspace.sdp_1_3.event_log` |
# MAGIC
# MAGIC Rhythm: land batch N in `00-land-course-data` → **Start** the pipeline → run the matching step here.

# COMMAND ----------

# MAGIC %run ./_event_log_helpers

# COMMAND ----------

dbutils.widgets.text("schema", "workspace.sdp_1_3")
dbutils.widgets.text("event_log", "workspace.sdp_1_3.event_log")
SCHEMA, EVENT_LOG = dbutils.widgets.get("schema"), dbutils.widgets.get("event_log")
spark.sql(f"USE {SCHEMA}")
CDC_PATH = "/Volumes/workspace/sdp_course/landing/customers_cdc"


def show_state(expected_scd1, expected_scd2):
    s1, s2 = spark.table("customers_scd1"), spark.table("customers_scd2")
    display(s1.orderBy("customer_id"))
    display(s2.orderBy("customer_id", "__START_AT"))
    print(f"customers_scd1: {s1.count()} rows (expected {expected_scd1})")
    print(f"customers_scd2: {s2.count()} rows (expected {expected_scd2}), "
          f"open versions: {s2.filter('__END_AT IS NULL').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Batch 0: initial load (4 inserts)

# COMMAND ----------

display(spark.read.json(f"{CDC_PATH}/cdc-000.json"))

# COMMAND ----------

show_state(expected_scd1=4, expected_scd2=4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Batch 1: updates, a delete, a new key, and two changes for one key in the same batch
# MAGIC - customer 1 moves to Porto → SCD2 closes the Lisbon version (`__END_AT = 2`) and opens Porto.
# MAGIC - customer 2: seq 2 changes only the **email** (not tracked: `track_history_except_column_list=["email"]`),
# MAGIC   seq 4 moves to Busan. Both arrive in one batch; AUTO CDC orders them by `sequence_num`.
# MAGIC - customer 3 is deleted (seq 3): gone from SCD1, its SCD2 version is closed.
# MAGIC - customer 5 is new.

# COMMAND ----------

display(spark.read.json(f"{CDC_PATH}/cdc-001.json"))

# COMMAND ----------

show_state(expected_scd1=4, expected_scd2=7)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Batch 2: late, out-of-order events
# MAGIC - customer 2, seq **3** (Incheon) arrives after seq 4 (Busan): SCD1 keeps Busan; SCD2 **inserts** Incheon between
# MAGIC   Seoul and Busan in sequence order.
# MAGIC - customer 3, seq 2 (Kyoto) arrives after its delete at seq 3. SCD1: **ignored** - the delete is remembered as a
# MAGIC   tombstone (`pipelines.cdc.tombstoneGCThresholdInSeconds`), so the key is not resurrected. SCD2: the key is never
# MAGIC   reopened (no open version), but because seq 2 precedes the delete, AUTO CDC may slot Kyoto into the closed
# MAGIC   history as `[2, 3)` and shorten Osaka to `[1, 2)`. The docs do not spell out this case - inspect customer 3 below.
# MAGIC - customer 4, seq 2: city Prague → Brno (and a new email) → a tracked change, so SCD2 closes Prague and opens Brno.
# MAGIC - customer 1, seq 3: identical values → no new SCD2 version.
# MAGIC
# MAGIC Expected SCD2 after batch 2: c1 Lisbon/Porto, c2 Seoul/Incheon/Busan, c3 Osaka (+ Kyoto?), c4 Prague/Brno, c5 Zurich
# MAGIC = **9 or 10** rows, 4 open versions.

# COMMAND ----------

display(spark.read.json(f"{CDC_PATH}/cdc-002.json"))

# COMMAND ----------

show_state(expected_scd1=4, expected_scd2="9 or 10")
display(spark.sql("SELECT * FROM customers_scd2 WHERE customer_id IN (2, 3) ORDER BY customer_id, __START_AT"))

# COMMAND ----------

assert spark.table("customers_scd1").filter("customer_id = 3").count() == 0, "customer 3 must stay deleted"
assert spark.table("customers_scd1").filter("customer_id = 2").first()["city"] == "Busan", \
    "SCD1 keeps the value with the highest sequence_num"
s2 = spark.table("customers_scd2")
assert s2.filter("customer_id = 3 AND __END_AT IS NULL").count() == 0, "customer 3 must have no open SCD2 version"
assert [r["city"] for r in s2.filter("customer_id = 2").orderBy("__START_AT").collect()] == ["Seoul", "Incheon", "Busan"],     "late seq 3 must be inserted between Seoul and Busan"
assert s2.filter("__END_AT IS NULL").count() == 4, "one open version per live customer (1, 2, 4, 5)"
print("OK - late events were handled in sequence order")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Point-in-time queries on SCD2
# MAGIC `__START_AT` / `__END_AT` hold `sequence_num` values and have the same type as the `sequence_by` column (here BIGINT).
# MAGIC "Which version was valid at sequence 3?" Comparing across keys only makes sense when `sequence_by` is a global clock
# MAGIC (e.g. a commit timestamp); in this toy feed it is a per-key counter.

# COMMAND ----------

dbutils.widgets.text("as_of_sequence", "3")
display(spark.sql(f"""
SELECT customer_id, name, city, email, __START_AT, __END_AT
FROM customers_scd2
WHERE __START_AT <= {int(dbutils.widgets.get('as_of_sequence'))}
  AND (__END_AT IS NULL OR __END_AT > {int(dbutils.widgets.get('as_of_sequence'))})
ORDER BY customer_id"""))
display(spark.table("customers_current").orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - For comparison: the hand-written MERGE (SCD1 only)
# MAGIC What AUTO CDC saves you: deduplicate per key inside a batch, respect ordering, handle deletes, and you'd still need
# MAGIC extra state (tombstones) to reject the late update for customer 3. Run it on all three batches at once:

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE customers_merge (customer_id BIGINT, name STRING, email STRING, city STRING, sequence_num BIGINT)")
(spark.read.schema("customer_id BIGINT, name STRING, email STRING, city STRING, operation STRING, sequence_num BIGINT")
 .json(CDC_PATH).createOrReplaceTempView("cdc_all"))
spark.sql("""
MERGE INTO customers_merge t
USING (SELECT * FROM cdc_all QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY sequence_num DESC) = 1) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.operation = 'DELETE' AND s.sequence_num > t.sequence_num THEN DELETE
WHEN MATCHED AND s.sequence_num > t.sequence_num THEN
  UPDATE SET name = s.name, email = s.email, city = s.city, sequence_num = s.sequence_num
WHEN NOT MATCHED AND s.operation != 'DELETE' THEN
  INSERT (customer_id, name, email, city, sequence_num) VALUES (s.customer_id, s.name, s.email, s.city, s.sequence_num)
""")
display(spark.table("customers_merge").orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Processed **all at once** this MERGE matches SCD1. Processed **batch by batch** it would resurrect customer 3 in batch 2
# MAGIC (the row was deleted, so `WHEN NOT MATCHED` inserts the late update). That is exactly the tombstone problem AUTO CDC solves.
# MAGIC
# MAGIC **Try:** create a second pipeline (schema `sdp_1_3_sql`) from `databricks/sql/auto_cdc.sql` and compare the syntax:
# MAGIC `CREATE FLOW ... AS AUTO CDC INTO ... KEYS ... APPLY AS DELETE WHEN ... SEQUENCE BY ... STORED AS SCD TYPE 2
# MAGIC TRACK HISTORY ON * EXCEPT (email)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - `AUTO CDC` (formerly `APPLY CHANGES`) needs `KEYS` and `SEQUENCE BY`; the target must be a streaming table
# MAGIC   (`dp.create_streaming_table` / `CREATE OR REFRESH STREAMING TABLE`). Default is SCD type 1.
# MAGIC - Ordering is by the `sequence_by` value, not arrival order; it must be sortable and not null. Multiple events per
# MAGIC   key in one micro-batch are resolved by sequence.
# MAGIC - SCD1: a DELETE removes the row and leaves a tombstone so later-arriving, older events cannot resurrect the key
# MAGIC   (retention: `pipelines.cdc.tombstoneGCThresholdInSeconds`).
# MAGIC - SCD2 adds `__START_AT` / `__END_AT` (same type as `sequence_by`); the open version has `__END_AT IS NULL`; a DELETE
# MAGIC   closes the current version instead of removing rows.
# MAGIC - `TRACK HISTORY ON * EXCEPT (col)` / `track_history_except_column_list`: changes only to untracked columns update the
# MAGIC   current row in place; no new version.
# MAGIC - `COLUMNS * EXCEPT (...)` / `except_column_list` drops metadata columns (operation, sequence) from the target;
# MAGIC   `apply_as_truncates` is SCD1-only.
# MAGIC - Hand-written MERGE needs per-key dedup (`QUALIFY row_number() ... = 1`), a sequence guard on updates, and extra
# MAGIC   state for deletes - exactly what AUTO CDC manages for you.
