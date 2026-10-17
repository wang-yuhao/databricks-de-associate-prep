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
# MAGIC - customer 3, seq 2 arrives after its delete at seq 3: **ignored** (the delete is remembered as a tombstone).
# MAGIC - customer 4: email-only change → updated in place in SCD2 (no new version).
# MAGIC - customer 1, seq 3: same tracked values → no new SCD2 version.

# COMMAND ----------

display(spark.read.json(f"{CDC_PATH}/cdc-002.json"))

# COMMAND ----------

show_state(expected_scd1=4, expected_scd2=8)
display(spark.sql("SELECT * FROM customers_scd2 WHERE customer_id = 2 ORDER BY __START_AT"))

# COMMAND ----------

assert spark.table("customers_scd1").filter("customer_id = 3").count() == 0, "customer 3 must stay deleted"
assert spark.table("customers_scd1").filter("customer_id = 2").first()["city"] == "Busan", \
    "SCD1 keeps the value with the highest sequence_num"
print("OK - late events were handled in sequence order")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Point-in-time queries on SCD2
# MAGIC `__START_AT` / `__END_AT` hold `sequence_num` values (the `sequence_by` column). "What did we know at sequence 3?"

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
