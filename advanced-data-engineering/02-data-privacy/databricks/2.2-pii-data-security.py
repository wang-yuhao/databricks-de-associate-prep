# Databricks notebook source
# MAGIC %md
# MAGIC # 2.2 Demo - PII Data Security: Pseudonymization and Anonymization (Databricks Free Edition)
# MAGIC **Lessons:** Pseudonymization & Anonymization · Summary & Best Practices
# MAGIC
# MAGIC | | Pseudonymization | Anonymization |
# MAGIC |---|---|---|
# MAGIC | Techniques | keyed / salted hashing, tokenization, encryption | generalization, suppression, perturbation, aggregation |
# MAGIC | Reversible? | yes, with the key / vault | no |
# MAGIC | Still personal data (GDPR)? | **yes** | no, if re-identification risk is low (check k-anonymity) |
# MAGIC
# MAGIC **One-time setup (laptop terminal, Databricks CLI):** keep the salt and key in a secret scope, never in code.
# MAGIC ```
# MAGIC databricks secrets create-scope privacy_course
# MAGIC databricks secrets put-secret privacy_course hash_salt      # type any long random string
# MAGIC databricks secrets put-secret privacy_course aes_key        # exactly 32 characters (AES-256)
# MAGIC ```
# MAGIC Without the scope, the notebook generates throwaway values for this session only.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

import secrets as pysecrets
from pyspark.sql import functions as F

SCOPE = "privacy_course"
try:
    SALT, AES_KEY = dbutils.secrets.get(SCOPE, "hash_salt"), dbutils.secrets.get(SCOPE, "aes_key")
    USE_SQL_SECRET = True
    print("Using secrets from scope", SCOPE, "(values are redacted in output)")
except Exception:
    SALT, AES_KEY = pysecrets.token_hex(16), pysecrets.token_hex(16)   # 32 hex chars = 32 bytes
    USE_SQL_SECRET = False
    print("Secret scope not found: using random session-only salt and key. Create the scope to practice the real setup.")
# SQL expression for the key: read inside the query, so the value never appears in query text or history
KEY_SQL = f"secret('{SCOPE}', 'aes_key')" if USE_SQL_SECRET else f"'{AES_KEY}'"

users = (spark.createDataFrame(datagen.make_users())
         .withColumn("dob", F.to_date("dob")).withColumn("signup_ts", F.to_timestamp("signup_ts")))
users.write.mode("overwrite").saveAsTable("users_raw")
display(spark.table("users_raw").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Hashing: why the salt must be secret
# MAGIC A plain hash of an email is **not** pseudonymous: anyone with a list of emails can hash them and join.

# COMMAND ----------

leaked_list = spark.table("users_raw").select("email").limit(50)   # e.g. a marketing list an attacker has
plain = spark.table("users_raw").select("user_id", F.sha2("email", 256).alias("email_hash"))
salted = spark.table("users_raw").select("user_id", F.sha2(F.concat(F.lit(SALT), "email"), 256).alias("email_hash"))
attack = leaked_list.select(F.sha2("email", 256).alias("email_hash"))
print("re-identified with a plain hash :", plain.join(attack, "email_hash").count())
print("re-identified with a secret salt:", salted.join(attack, "email_hash").count())

# COMMAND ----------

# MAGIC %md
# MAGIC In SQL, the `secret()` function reads the value inside the query, so it never appears in a notebook variable.
# MAGIC Only principals with `READ` on the scope can run it.

# COMMAND ----------

if USE_SQL_SECRET:
    display(spark.sql(f"""SELECT user_id, sha2(concat(secret('{SCOPE}', 'hash_salt'), email), 256) AS user_key
                          FROM users_raw LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Tokenization with a vault
# MAGIC Replace the value with a random token and keep the mapping in a **separate, locked-down** table. No key exists that
# MAGIC could be stolen; re-identification requires access to the vault. Deleting a person = deleting the vault row
# MAGIC (crypto-shredding style): every token of that person becomes meaningless.

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE token_vault AS
             SELECT email, uuid() AS email_token FROM (SELECT DISTINCT email FROM users_raw)""")
spark.sql("""CREATE OR REPLACE TABLE users_tokenized AS
             SELECT u.user_id, v.email_token, u.city, u.region FROM users_raw u JOIN token_vault v USING (email)""")
spark.sql("ALTER TABLE token_vault SET TAGS ('data_classification' = 'restricted')")
spark.sql("REVOKE ALL PRIVILEGES ON TABLE token_vault FROM `account users`")
display(spark.table("users_tokenized").limit(5))
# Authorized re-identification (e.g. support, with access to the vault):
display(spark.sql("SELECT t.user_id, v.email FROM users_tokenized t JOIN token_vault v USING (email_token) LIMIT 3"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Reversible encryption
# MAGIC `aes_encrypt` / `aes_decrypt` (AES-GCM by default). Store the ciphertext as base64.

# COMMAND ----------

enc = spark.table("users_raw").select(
    "user_id", F.base64(F.expr(f"aes_encrypt(ssn, {KEY_SQL})")).alias("ssn_encrypted"))
enc.write.mode("overwrite").saveAsTable("users_encrypted")
display(spark.sql(f"""SELECT user_id, ssn_encrypted,
                             CAST(aes_decrypt(unbase64(ssn_encrypted), {KEY_SQL}) AS STRING) AS ssn_decrypted
                      FROM users_encrypted LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Production pattern: put `aes_decrypt(unbase64(ssn_encrypted), secret('privacy_course', 'aes_key'))` inside a
# MAGIC column-mask function, so only authorized users ever see the decrypted value (lesson 2.1).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Masking (display-time redaction)
# MAGIC The built-in `mask()` replaces upper-case, lower-case and digit characters. Partial masks keep a useful part.

# COMMAND ----------

display(spark.sql("""
SELECT mask(phone) AS phone_masked,
       concat(repeat('*', length(phone) - 4), right(phone, 4)) AS phone_last4,
       regexp_replace(email, '^(.).*(@.*)$', '$1***$2') AS email_partial,
       mask(ssn, 'X', 'x', '#', '-') AS ssn_masked
FROM users_raw LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Anonymization: generalize, suppress, perturb, aggregate

# COMMAND ----------

anon = spark.sql("""
SELECT
  -- generalization
  concat(CAST(floor(months_between(current_date(), dob) / 120) * 10 AS INT), 's') AS age_band,
  concat(left(zip, 2), '***') AS zip_prefix,
  gender,
  region,
  -- perturbation: +-5% noise, then rounded, so the exact salary can't be looked up
  CAST(round(salary * (1 + (rand(42) - 0.5) * 0.1), -3) AS BIGINT) AS salary_noisy,
  CASE WHEN salary < 60000 THEN '<60k' WHEN salary < 120000 THEN '60-120k' ELSE '120k+' END AS salary_band
  -- suppression: no name, email, phone, ssn, street, exact dob, user_id
FROM users_raw""")
anon.write.mode("overwrite").saveAsTable("users_anonymized")
display(spark.table("users_anonymized").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Measure re-identification risk: k-anonymity and l-diversity
# MAGIC **k-anonymity:** every combination of quasi-identifiers is shared by at least *k* rows.
# MAGIC **l-diversity:** within each group, the sensitive attribute has at least *l* distinct values (otherwise the group
# MAGIC reveals it anyway).

# COMMAND ----------

def k_anonymity(df, quasi_ids, k=5):
    groups = df.groupBy(*quasi_ids).count()
    r = groups.agg(F.min("count").alias("k"), F.count("*").alias("groups"),
                   F.sum(F.when(F.col("count") < k, F.col("count")).otherwise(0)).alias(f"rows_in_groups_below_{k}")).first()
    return r.asDict()


def l_diversity(df, quasi_ids, sensitive):
    return df.groupBy(*quasi_ids).agg(F.countDistinct(sensitive).alias("l")).agg(F.min("l")).first()[0]


print("raw  (dob, zip, gender)                :", k_anonymity(spark.table("users_raw"), ["dob", "zip", "gender"]))
print("anon (age_band, zip_prefix, gender)    :", k_anonymity(spark.table("users_anonymized"), ["age_band", "zip_prefix", "gender"]))
print("anon (age_band, region, gender)        :", k_anonymity(spark.table("users_anonymized"), ["age_band", "region", "gender"]))
print("l-diversity of salary_band (age_band, region, gender):",
      l_diversity(spark.table("users_anonymized"), ["age_band", "region", "gender"], "salary_band"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Task:** find a generalization with **k ≥ 5** (try coarser age bands like 20-year bands, or drop `gender`). Then
# MAGIC **suppress** the rows that are still in small groups and publish an aggregate:

# COMMAND ----------

display(spark.sql("""
SELECT age_band, region, count(*) AS people, round(avg(salary_noisy), -3) AS avg_salary
FROM users_anonymized
GROUP BY age_band, region
HAVING count(*) >= 5          -- suppression of small cells
ORDER BY age_band, region"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Best practices
# MAGIC - Classify at ingestion (tags, Data Classification); minimize what you keep (drop columns you don't need).
# MAGIC - Pseudonymize in Silver with a **secret** salt/key from a secret scope or KMS; restrict the vault/key to a few principals.
# MAGIC - Use column masks / row filters for who-sees-what; use anonymized aggregates for broad sharing.
# MAGIC - Pseudonymized data is still personal data: it is still subject to deletion requests (lesson 2.3).
# MAGIC - Verify anonymization (k-anonymity, l-diversity) before publishing; re-check when columns are added.
