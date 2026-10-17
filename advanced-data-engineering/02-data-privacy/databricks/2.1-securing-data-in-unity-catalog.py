# Databricks notebook source
# MAGIC %md
# MAGIC # 2.1 Demo - Securing Data in Unity Catalog (Databricks Free Edition)
# MAGIC **Lessons:** Regulatory Compliance · Data Privacy Key Concepts · Audit Your Data · Data Isolation · Securing Data in UC
# MAGIC
# MAGIC Free Edition has a single user (you) and no account console, so the **personas** are simulated with a mapping
# MAGIC table keyed by `current_user()`: you change your own persona and rerun the same query. Row filters and column
# MAGIC masks apply to **every** user, including the table owner, so this is a real test of the policies.
# MAGIC (A group-based version with `is_account_group_member()` is shown in step 5 for workspaces with groups.)

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - A customer table full of PII

# COMMAND ----------

from pyspark.sql import functions as F

users = (spark.createDataFrame(datagen.make_users())
         .withColumn("dob", F.to_date("dob")).withColumn("signup_ts", F.to_timestamp("signup_ts")))
spark.sql("DROP TABLE IF EXISTS customers")   # also removes policies left over from an earlier run
users.write.saveAsTable("customers")
display(spark.table("customers").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Privileges: the Unity Catalog object model
# MAGIC `metastore → catalog → schema → table / view / volume / function / model`. Privileges are inherited downward.
# MAGIC A reader needs **all three**: `USE CATALOG` + `USE SCHEMA` + `SELECT`. Grant to **groups**, not to individuals.

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `account users`")
spark.sql(f"GRANT SELECT ON TABLE {FQ}.customers TO `account users`")
display(spark.sql(f"SHOW GRANTS ON TABLE {FQ}.customers"))
display(spark.sql(f"SHOW GRANTS ON SCHEMA {FQ}"))

# COMMAND ----------

# Same information through INFORMATION_SCHEMA (queryable, good for audits)
display(spark.sql(f"""SELECT grantee, privilege_type, table_name FROM {CATALOG}.information_schema.table_privileges
                      WHERE table_schema = '{SCHEMA}'"""))
spark.sql(f"REVOKE SELECT ON TABLE {FQ}.customers FROM `account users`")  # least privilege: undo the demo grant

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Find and label the PII first
# MAGIC Classify before you protect. **Tags** are searchable metadata. **Data Classification** (Catalog Explorer → the
# MAGIC catalog → *Data classification*, if enabled in your workspace) scans tables and suggests PII tags automatically.

# COMMAND ----------

for col, tag in [("email", "email"), ("phone", "phone"), ("ssn", "national_id"), ("first_name", "name"),
                 ("last_name", "name"), ("street", "address"), ("dob", "quasi_identifier"), ("zip", "quasi_identifier")]:
    spark.sql(f"ALTER TABLE customers ALTER COLUMN {col} SET TAGS ('pii' = '{tag}')")
spark.sql("ALTER TABLE customers SET TAGS ('data_classification' = 'confidential', 'owner_team' = 'crm')")
display(spark.sql(f"""SELECT column_name, tag_name, tag_value FROM {CATALOG}.information_schema.column_tags
                      WHERE schema_name = '{SCHEMA}' AND table_name = 'customers' ORDER BY column_name"""))

# COMMAND ----------

# MAGIC %md
# MAGIC In Catalog Explorer, open `workspace.privacy_course.customers` and look at the tags. Search the workspace for
# MAGIC `tag:pii` to find every tagged column.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Personas (mapping table)
# MAGIC | Persona | Sees rows | Sees email | Sees SSN |
# MAGIC |---|---|---|---|
# MAGIC | `admin` | all | yes | yes |
# MAGIC | `eu_analyst` | EU only | masked | last 4 digits |
# MAGIC | `us_analyst` | US only | masked | last 4 digits |
# MAGIC | `support` (pii reader) | all | yes | last 4 digits |

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE user_access (user_email STRING, persona STRING, region STRING, can_read_pii BOOLEAN)
             COMMENT 'Access mapping used by the row filter and column masks.'""")
PERSONAS = {"admin": ("ALL", True), "eu_analyst": ("EU", False), "us_analyst": ("US", False), "support": ("ALL", True)}


def use_persona(persona: str):
    region, pii = PERSONAS[persona]
    spark.sql(f"DELETE FROM user_access WHERE user_email = '{ME}'")
    spark.sql(f"INSERT INTO user_access VALUES ('{ME}', '{persona}', '{region}', {pii})")
    print(f"{ME} is now '{persona}'")


use_persona("admin")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Row filter and column mask functions
# MAGIC Policies are ordinary SQL UDFs. A row filter returns BOOLEAN; a mask returns the column's type.
# MAGIC They look up the **querying** user with `current_user()`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION region_filter(row_region STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: admins and support see every region, analysts only their own.'
RETURN EXISTS (SELECT 1 FROM {FQ}.user_access a
               WHERE a.user_email = current_user() AND (a.region = 'ALL' OR a.region = row_region))""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN EXISTS (SELECT 1 FROM {FQ}.user_access a WHERE a.user_email = current_user() AND a.can_read_pii)
            THEN email ELSE regexp_replace(email, '^[^@]+', '*****') END""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN EXISTS (SELECT 1 FROM {FQ}.user_access a WHERE a.user_email = current_user() AND a.persona = 'admin')
            THEN ssn ELSE concat('***-**-', right(ssn, 4)) END""")

display(spark.sql("SELECT mask_email('ana.silva@example.com') AS email, mask_ssn('123-45-6789') AS ssn"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Group-based version** (the course's version, for workspaces with account groups). Check what your identity
# MAGIC resolves to. `is_member()` checks workspace-local groups (e.g. `admins`), `is_account_group_member()` account groups.

# COMMAND ----------

display(spark.sql("""SELECT current_user() AS me, is_member('admins') AS workspace_admin,
                            is_account_group_member('account users') AS in_account_users"""))
# CREATE FUNCTION region_filter_groups(row_region STRING) RETURNS BOOLEAN
# RETURN is_account_group_member('admins')
#     OR (row_region = 'EU' AND is_account_group_member('eu_analysts'))
#     OR (row_region = 'US' AND is_account_group_member('us_analysts'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Attach the policies to the table
# MAGIC From now on **every** query path (SQL, Python, BI tools, other workspaces) is filtered and masked.

# COMMAND ----------

spark.sql("ALTER TABLE customers SET ROW FILTER region_filter ON (region)")
spark.sql("ALTER TABLE customers ALTER COLUMN email SET MASK mask_email")
spark.sql("ALTER TABLE customers ALTER COLUMN ssn SET MASK mask_ssn")
display(spark.sql("DESCRIBE EXTENDED customers").filter("col_name IN ('Row Filter', 'Column Masks') OR col_name ILIKE '%mask%' OR col_name ILIKE '%filter%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Same query, different users

# COMMAND ----------

QUERY = "SELECT region, count(*) AS rows, max(email) AS sample_email, max(ssn) AS sample_ssn FROM customers GROUP BY region"
for persona in ["admin", "eu_analyst", "us_analyst", "support"]:
    use_persona(persona)
    display(spark.sql(QUERY))

# COMMAND ----------

# MAGIC %md
# MAGIC **CHECK:** as `eu_analyst` only the EU group is returned and emails start with `*****`.

# COMMAND ----------

use_persona("eu_analyst")
r = spark.sql("SELECT collect_set(region) AS regions, count_if(email NOT LIKE '*****@%') AS clear_emails FROM customers").first()
assert r.regions == ["EU"] and r.clear_emails == 0, r
use_persona("admin")
print("OK - row filter and column mask work")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Alternative: a dynamic view
# MAGIC The older pattern: no policy on the table, a view applies the functions, and readers get `SELECT` on the **view only**.
# MAGIC Use either table policies or a view. Table policies protect *every* path; a view only protects readers who can't
# MAGIC see the base table.

# COMMAND ----------

spark.sql("ALTER TABLE customers DROP ROW FILTER")
spark.sql("ALTER TABLE customers ALTER COLUMN email DROP MASK")
spark.sql("ALTER TABLE customers ALTER COLUMN ssn DROP MASK")
spark.sql("""CREATE OR REPLACE VIEW customers_secure AS
             SELECT user_id, mask_email(email) AS email, mask_ssn(ssn) AS ssn, city, region
             FROM customers WHERE region_filter(region)""")
use_persona("us_analyst")
display(spark.sql("SELECT region, count(*) AS rows, max(email) AS sample_email FROM customers_secure GROUP BY region"))
use_persona("admin")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: attribute-based access control (ABAC)
# MAGIC With ABAC (preview; Catalog Explorer → *Policies*), one policy on a catalog or schema masks **every column tagged**
# MAGIC `pii = email` in every current and future table, instead of `ALTER TABLE` per column. If your workspace shows a
# MAGIC *Policies* tab, create a column-mask policy on `workspace.privacy_course` that uses `mask_email` for columns with
# MAGIC tag `pii` = `email`, then query `customers` as `eu_analyst`. Governed tags (*Catalog → Govern → Governed tags*)
# MAGIC may be required first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Data isolation
# MAGIC Isolate by **catalog** per environment / region / business unit, each with its own managed storage location
# MAGIC (data residency), plus **workspace-catalog binding** (Catalog Explorer → catalog → *Workspaces*) so a catalog is only
# MAGIC usable from approved workspaces. Free Edition has one workspace and default storage; try creating a regional catalog:

# COMMAND ----------

try:
    spark.sql("CREATE CATALOG IF NOT EXISTS privacy_eu COMMENT 'EU-resident data only'")
    spark.sql("CREATE SCHEMA IF NOT EXISTS privacy_eu.crm")
    spark.sql("CREATE OR REPLACE TABLE privacy_eu.crm.customers AS SELECT * FROM workspace.privacy_course.customers WHERE region = 'EU'")
    display(spark.sql("DESCRIBE CATALOG EXTENDED privacy_eu"))
except Exception as e:
    print("Catalog creation not allowed here; use a schema per region instead:", str(e)[:200])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.privacy_eu")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Audit your data
# MAGIC **History** (what changed a table), **lineage** (where data flows), **audit log** (who accessed what).

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY customers").select("version", "timestamp", "userName", "operation", "operationParameters"))

# COMMAND ----------

def try_display(sql, what):
    try:
        display(spark.sql(sql))
    except Exception as e:
        print(f"{what} is not available in this workspace ({str(e).splitlines()[0][:150]}). Use the UI instead.")


try_display(f"""
SELECT event_time, user_identity.email, action_name, request_params.full_name_arg AS object
FROM system.access.audit
WHERE service_name = 'unityCatalog' AND request_params.full_name_arg LIKE '{FQ}.%'
  AND event_date >= current_date() - INTERVAL 2 DAYS
ORDER BY event_time DESC LIMIT 50""", "system.access.audit")

try_display(f"""
SELECT source_table_full_name, target_table_full_name, entity_type, event_time
FROM system.access.table_lineage
WHERE source_table_full_name LIKE '{FQ}.%' OR target_table_full_name LIKE '{FQ}.%'
ORDER BY event_time DESC LIMIT 50""", "system.access.table_lineage")

# COMMAND ----------

# MAGIC %md
# MAGIC **UI:** Catalog Explorer → `customers` → **Lineage** tab → *See lineage graph*: `customers_secure` and
# MAGIC `privacy_eu...customers` appear downstream, with column-level lineage for `email`. This is how you find every copy
# MAGIC of a PII column before a deletion request (lesson 2.3).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up (optional)

# COMMAND ----------

# spark.sql("DROP CATALOG IF EXISTS privacy_eu CASCADE")
