# Day 9 Practice Tasks — Unity Catalog & Data Governance

> **Exam section:** Data Governance (8%), Ensuring Data Security and Compliance (10%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours  
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## Task 1 — Unity Catalog Namespace Hierarchy

📖 **Context:** The Professional exam tests deep understanding of Unity Catalog's 3-level namespace (catalog.schema.table).

🛠️ **Instructions:**

```sql
-- Create catalog and schemas
CREATE CATALOG IF NOT EXISTS training_uc COMMENT 'Training catalog';
USE CATALOG training_uc;

CREATE SCHEMA training_uc.bronze COMMENT 'Raw landing zone';
CREATE SCHEMA training_uc.silver COMMENT 'Cleaned data';
CREATE SCHEMA training_uc.gold COMMENT 'Business-level data';

-- Create managed table (UC controls storage)
CREATE TABLE training_uc.bronze.raw_events (
    event_id BIGINT,
    user_id STRING,
    event_type STRING,
    event_timestamp TIMESTAMP
) USING DELTA;

SHOW TABLES IN training_uc.bronze;
```

✅ **Expected:** catalog with 3 schemas created, managed table in bronze

⚠️ **Exam trap:** Managed tables: DROP deletes data. External tables: DROP keeps data files.

---

## Task 2 — GRANT and REVOKE Permissions

📖 **Context:** Unity Catalog uses SQL-standard GRANT/REVOKE for access control.

🛠️ **Instructions:**

```sql
-- Grant catalog access
GRANT USE CATALOG ON CATALOG training_uc TO `data-engineers`;
GRANT CREATE SCHEMA ON CATALOG training_uc TO `data-engineers`;

-- Grant table access
GRANT SELECT ON TABLE training_uc.silver.validated_events TO `analysts`;
GRANT MODIFY ON TABLE training_uc.silver.validated_events TO `data-engineers`;

-- Show and revoke
SHOW GRANTS ON TABLE training_uc.silver.validated_events;
REVOKE SELECT ON TABLE training_uc.silver.validated_events FROM `analysts`;
```

✅ **Expected:** Privileges at different levels, inheritance works

⚠️ **Exam trap:** To SELECT a table, users need: USE CATALOG + USE SCHEMA + SELECT. Missing any breaks access.

---

## Task 3 — Column Masking

📖 **Context:** Column masking protects PII by showing different data to different users.

🛠️ **Instructions:**

```sql
-- Create masking function
CREATE OR REPLACE FUNCTION training_uc.silver.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN is_member('pii-access') THEN email
    ELSE regexp_replace(email, '(.{2}).+(@.+)', '$1***$2')
END;

-- Apply mask
ALTER TABLE training_uc.silver.customers
ALTER COLUMN email
SET MASK training_uc.silver.mask_email;

SELECT * FROM training_uc.silver.customers;
-- Non-PII users see: al***@email.com
```

✅ **Expected:** Users without `pii-access` see masked data, others see real data

⚠️ **Exam trap:** `is_member('group')` checks Azure AD group membership automatically.

---

## Task 4 — Row-Level Security

📖 **Context:** Row filters restrict which rows users can see.

🛠️ **Instructions:**

```sql
-- Create row filter
CREATE OR REPLACE FUNCTION training_uc.silver.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_member('global-access') OR region = current_user();

-- Apply filter
ALTER TABLE training_uc.silver.sales
SET ROW FILTER training_uc.silver.region_filter ON (region);

SELECT * FROM training_uc.silver.sales;
-- Users only see their region rows
```

✅ **Expected:** Users see only matching rows, admins see all

⚠️ **Exam trap:** Row filters return BOOLEAN (not STRING/INT). `current_user()` returns email.

---

## Task 5 — Audit Logging

📖 **Context:** Audit logs track all data access for compliance.

🛠️ **Instructions:**

```sql
-- Query recent SELECT operations
SELECT 
    event_time,
    user_identity.email AS user,
    action_name,
    request_params.table_full_name AS table_accessed
FROM system.access.audit
WHERE action_name = 'SELECT'
    AND event_time > current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC LIMIT 20;

-- Find failed access
SELECT 
    event_time,
    user_identity.email,
    response.error_message
FROM system.access.audit
WHERE response.status_code != 200
    AND event_time > current_timestamp() - INTERVAL 7 DAYS;
```

✅ **Expected:** Query who accessed what and when, track failures

⚠️ **Exam trap:** Audit is in `system.access.audit` (not `information_schema.audit`). READ-ONLY.

---

## Task 6 — External Locations

📖 **Context:** External locations enable Unity Catalog to govern external storage.

🛠️ **Instructions:**

```sql
-- Create storage credential (admin task)
CREATE STORAGE CREDENTIAL azure_mi_credential
WITH (AZURE_MANAGED_IDENTITY = (
    CONNECTOR_ID = '/subscriptions/.../accessConnectors/...'
));

-- Create external location
CREATE EXTERNAL LOCATION raw_data_location
URL 'abfss://raw-data@storage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL azure_mi_credential);

-- Grant access
GRANT READ FILES ON EXTERNAL LOCATION raw_data_location TO `data-engineers`;

SHOW EXTERNAL LOCATIONS;
```

✅ **Expected:** External location registered, access controlled via GRANT

⚠️ **Exam trap:** Storage credential = authentication. External location = authorization + path governance.

---

## Task 7 — Data Lineage

📖 **Context:** Unity Catalog automatically tracks column-level lineage.

🛠️ **Instructions:**

```python
# Create multi-layer pipeline
bronze_df.write.saveAsTable("training_uc.bronze.raw_orders")

silver_df = (
    spark.table("training_uc.bronze.raw_orders")
    .withColumn("total", col("qty") * col("price"))
)
silver_df.write.saveAsTable("training_uc.silver.processed_orders")

gold_df = (
    spark.table("training_uc.silver.processed_orders")
    .groupBy("date")
    .agg(sum("total").alias("revenue"))
)
gold_df.write.saveAsTable("training_uc.gold.daily_revenue")
```

```sql
-- Query lineage
SELECT 
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name
FROM system.information_schema.column_lineage
WHERE target_table_full_name = 'training_uc.gold.daily_revenue';
```

✅ **Expected:** Lineage tracked automatically bronze → silver → gold

⚠️ **Exam trap:** Lineage is AUTOMATIC — no setup needed. Query `system.information_schema.column_lineage`.

---

## Concept Quiz

1. What are the 3 levels in Unity Catalog namespace?
2. What privileges are needed to SELECT from a table?
3. What function checks if current user is in a group?
4. What does `SET MASK` do?
5. What does row filter return?
6. What system table stores audit logs?
7. Can you write to `system.access.audit`?
8. Managed vs external table: DROP behavior?
9. What does `current_user()` return?
10. How do privileges inherit?

**Answers:**
1. catalog.schema.table
2. USE CATALOG + USE SCHEMA + SELECT
3. `is_member('group-name')`
4. Applies dynamic masking function
5. BOOLEAN (true=show, false=hide)
6. `system.access.audit`
7. No — read-only
8. Managed: deletes data. External: keeps data
9. Current user's email
10. Top-down: catalog → schema → table

---

## Key Takeaways

✅ **3-level namespace:** catalog.schema.table  
✅ **GRANT hierarchy:** catalog → schema → table (privileges inherit)  
✅ **Column masking:** uses `is_member()` 
✅ **Row filters:** return BOOLEAN  
✅ **Audit:** `system.access.audit` (read-only)  
✅ **Managed tables:** UC controls storage, DROP deletes data  
✅ **External tables:** you control storage, DROP keeps data  
✅ **`current_user()`:** returns email  
✅ **Lineage:** automatic, no config needed
