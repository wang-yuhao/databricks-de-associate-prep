# Day 6 — Practice Tasks: Unity Catalog & Data Governance

> **Environment:** Databricks Community Edition OR a workspace with Unity Catalog enabled.
> 
> ⚠️ **Note:** Community Edition may not have Unity Catalog. Many tasks can be run as SQL exercises to understand syntax. Use `SHOW` commands to explore what's available in your workspace.

---

## 🛠️ Setup (10 minutes)

```sql
-- Check if Unity Catalog is available
SHOW CATALOGS;
-- If you see 'main' catalog → Unity Catalog is available
-- If you only see 'hive_metastore' → using legacy Hive Metastore

-- Check your current catalog/schema context
SELECT current_catalog(), current_schema();

-- Check current user
SELECT current_user();
```

---

## Task 1 — Three-Level Namespace Navigation (20 min)

**Goal:** Understand and use the catalog.schema.table hierarchy.

```sql
-- === Part A: Explore the namespace ===

-- List all catalogs
SHOW CATALOGS;

-- List schemas in main catalog
SHOW SCHEMAS IN main;

-- List tables in a schema
SHOW TABLES IN main.default;

-- === Part B: Create a practice catalog/schema/table ===
-- (Skip if no create privileges — just read the syntax)

CREATE CATALOG IF NOT EXISTS training
COMMENT 'Practice catalog for Day 6 exercises';

USE CATALOG training;

CREATE SCHEMA IF NOT EXISTS retail
COMMENT 'Retail domain tables';

USE SCHEMA retail;

-- Create a MANAGED table (UC manages storage)
CREATE OR REPLACE TABLE training.retail.customers (
  customer_id STRING NOT NULL,
  name        STRING,
  email       STRING,
  country     STRING,
  created_at  TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
COMMENT 'Customer master data - managed table';

-- Insert sample data
INSERT INTO training.retail.customers VALUES
  ('C001', 'Alice Müller',   'alice@example.com',  'DE', current_timestamp()),
  ('C002', 'Bob Schmidt',    'bob@example.com',    'DE', current_timestamp()),
  ('C003', 'Carol Johnson',  'carol@example.com',  'US', current_timestamp()),
  ('C004', 'David Li',       'david@example.com',  'CN', current_timestamp()),
  ('C005', 'Eva Braun',      'eva@example.com',    'DE', current_timestamp());

SELECT * FROM training.retail.customers;

-- Create an orders table
CREATE OR REPLACE TABLE training.retail.orders (
  order_id    STRING NOT NULL,
  customer_id STRING,
  order_date  DATE,
  amount      DOUBLE,
  status      STRING
) USING DELTA
COMMENT 'Order transactions fact table';

INSERT INTO training.retail.orders VALUES
  ('ORD001', 'C001', '2024-01-10', 150.00, 'completed'),
  ('ORD002', 'C002', '2024-01-11', 75.50,  'completed'),
  ('ORD003', 'C001', '2024-01-12', 320.00, 'pending'),
  ('ORD004', 'C003', '2024-01-13', 99.99,  'completed'),
  ('ORD005', 'C004', '2024-01-14', 450.00, 'completed');

SELECT * FROM training.retail.orders;
```

**✅ Verify:** Run `DESCRIBE DETAIL training.retail.customers` — what is the `location` field? For managed tables, it should be inside the metastore storage.

```sql
DESCRIBE DETAIL training.retail.customers;
DESCRIBE DETAIL training.retail.orders;
```

---

## Task 2 — Managed vs External Tables (15 min)

```sql
-- === Understand DROP behavior ===

-- Test 1: Create a managed table and drop it
CREATE TABLE training.retail.temp_managed (id INT, val STRING) USING DELTA;
INSERT INTO training.retail.temp_managed VALUES (1, 'test');
SELECT * FROM training.retail.temp_managed;  -- verify data exists

DROP TABLE training.retail.temp_managed;
-- Result: BOTH the metadata AND data files are deleted
-- You cannot recover the data (no recycle bin)

-- Test 2: Describe the difference conceptually
-- An EXTERNAL table:
-- CREATE TABLE training.retail.ext_table USING DELTA
-- LOCATION 'abfss://mycontainer@mystorage.dfs.core.windows.net/ext_data/';
-- DROP TABLE → only metadata removed, data files stay at the LOCATION

-- Check if a table is managed or external:
DESCRIBE DETAIL training.retail.customers;
-- Look at 'type' column: 'MANAGED' or 'EXTERNAL'

-- Question to answer:
-- Q: You accidentally drop a managed table in production.
--    Can you recover the data? How?
-- A: No direct recovery. You must restore from:
--    1. Delta time travel IF you restore before the DROP (use UNDROP if available in workspace)
--    2. CLONE from backup
--    3. External storage backup
--    UNDROP TABLE is available in Databricks for managed UC tables within the retention period

SQL -- In some workspaces: UNDROP TABLE training.retail.temp_managed;
```

---

## Task 3 — Volumes (15 min)

```sql
-- Create a managed volume
CREATE VOLUME IF NOT EXISTS training.retail.raw_files
COMMENT 'Landing zone for raw file uploads';

-- List volumes
SHOW VOLUMES IN training.retail;

-- The volume path:
-- /Volumes/training/retail/raw_files/

-- Write a file to the volume via PySpark:
```

```python
# Write a sample CSV to the volume
import pandas as pd

data = {"order_id": ["A001", "A002"], "amount": [100.0, 200.0]}
pdf = pd.DataFrame(data)
spark_df = spark.createDataFrame(pdf)

spark_df.write.mode("overwrite").option("header", True).csv("/Volumes/training/retail/raw_files/sample_orders")

print("Written to volume successfully")

# Read back from the volume
df_from_volume = spark.read.option("header", True).csv("/Volumes/training/retail/raw_files/sample_orders")
df_from_volume.show()
```

```sql
-- Read from volume in SQL
SELECT * FROM read_files(
  '/Volumes/training/retail/raw_files/sample_orders',
  format => 'csv',
  header => true
);
```

---

## Task 4 — Access Control (25 min)

```sql
-- === Grants and Revokes ===

-- Add documentation and tags
COMMENT ON TABLE training.retail.orders IS 'All order transactions. Source: ERP system.';
COMMENT ON COLUMN training.retail.orders.amount IS 'Order value in EUR. Never negative.';

ALTER TABLE training.retail.orders
SET TAGS ('domain' = 'retail', 'tier' = 'silver', 'pii' = 'false');

ALTER TABLE training.retail.customers
SET TAGS ('domain' = 'retail', 'tier' = 'silver', 'pii' = 'true');

-- View tags
DESCRIBE EXTENDED training.retail.orders;

-- === Practice GRANT syntax (read-only if you don't have admin) ===

-- Grant SELECT on a table to a user
-- GRANT SELECT ON TABLE training.retail.orders TO `analyst@company.com`;

-- Grant SELECT on whole schema
-- GRANT SELECT ON SCHEMA training.retail TO `data_analysts`;

-- Grant USE permissions (required before any access)
-- GRANT USE CATALOG ON CATALOG training TO `data_analysts`;
-- GRANT USE SCHEMA ON SCHEMA training.retail TO `data_analysts`;

-- View what grants exist on a table
SHOW GRANTS ON TABLE training.retail.customers;
SHOW GRANTS ON TABLE training.retail.orders;

-- Show all grants your current user has
-- SHOW GRANTS TO `current_user@company.com`;
```

**Exercise — Answer these:**
```sql
-- Q1: A user wants to run SELECT * FROM training.retail.orders
--     What minimum set of privileges are needed?
-- A1: USE CATALOG on 'training', USE SCHEMA on 'training.retail', SELECT on the table

-- Q2: If you GRANT SELECT ON SCHEMA training.retail TO analyst_group
--     Can they create new tables in that schema? Why/why not?
-- A2: No. SELECT only allows reading. CREATE TABLE requires separate CREATE TABLE privilege.

-- Q3: What happens when you DROP a managed table in Unity Catalog?
-- A3: Both metadata AND data files are deleted permanently (use UNDROP within retention period)
SELECT 'Access control answers verified' AS status;
```

---

## Task 5 — Delta Sharing Concepts (15 min)

```sql
-- These commands require metastore admin or share provider privileges
-- Practice the syntax even if you can't execute in your environment

-- === Provider side ===

-- Create a share
-- CREATE SHARE retail_data_share
-- COMMENT 'Share retail order summaries with external partners';

-- Add a table to the share
-- ALTER SHARE retail_data_share
-- ADD TABLE training.retail.orders
-- COMMENT 'Order data without PII';

-- Add a partition to limit what's shared
-- ALTER SHARE retail_data_share
-- ADD TABLE training.retail.orders
-- PARTITION (country = 'DE');  -- Only share German orders

-- Create a recipient
-- CREATE RECIPIENT partner_acme;

-- Grant the recipient access
-- GRANT SELECT ON SHARE retail_data_share TO RECIPIENT partner_acme;

-- View shares
-- SHOW SHARES;
-- DESCRIBE SHARE retail_data_share;

-- === Key facts to memorize ===
SELECT
  'No data copying - recipient reads provider storage directly' AS fact_1,
  'Open protocol - works across clouds (AWS, Azure, GCP)' AS fact_2,
  'Shares contain tables, views, or schemas' AS fact_3,
  'Recipients get activation link to configure their client' AS fact_4;
```

---

## Task 6 — Knowledge Check Quiz (15 min)

```sql
-- Answer in SQL comments or in a notebook markdown cell

-- Q1. You run: DROP TABLE main.retail.transactions
--     This table was created with:
--     CREATE TABLE main.retail.transactions USING DELTA
--     LOCATION 'abfss://data@storage.dfs.core.windows.net/transactions'
--     What happens to the data files in ADLS?
-- A1: ____________

-- Q2. What is the correct 3-level path to access a volume file?
-- A2: ____________

-- Q3. A user belongs to group 'analysts' which has GRANT SELECT ON SCHEMA main.retail.
--     Can this user run INSERT INTO main.retail.orders ... ?
-- A3: ____________

-- Q4. What Unity Catalog feature automatically tracks which tables
--     fed data into another table without any setup?
-- A4: ____________

-- Q5. In Delta Sharing, the recipient runs:
--     spark.read.format('deltaSharing').load(...)
--     Are the data files copied to the recipient's storage?
-- A5: ____________
```

```python
# Answers
print("""
Q1: Data files are NOT deleted (external table). Only the metadata entry is removed.
Q2: /Volumes/{catalog}/{schema}/{volume}/{optional_path}/filename.ext
Q3: No. SELECT privilege does not grant INSERT/MODIFY rights.
Q4: Data Lineage (automatic in Unity Catalog)
Q5: No. Recipient reads directly from provider's storage. No data copying.
""")
```

---

## Task 7 — System Tables Exploration (10 min)

```sql
-- Unity Catalog System Tables (available in workspaces with UC)

-- Browse available system schemas
SHOW SCHEMAS IN system;

-- Information schema: table metadata
SELECT table_catalog, table_schema, table_name, table_type, created
FROM system.information_schema.tables
WHERE table_catalog = 'training'
ORDER BY created DESC
LIMIT 20;

-- Column metadata
SELECT table_name, column_name, data_type, is_nullable
FROM system.information_schema.columns
WHERE table_catalog = 'training' AND table_schema = 'retail'
ORDER BY table_name, ordinal_position;

-- Access audit logs (if system.access is available)
-- SELECT event_time, user_identity.email, action_name, request_params
-- FROM system.access.audit
-- WHERE event_date = current_date()
-- ORDER BY event_time DESC
-- LIMIT 50;
```

---

## ✅ Day 6 Completion Checklist

- [ ] Know the 3-level namespace: catalog.schema.table
- [ ] Understand managed vs external table DROP behavior
- [ ] Know volume path format: `/Volumes/{catalog}/{schema}/{volume}/`
- [ ] Can write GRANT/REVOKE statements
- [ ] Know that `USE CATALOG` + `USE SCHEMA` are prerequisites for table access
- [ ] Understand row filters and column masks concept
- [ ] Know that data lineage is automatic in Unity Catalog
- [ ] Understand Delta Sharing (no data copy, open protocol)
- [ ] Know Lakehouse Federation purpose (query external DBs directly)
- [ ] Completed Tasks 1-7

## 🔗 Additional Practice
- [Unity Catalog Tutorial](https://docs.databricks.com/data-governance/unity-catalog/get-started.html)
- [Privileges Reference](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- [CertSafari Unity Catalog Questions](https://www.certsafari.com/databricks/data-engineer-associate)
