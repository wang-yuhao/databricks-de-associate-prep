# Day 6 — Practice Tasks: Unity Catalog & Data Governance
### ☁️ Azure Databricks

> **Environment:** Azure Databricks with Unity Catalog enabled (Standard workspace).
> All tasks run directly — no simulation or workarounds needed.

---

## 🛠️ Setup (10 minutes)

```sql
-- Cell 1
USE CATALOG training;
USE SCHEMA prep;

SELECT current_catalog(), current_schema(), current_user();
-- Expected: training | prep | <your email>

-- List available catalogs (you should see 'training' + 'hive_metastore' + 'system')
SHOW CATALOGS;
```

---

## Task 1 — Three-Level Namespace Navigation (20 min)

**Goal:** Understand and use the catalog.schema.table hierarchy.

```sql
-- Part A: Explore the namespace
SHOW CATALOGS;
SHOW SCHEMAS IN training;
SHOW TABLES  IN training.prep;

-- Part B: Create a dedicated schema for governance practice
CREATE SCHEMA IF NOT EXISTS training.governance
  COMMENT 'Day 6: Unity Catalog governance exercises';

USE CATALOG training;
USE SCHEMA governance;

SELECT current_catalog(), current_schema();
```

```sql
-- Part C: Create a practice table
CREATE OR REPLACE TABLE training.governance.customers (
  customer_id   BIGINT,
  name          STRING,
  email         STRING,
  phone         STRING,
  region        STRING,
  signup_date   DATE,
  annual_spend  DOUBLE
) USING DELTA
COMMENT 'Customer dimension table for governance exercises';

INSERT INTO training.governance.customers VALUES
  (1, 'Alice Mueller',  'alice@example.com',   '+4989123456', 'EU',   '2022-01-15', 8500.0),
  (2, 'Bob Smith',     'bob@example.com',     '+12025550181','US',   '2021-06-01', 12000.0),
  (3, 'Carol Chen',    'carol@example.com',   '+86102345678','APAC', '2023-03-10', 6200.0),
  (4, 'Dirk Hoffman',  'dirk@example.com',    '+4930987654', 'EU',   '2020-11-20', 18000.0),
  (5, 'Emma Wilson',   'emma@example.com',    '+447911123456','EU',  '2023-07-05', 4500.0);

SELECT * FROM training.governance.customers;
```

**What to observe:**
- In the **Catalog Explorer** (Data tab), navigate to `training.governance.customers`
- Note the **Owner**, **Created**, **Tags**, and **Lineage** tabs
- Click **Permissions** — this is where you manage grants

---

## Task 2 — Grants and Privileges (30 min)

**Goal:** Practice GRANT, REVOKE, and SHOW GRANTS.

```sql
-- View your own current privileges
SHOW GRANTS ON TABLE training.governance.customers;

-- Grant SELECT to all users in the workspace (account users group)
GRANT SELECT ON TABLE training.governance.customers
TO `account users`;

-- Grant USE privileges up the hierarchy (required before table grants take effect)
GRANT USE CATALOG ON CATALOG training TO `account users`;
GRANT USE SCHEMA  ON SCHEMA training.governance TO `account users`;

-- Verify grants
SHOW GRANTS ON TABLE training.governance.customers;
SHOW GRANTS ON SCHEMA training.governance;
SHOW GRANTS ON CATALOG training;
```

```sql
-- Grant to a specific user (replace with a real user in your workspace if available)
-- GRANT MODIFY ON TABLE training.governance.customers TO `analyst@yourcompany.com`;

-- Grant ALL PRIVILEGES on schema to a group
-- GRANT ALL PRIVILEGES ON SCHEMA training.governance TO `data_engineers`;

-- Revoke
REVOKE SELECT ON TABLE training.governance.customers FROM `account users`;

-- Verify revoke
SHOW GRANTS ON TABLE training.governance.customers;
```

**Privilege hierarchy to remember:**
```
To access catalog.schema.table, a user needs:
  GRANT USE CATALOG ON CATALOG <catalog>
  GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema>
  GRANT SELECT      ON TABLE   <catalog>.<schema>.<table>

All three are required. Missing any one → access denied.
```

---

## Task 3 — Column Masking (PII Protection) (25 min)

**Goal:** Implement column-level masking to protect PII.

```sql
-- Step 1: Create the masking function
CREATE OR REPLACE FUNCTION training.governance.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_readers') THEN email         -- show full email to PII readers
  WHEN is_account_admin()        THEN email         -- admins always see full data
  ELSE CONCAT(LEFT(email, 2), '***@***.com')        -- everyone else sees masked
END;

-- Step 2: Apply mask to the email column
ALTER TABLE training.governance.customers
SET MASK training.governance.mask_email ON COLUMN (email);

-- Step 3: Query — your email may or may not be masked depending on your group membership
SELECT customer_id, name, email, region FROM training.governance.customers;
-- If you're an admin: full email shown
-- If not in pii_readers group: email shown as ab***@***.com
```

```sql
-- Step 4: Mask phone number too
CREATE OR REPLACE FUNCTION training.governance.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_readers') OR is_account_admin() THEN phone
  ELSE CONCAT('****', RIGHT(phone, 4))
END;

ALTER TABLE training.governance.customers
SET MASK training.governance.mask_phone ON COLUMN (phone);

-- Query with both masks applied
SELECT customer_id, name, email, phone FROM training.governance.customers;

-- Drop mask (to reset for next task)
ALTER TABLE training.governance.customers DROP MASK ON COLUMN (email);
ALTER TABLE training.governance.customers DROP MASK ON COLUMN (phone);
```

---

## Task 4 — Row-Level Security (RLS) (25 min)

**Goal:** Restrict rows based on the current user's region access.

```sql
-- Step 1: Create region access table
CREATE OR REPLACE TABLE training.governance.region_access (
  user_email STRING,
  region     STRING
) USING DELTA;

-- Grant your own user access to EU region
INSERT INTO training.governance.region_access VALUES
  (current_user(), 'EU'),
  ('analyst@example.com', 'US'),
  ('apac_analyst@example.com', 'APAC');

SELECT * FROM training.governance.region_access;
```

```sql
-- Step 2: Create the row filter function
CREATE OR REPLACE FUNCTION training.governance.customer_region_filter(region STRING)
RETURNS BOOLEAN
RETURN (
  is_account_admin()
  OR is_member('global_data_readers')
  OR EXISTS (
    SELECT 1 FROM training.governance.region_access ra
    WHERE ra.user_email = current_user()
      AND ra.region = region
  )
);

-- Step 3: Apply the row filter to the table
ALTER TABLE training.governance.customers
SET ROW FILTER training.governance.customer_region_filter ON (region);

-- Step 4: Query — you should only see EU rows (unless you're an admin)
SELECT customer_id, name, region, annual_spend
FROM training.governance.customers
ORDER BY customer_id;
-- If your user is mapped to 'EU': only Alice, Dirk, Emma visible
-- Admins: all 5 rows visible

-- Remove the row filter
ALTER TABLE training.governance.customers DROP ROW FILTER;
```

---

## Task 5 — External Locations and Volumes (20 min)

```sql
-- List existing external locations (set up by workspace admin)
SHOW EXTERNAL LOCATIONS;

-- List storage credentials
SHOW STORAGE CREDENTIALS;

-- Create a volume (managed — no external storage needed)
CREATE VOLUME IF NOT EXISTS training.governance.files
  COMMENT 'File storage for governance exercises';

-- List volumes in schema
SHOW VOLUMES IN training.governance;
```

```python
# Write a file to the volume
dbutils.fs.put(
    "/Volumes/training/governance/files/test.txt",
    "Hello from Unity Catalog Volume!",
    overwrite=True
)

# Read it back
content = dbutils.fs.head("/Volumes/training/governance/files/test.txt")
print(content)
# Output: Hello from Unity Catalog Volume!

# List volume contents
display(dbutils.fs.ls("/Volumes/training/governance/files/"))
```

```sql
-- External location example (read-only — admin must have set this up)
-- CREATE EXTERNAL LOCATION my_adls_location
-- URL 'abfss://container@mystorage.dfs.core.windows.net/data/'
-- WITH (STORAGE CREDENTIAL my_storage_credential)
-- COMMENT 'ADLS Gen2 external location for raw data';

-- Create an external table (data stored outside UC, managed externally)
-- CREATE TABLE training.governance.external_sales
-- LOCATION 'abfss://container@mystorage.dfs.core.windows.net/data/sales/'
-- COMMENT 'External Delta table on ADLS Gen2';
```

**Managed vs External Tables:**
| | Managed Table | External Table |
|---|---|---|
| **Storage** | UC-managed ADLS (automatic) | Your ADLS path (you control) |
| **DROP TABLE** | Deletes data | Deletes metadata only |
| **LOCATION clause** | Not needed | Required |
| **Best for** | Most use cases | Sharing data with non-Databricks tools |

---

## Task 6 — Data Lineage (15 min)

```sql
-- Create a lineage chain: raw → cleaned → aggregated
CREATE OR REPLACE TABLE training.governance.raw_transactions AS
SELECT
  customer_id,
  annual_spend AS transaction_amount,
  signup_date  AS transaction_date
FROM training.governance.customers;

CREATE OR REPLACE TABLE training.governance.clean_transactions AS
SELECT
  customer_id,
  transaction_amount,
  transaction_date,
  YEAR(transaction_date) AS transaction_year
FROM training.governance.raw_transactions
WHERE transaction_amount > 0;

CREATE OR REPLACE TABLE training.governance.customer_ytd AS
SELECT
  customer_id,
  SUM(transaction_amount) AS ytd_spend
FROM training.governance.clean_transactions
GROUP BY customer_id;

-- Now go to Catalog Explorer → training.governance.customer_ytd
-- Click the 'Lineage' tab to see the data flow graph
-- You should see: customers → raw_transactions → clean_transactions → customer_ytd
```

---

## Task 7 — Tags and Documentation (10 min)

```sql
-- Add tags to a table (helps with data discovery and governance)
ALTER TABLE training.governance.customers
SET TAGS ('pii' = 'true', 'domain' = 'customer', 'owner' = 'data-team');

-- Add tags to a column
ALTER TABLE training.governance.customers
ALTER COLUMN email SET TAGS ('pii' = 'true', 'sensitivity' = 'high');

-- View tags in Catalog Explorer → Tags tab
-- Or query via system tables:
SELECT * FROM system.information_schema.table_tags
WHERE catalog_name = 'training' AND schema_name = 'governance';
```

---

## Task 8 — System Tables & Audit (15 min)

```sql
-- Unity Catalog system tables (available in Azure Databricks)
-- Query audit logs, lineage, and billing

-- Table access history (requires system catalog access)
SELECT *
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('getTable', 'createTable', 'deleteTable')
  AND DATE(event_date) = CURRENT_DATE()
LIMIT 20;

-- Column lineage
SELECT *
FROM system.lineage.column_lineage
WHERE target_table_full_name = 'training.governance.customer_ytd'
LIMIT 20;

-- Table lineage
SELECT *
FROM system.lineage.table_lineage
WHERE target_table_full_name LIKE 'training.governance.%'
LIMIT 20;
```

---

## ✅ Day 6 Completion Checklist

- [ ] Three-level namespace navigated (catalogs, schemas, tables shown)
- [ ] `training.governance.customers` created with 5 rows
- [ ] GRANT + REVOKE executed; verified with SHOW GRANTS
- [ ] Column mask on `email` applied and tested
- [ ] Row filter on `region` applied and tested (EU rows only visible)
- [ ] Volume created and file written/read via `/Volumes/`
- [ ] Lineage chain created (3 tables); lineage graph viewed in Catalog Explorer
- [ ] Table tags added to `customers` table
