# Day 6 — Data Governance & Unity Catalog

> **Exam weight:** ~15% | **Time budget:** 8–10 hours

---

## 1. Unity Catalog Architecture

### The Three-Level Namespace
Unity Catalog introduces a **three-level namespace** that governs all data assets:

```
catalog.schema.table
   │       │     └── Table / View / Volume / Function
   │       └───── Schema (= Database in Hive Metastore)
   └─────── Catalog
```

**Examples:**
```sql
SELECT * FROM main.retail.orders;          -- catalog=main, schema=retail, table=orders
DESCRIBE TABLE main.retail.orders;
USE CATALOG main;
USE SCHEMA retail;
SELECT * FROM orders;                      -- short form after USE statements
```

### Hierarchy Overview

```
Metastore (one per region per account)
└─ Catalog
   └─ Schema (Database)
      └─ Tables (managed / external)
      └─ Views
      └─ Volumes (file storage)
      └─ Functions (UDFs, Python UDFs)
```

**Metastore:**
- Top-level governance container
- One per Databricks account per cloud region
- Linked to one or more workspaces
- Contains all Unity Catalog metadata

**Catalog:** First level of grouping. Use catalogs to separate environments (dev/staging/prod) or business units.

**Schema:** Equivalent to a database in the old Hive metastore. Groups related tables.

---

## 2. Managed vs External Tables

| Feature | Managed Table | External Table |
|---------|--------------|---------------|
| Data location | Managed by Unity Catalog (in metastore storage) | User-specified external location |
| `DROP TABLE` | Deletes **both** metadata AND data files | Deletes metadata only; data files remain |
| `CREATE TABLE` syntax | No `LOCATION` clause | Requires `LOCATION` clause |
| Migration | Simple move within UC | Must update location explicitly |
| Use case | Default for new tables | Existing data lakes, shared storage |

```sql
-- Managed Table (UC manages storage)
CREATE TABLE main.retail.customers (
  customer_id STRING,
  name STRING,
  email STRING
) USING DELTA;

-- External Table (you specify location)
CREATE TABLE main.retail.transactions
USING DELTA
LOCATION 'abfss://data@storageaccount.dfs.core.windows.net/transactions';
```

---

## 3. Volumes

Volumes are Unity Catalog objects for managing **non-tabular file storage** (PDFs, images, CSVs, Parquet, etc.).

| Type | Description |
|------|-------------|
| **Managed Volume** | UC manages the storage location |
| **External Volume** | Points to a user-specified external storage path |

```sql
-- Create a managed volume
CREATE VOLUME main.retail.raw_files;

-- Create an external volume
CREATE EXTERNAL VOLUME main.retail.archives
LOCATION 'abfss://archive@storage.dfs.core.windows.net/data';

-- Access files in a volume
SELECT * FROM read_files('/Volumes/main/retail/raw_files/orders.csv');

-- Or in PySpark
df = spark.read.csv("/Volumes/main/retail/raw_files/orders.csv")
```

**Volume path format:** `/Volumes/{catalog}/{schema}/{volume}/{path}`

---

## 4. Access Control & Privileges

### Securable Objects
Privileges can be granted on:
- `METASTORE` (global)
- `CATALOG`
- `SCHEMA`
- `TABLE` / `VIEW`
- `VOLUME`
- `FUNCTION`
- `EXTERNAL LOCATION`
- `STORAGE CREDENTIAL`

### Privilege Types

| Privilege | Applies To | What It Allows |
|-----------|-----------|----------------|
| `SELECT` | Table, View | Query data |
| `MODIFY` | Table | INSERT, UPDATE, DELETE, MERGE |
| `CREATE TABLE` | Schema | Create tables in the schema |
| `CREATE SCHEMA` | Catalog | Create schemas in the catalog |
| `CREATE CATALOG` | Metastore | Create new catalogs |
| `USE CATALOG` | Catalog | Required to use any catalog |
| `USE SCHEMA` | Schema | Required to use any schema |
| `ALL PRIVILEGES` | Any | All privileges on the object |
| `READ VOLUME` | Volume | Read files from volume |
| `WRITE VOLUME` | Volume | Write files to volume |

**Important:** `USE CATALOG` + `USE SCHEMA` are required before `SELECT` can work on a table.

```sql
-- Grant a user SELECT on a table
GRANT SELECT ON TABLE main.retail.orders TO `alice@company.com`;

-- Grant use privileges (required for access)
GRANT USE CATALOG ON CATALOG main TO `alice@company.com`;
GRANT USE SCHEMA ON SCHEMA main.retail TO `alice@company.com`;

-- Grant a group broad access
GRANT SELECT ON SCHEMA main.retail TO `analysts`;

-- Revoke access
REVOKE SELECT ON TABLE main.retail.orders FROM `alice@company.com`;

-- Show grants
SHOW GRANTS ON TABLE main.retail.orders;
SHOW GRANTS TO `alice@company.com`;
```

### Inheritance
Privileges cascade down the hierarchy:
- GRANT on CATALOG → applies to all schemas + tables within
- GRANT on SCHEMA → applies to all tables within
- GRANT on specific TABLE → that table only

---

## 5. Row & Column-Level Security

### Row Filters
Row filters restrict which rows a user can see:
```sql
-- Create a row filter function
CREATE FUNCTION main.security.orders_filter(customer_id_param STRING)
RETURNS STRING
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admins'), TRUE, customer_id = customer_id_param);

-- Attach the filter to a table
ALTER TABLE main.retail.orders
SET ROW FILTER main.security.orders_filter ON (customer_id);
```

### Column Masks
Column masks hide or transform sensitive column values:
```sql
-- Create a masking function for PII
CREATE FUNCTION main.security.mask_email(email STRING)
RETURNS STRING
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('pii_access'), email, CONCAT(LEFT(email, 2), '***@***.***'));

-- Attach the mask to a column
ALTER TABLE main.retail.customers
ALTER COLUMN email
SET MASK main.security.mask_email;
```

---

## 6. Data Lineage

Unity Catalog **automatically captures data lineage** — no configuration required.

- Tracks: Table → Table, Column → Column flows
- Available in: Catalog Explorer UI or via REST API
- Works for: SQL queries, Spark jobs, DLT pipelines

**Viewing lineage:**
```
Catalog Explorer → Select Table → Lineage tab
  → See upstream (what fed this table) and downstream (what reads from it)
```

**System tables for lineage:**
```sql
-- Table lineage
SELECT * FROM system.access.table_lineage
WHERE target_table_full_name = 'main.retail.orders';

-- Column lineage
SELECT * FROM system.access.column_lineage
WHERE target_table_full_name = 'main.retail.orders';
```

---

## 7. Audit Logs

Unity Catalog logs all data access for compliance:

```sql
-- Audit log: who accessed what tables
SELECT
  event_time,
  user_identity.email,
  request_params.table_full_name,
  action_name
FROM system.access.audit
WHERE action_name = 'readTable'
  AND event_date >= '2024-01-01'
ORDER BY event_time DESC
LIMIT 100;
```

**Key audit event types:**
- `createTable`, `dropTable`, `readTable`, `writeTable`
- `grantPrivilege`, `revokePrivilege`
- `listTables`, `describeTable`

---

## 8. Delta Sharing

Delta Sharing is an **open protocol** for sharing data across organizations, clouds, and platforms — without copying data.

**Roles:**
- **Provider:** Shares data
- **Recipient:** Reads shared data

**Provider workflow:**
```sql
-- 1. Create a share
CREATE SHARE orders_share;

-- 2. Add tables to the share
ALTER SHARE orders_share
ADD TABLE main.retail.orders
COMMENT 'Sharing order data with partner';

-- 3. Create a recipient
CREATE RECIPIENT partner_company
COMMENT 'External partner - Acme Corp';

-- 4. Grant the recipient access to the share
GRANT SELECT ON SHARE orders_share TO RECIPIENT partner_company;

-- Show shares
SHOW SHARES;
SHOW GRANTS ON SHARE orders_share;
```

**Recipient workflow:**
```python
# Recipient accesses shared data using the activation link/token
# They can query it without copying files
df = spark.read.format("deltaSharing").load("path_to_share_profile")
```

---

## 9. Lakehouse Federation

Lakehouse Federation allows querying **external databases** (PostgreSQL, MySQL, Snowflake, BigQuery) **directly from Databricks** using Unity Catalog — without ETL.

```sql
-- 1. Create a connection to external DB
CREATE CONNECTION postgresql_prod
TYPE POSTGRESQL
OPTIONS (
  host 'db.example.com',
  port '5432',
  user '<user>',
  password secret('scope', 'key'),
  database 'production'
);

-- 2. Create a foreign catalog
CREATE FOREIGN CATALOG postgres_catalog
USING CONNECTION postgresql_prod
OPTIONS (database 'production');

-- 3. Query the external table directly
SELECT * FROM postgres_catalog.public.customers LIMIT 10;
```

**Supported external systems:** PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery, Azure Synapse, Redshift, Salesforce, etc.

---

## 10. Tags & Documentation

```sql
-- Add tags to objects
ALTER TABLE main.retail.orders
SET TAGS ('pii' = 'true', 'domain' = 'sales', 'tier' = 'gold');

-- Tag a column
ALTER TABLE main.retail.customers
ALTER COLUMN email
SET TAGS ('classification' = 'PII');

-- Add comments
COMMENT ON TABLE main.retail.orders IS 'Fact table of all customer orders';
COMMENT ON COLUMN main.retail.orders.amount IS 'Order total in EUR';

-- Search by tag in Catalog Explorer (UI) or:
SELECT * FROM system.information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';
```

---

## 11. Exam-Focused Key Points

### Namespace & Access
- `USE CATALOG` + `USE SCHEMA` are prerequisites before any table access
- `DROP TABLE` on a managed table → **deletes data AND metadata**
- `DROP TABLE` on an external table → **deletes metadata only**
- Row filters and column masks use SQL functions attached to tables via `ALTER TABLE ... SET ROW FILTER`

### Volumes
- Access path: `/Volumes/{catalog}/{schema}/{volume}/{file}`
- Managed volumes = UC owns storage; External volumes = user-specified path
- Use `read_files()` in SQL or `spark.read` in Python

### Delta Sharing
- Share = collection of tables; Recipient = entity that gets access
- No data copying — recipient reads directly from provider's storage
- Open protocol = works across clouds and platforms (not Databricks-only)

### Unity Catalog vs Hive Metastore
| Feature | Hive Metastore | Unity Catalog |
|---------|---------------|---------------|
| Namespace | 2-level (db.table) | 3-level (catalog.schema.table) |
| Governance | Per-workspace | Account-level |
| Lineage | No | Automatic |
| Row/Column security | No | Yes |
| Delta Sharing | No | Yes |
| Audit logs | Limited | Full (system tables) |

---

## 📺 Recommended Videos

| Topic | Video | Duration |
|-------|-------|----------|
| Unity Catalog Overview | [Unity Catalog Deep Dive (Databricks)](https://www.youtube.com/watch?v=ZFZOoFb-HKo) | 40 min |
| Row & Column Security | [Fine-Grained Access Control](https://www.youtube.com/watch?v=iqb5tAAlRd0) | 20 min |
| Delta Sharing | [Delta Sharing Overview](https://www.youtube.com/watch?v=M0bJyaFXqNo) | 15 min |
| Lakehouse Federation | [Lakehouse Federation](https://www.youtube.com/watch?v=3RhX5o4dkV0) | 15 min |

## 📖 Recommended Reading

- [Unity Catalog Overview](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Privileges Reference](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Delta Sharing](https://docs.databricks.com/delta-sharing/index.html)
- [Volumes](https://docs.databricks.com/ingestion/file-upload/upload-to-volume.html)
- [Lakehouse Federation](https://docs.databricks.com/query-federation/index.html)
