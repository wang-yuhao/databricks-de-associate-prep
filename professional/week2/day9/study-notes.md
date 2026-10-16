# Day 9: Unity Catalog & Data Governance

## Schedule
- Morning: Unity Catalog architecture, metastore, namespaces
- Afternoon: Permissions, data lineage, auditing
- Evening: Practice tasks + review

---

## 9.1 Unity Catalog Overview

- Three-level namespace: `catalog.schema.table`
- Metastore: top-level container, one per region
- Workspace can be attached to one metastore
- Replaces legacy Hive metastore

```sql
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS my_catalog;
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

-- Create managed table
CREATE TABLE my_catalog.my_schema.orders (
  order_id BIGINT,
  customer_id BIGINT,
  amount DOUBLE,
  order_date DATE
) USING DELTA;

-- Create external table
CREATE TABLE my_catalog.my_schema.ext_orders
USING DELTA
LOCATION 'abfss://container@storage.dfs.core.windows.net/orders';
```

---

## 9.2 Securable Objects & Privilege Model

| Object | Privileges |
|--------|------------|
| Catalog | USE CATALOG, CREATE SCHEMA |
| Schema | USE SCHEMA, CREATE TABLE, CREATE VIEW |
| Table | SELECT, MODIFY, ALL PRIVILEGES |
| View | SELECT |
| Function | EXECUTE |
| External Location | CREATE EXTERNAL TABLE, READ FILES |
| Storage Credential | CREATE EXTERNAL LOCATION |

```sql
-- Grant privileges
GRANT USE CATALOG ON CATALOG my_catalog TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA my_catalog.my_schema TO `data-engineers`;
GRANT SELECT ON TABLE my_catalog.my_schema.orders TO `analysts`;
GRANT MODIFY ON TABLE my_catalog.my_schema.orders TO `data-engineers`;

-- Revoke
REVOKE SELECT ON TABLE my_catalog.my_schema.orders FROM `analysts`;

-- Show grants
SHOW GRANTS ON TABLE my_catalog.my_schema.orders;
```

---

## 9.3 Data Lineage

- Automatic column-level lineage tracking
- Captured for: tables, views, notebooks, jobs, DLT pipelines
- View in Catalog Explorer UI or via system tables

```sql
-- System tables for lineage
SELECT * FROM system.access.audit
WHERE action_name = 'commandSubmit'
LIMIT 100;

-- Column lineage via information_schema
SELECT * FROM system.information_schema.column_lineage
WHERE target_table_name = 'orders';
```

---

## 9.4 Row & Column Level Security

```sql
-- Column masking
CREATE FUNCTION my_catalog.my_schema.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii-access') THEN email
  ELSE regexp_replace(email, '(.).+(@.+)', '$1***$2')
END;

ALTER TABLE my_catalog.my_schema.customers
ALTER COLUMN email SET MASK my_catalog.my_schema.mask_email;

-- Row filter
CREATE FUNCTION my_catalog.my_schema.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_member('global-access') OR region = current_user();

ALTER TABLE my_catalog.my_schema.sales
SET ROW FILTER my_catalog.my_schema.region_filter ON (region);
```

---

## 9.5 Audit Logging

```sql
-- Audit log system table
SELECT
  event_time,
  user_identity.email AS user,
  action_name,
  request_params
FROM system.access.audit
WHERE action_name IN ('SELECT', 'commandSubmit')
  AND event_time > current_timestamp() - INTERVAL 1 DAY
ORDER BY event_time DESC;
```

---

## 9.6 External Locations & Storage Credentials

```sql
-- Create storage credential (admin task)
CREATE STORAGE CREDENTIAL my_cred
WITH AZURE_MANAGED_IDENTITY = (CONNECTOR_ID = '/subscriptions/.../connectors/myConnector');

-- Create external location
CREATE EXTERNAL LOCATION my_ext_loc
URL 'abfss://container@storage.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL my_cred);

-- Validate
VALIDATE STORAGE CREDENTIAL my_cred;
```

---

## Key Exam Points

- Unity Catalog uses 3-level namespace: **catalog.schema.table**
- `GRANT` / `REVOKE` syntax is SQL-standard
- Column masking and row filters are set via ALTER TABLE
- Audit logs stored in `system.access.audit`
- External locations require storage credentials
- Data lineage is automatic - no setup needed
- Metastore is regional; workspaces attach to metastore
- `information_schema` views available in each catalog

---

## Practice Tasks
- [ ] Create a catalog, schema, and managed Delta table
- [ ] Grant SELECT to an analyst group, MODIFY to engineers
- [ ] Implement column masking on a PII column
- [ ] Query audit logs for recent SELECT operations
- [ ] Create external location with storage credential
