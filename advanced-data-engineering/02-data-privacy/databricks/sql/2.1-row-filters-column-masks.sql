-- Demo 2.1 on a real Unity Catalog workspace (Databricks Free Edition works).
-- Run in a SQL editor or notebook. Replace group names with groups that exist in your account
-- (Free Edition: use your own user name or the built-in `account users` group).
-- Free Edition has no custom account groups (only `account users`): is_account_group_member() returns false for admins/eu_analysts/
-- us_analysts/pii_readers (no error), so after step 4 YOU see 0 rows and masked values. That is the point:
-- policies apply to every user, including the table owner.
-- Shares workspace.privacy_course.customers with the 2.1 notebook; each one recreates the table.

CREATE SCHEMA IF NOT EXISTS workspace.privacy_course;
USE workspace.privacy_course;

-- 1. A table with PII (a few synthetic rows; load more with CTAS from any source)
-- DROP first: CREATE OR REPLACE would keep a row filter (and masks on same-named columns) from an earlier run.
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  user_id BIGINT, first_name STRING, last_name STRING, email STRING, phone STRING, ssn STRING,
  dob DATE, gender STRING, city STRING, zip STRING, region STRING, salary BIGINT);

INSERT INTO customers VALUES
  (1, 'Ana',   'Silva',  'ana.silva@example.com',  '+351 912 000 001', '123-45-6701', DATE'1988-04-12', 'F', 'Lisbon',  '1100', 'EU', 52000),
  (2, 'Jonas', 'Weber',  'jonas.weber@example.com', '+49 151 0000002',  '123-45-6702', DATE'1975-11-30', 'M', 'Berlin',  '10115', 'EU', 71000),
  (3, 'Maria', 'Lopez',  'maria.lopez@example.com', '+1 415 555 0103',  '123-45-6703', DATE'1992-02-07', 'F', 'Austin',  '73301', 'US', 64000),
  (4, 'Sam',   'Carter', 'sam.carter@example.com',  '+1 212 555 0104',  '123-45-6704', DATE'1983-08-21', 'M', 'New York', '10001', 'US', 88000);

-- 2. Classify: tags (Data Classification can suggest them automatically)
ALTER TABLE customers ALTER COLUMN email SET TAGS ('pii' = 'email');
ALTER TABLE customers ALTER COLUMN ssn   SET TAGS ('pii' = 'national_id');
ALTER TABLE customers SET TAGS ('data_classification' = 'confidential');

-- 3. Policy functions (identical to the local notebook)
CREATE OR REPLACE FUNCTION region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('admins')
    OR (region = 'EU' AND is_account_group_member('eu_analysts'))
    OR (region = 'US' AND is_account_group_member('us_analysts'));

CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE regexp_replace(email, '^[^@]+', '*****') END;

CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('admins') THEN ssn ELSE concat('***-**-', right(ssn, 4)) END;

-- Which groups does your identity resolve to? (is_member = workspace-local groups, e.g. admins.)
SELECT current_user() AS me, is_member('admins') AS workspace_admin,
       is_account_group_member('admins') AS account_admins_group, is_account_group_member('pii_readers') AS pii_reader;

-- 4. Attach them to the table: every query path (SQL, Python, BI) is now filtered and masked.
SELECT * FROM customers;   -- before: 4 rows in clear text
ALTER TABLE customers SET ROW FILTER region_filter ON (region);
ALTER TABLE customers ALTER COLUMN email SET MASK mask_email;
ALTER TABLE customers ALTER COLUMN ssn   SET MASK mask_ssn;
SELECT * FROM customers;   -- after: only rows your groups allow (0 on Free Edition), masked email/ssn
DESCRIBE TABLE EXTENDED customers;   -- shows the row filter and column masks

-- 5. Alternative: dynamic view (grant SELECT on the view, not on the table).
--    Use either table policies (step 4) or a dynamic view, not both (masks would be applied twice).
ALTER TABLE customers DROP ROW FILTER;
ALTER TABLE customers ALTER COLUMN email DROP MASK;
ALTER TABLE customers ALTER COLUMN ssn   DROP MASK;
CREATE OR REPLACE VIEW customers_secure AS
SELECT user_id, mask_email(email) AS email, mask_ssn(ssn) AS ssn, city, region
FROM customers
WHERE region_filter(region);
SELECT * FROM customers_secure;   -- same result as step 4: the functions check the querying user

-- 6. Privileges (a reader needs USE CATALOG + USE SCHEMA + SELECT)
-- USE CATALOG on `workspace` is normally already granted to workspace users, and granting it needs catalog
-- ownership/MANAGE, which a Free Edition user may not have; uncomment on a workspace where you own the catalog.
-- GRANT USE CATALOG ON CATALOG workspace TO `account users`;
GRANT USE SCHEMA ON SCHEMA workspace.privacy_course TO `account users`;
GRANT SELECT ON VIEW customers_secure TO `account users`;
SHOW GRANTS ON TABLE customers;

-- 7-8. System tables may be unavailable on Free Edition (TABLE_OR_VIEW_NOT_FOUND / permission denied); if so,
--      use Catalog Explorer -> customers -> Lineage / History instead. They also lag by minutes to hours.
-- 7. Audit: who read the table in the last 7 days?
SELECT event_time, user_identity.email, action_name, request_params.full_name_arg AS object
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND request_params.full_name_arg = 'workspace.privacy_course.customers'
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC;

-- 8. Lineage: where does this table flow downstream?
SELECT target_table_full_name, entity_type, event_time
FROM system.access.table_lineage
WHERE source_table_full_name = 'workspace.privacy_course.customers'
ORDER BY event_time DESC;

-- Exam notes (Data Engineer Professional)
-- - Row filter = SQL UDF returning BOOLEAN (ALTER TABLE t SET ROW FILTER f ON (cols)); column mask = UDF returning the
--   column's type (ALTER COLUMN c SET MASK f [USING COLUMNS (...)]). One row filter per table, one mask per column.
-- - Policies apply to every reader, including the owner; setting them needs table ownership (or MANAGE + SELECT)
--   and EXECUTE on the function. Change the logic with CREATE OR REPLACE FUNCTION, no ALTER TABLE needed.
-- - is_account_group_member() checks account-level groups and is what UC policies should use; is_member() checks
--   workspace-local groups. User-context functions run as the invoker, the rest of the filter with definer's rights.
-- - REPLACE TABLE keeps an existing row filter (and masks on same-named columns), so use DROP TABLE to reset.
-- - Dynamic view = older alternative: grant SELECT on the view only; it protects nothing for users with SELECT on
--   the base table. Row filters/masks cannot be set on a view, and tables with them don't support time travel or clones.
-- - Reading a table needs USE CATALOG + USE SCHEMA + SELECT; audit = system.access.audit, lineage = system.access.table_lineage.
