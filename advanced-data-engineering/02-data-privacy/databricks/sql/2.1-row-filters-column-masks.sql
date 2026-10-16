-- Demo 2.1 on a real Unity Catalog workspace (Databricks Free Edition works).
-- Run in a SQL editor or notebook. Replace group names with groups that exist in your account
-- (Free Edition: use your own user name or the built-in `account users` group).

CREATE SCHEMA IF NOT EXISTS workspace.privacy_course;
USE workspace.privacy_course;

-- 1. A table with PII (a few synthetic rows; load more with CTAS from any source)
CREATE OR REPLACE TABLE customers (
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

-- 4. Attach them to the table: every query path (SQL, Python, BI) is now filtered and masked.
--    Run SELECT * FROM customers before and after, and compare.
ALTER TABLE customers SET ROW FILTER region_filter ON (region);
ALTER TABLE customers ALTER COLUMN email SET MASK mask_email;
ALTER TABLE customers ALTER COLUMN ssn   SET MASK mask_ssn;

-- Remove again
-- ALTER TABLE customers DROP ROW FILTER;
-- ALTER TABLE customers ALTER COLUMN email DROP MASK;

-- 5. Alternative: dynamic view (grant SELECT on the view, not on the table).
--    Use either table policies (step 4) or a dynamic view; drop the policies first if you try the view.
CREATE OR REPLACE VIEW customers_secure AS
SELECT user_id, mask_email(email) AS email, mask_ssn(ssn) AS ssn, city, region
FROM customers
WHERE region_filter(region);

-- 6. Privileges (a reader needs USE CATALOG + USE SCHEMA + SELECT)
GRANT USE CATALOG ON CATALOG workspace TO `account users`;
GRANT USE SCHEMA ON SCHEMA workspace.privacy_course TO `account users`;
GRANT SELECT ON VIEW customers_secure TO `account users`;
SHOW GRANTS ON TABLE customers;

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
