# Day 6 Practice Tasks — Unity Catalog & Data Governance

> **Exam section:** Data Governance (20%), Security & Compliance (15%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## How to Use These Tasks

Work through each task **in order** — each one builds on the last. Every task has:

- 📖 **Context** — why this matters for the exam
- 🛠️ **Instructions** — what you must do, step by step
- ✅ **Expected outcome** — how to verify your answer
- ⚠️ **Exam trap** — a common wrong-answer pitfall

---

## Task 1 — Unity Catalog Three-Level Namespace

📖 **Context**: Unity Catalog uses a 3-level namespace: `catalog.schema.table`. The Professional exam tests your understanding of this hierarchy.

🛠️ **Instructions**:

### Step 1 — Navigate the namespace:

```sql
-- List all catalogs in the metastore
SHOW CATALOGS;

-- List all schemas in training_uc catalog
SHOW SCHEMAS IN training_uc;

-- List all tables in training_uc.bronze schema
SHOW TABLES IN training_uc.bronze;

-- Show full lineage path
DESCRIBE EXTENDED training_uc.bronze.orders_clean;
```

### Step 2 — Create namespace hierarchy:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS training_uc;

-- Create schemas (Bronze, Silver, Gold pattern)
CREATE SCHEMA IF NOT EXISTS training_uc.bronze;
CREATE SCHEMA IF NOT EXISTS training_uc.silver;
CREATE SCHEMA IF NOT EXISTS training_uc.gold;

-- Create table with full namespace
CREATE TABLE training_uc.bronze.orders (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  amount DECIMAL(10,2),
  status STRING
) USING DELTA;
```

### Step 3 — Set default catalog and schema:

```sql
-- Set default catalog (session-level)
USE CATALOG training_uc;

-- Set default schema
USE SCHEMA bronze;

-- Now you can query without full namespace
SELECT * FROM orders LIMIT 10;
```

✅ **Expected outcome**: 
- Understand 3-level namespace: `catalog.schema.table`
- Can navigate and query at any level
- Default catalog/schema simplifies queries

⚠️ **Exam trap**: Thinking you can skip levels. Wrong! You MUST always use `catalog.schema.table` format. `schema.table` alone doesn't work in Unity Catalog (unless defaults are set).

---

## Task 2 — GRANT and REVOKE Permissions

📖 **Context**: Unity Catalog uses standard SQL GRANT/REVOKE syntax. The exam tests privilege inheritance and minimum required privileges.

🛠️ **Instructions**:

### Step 1 — Grant SELECT permission:

```sql
-- To query a table, user needs THREE privileges:
-- 1. USE CATALOG
-- 2. USE SCHEMA  
-- 3. SELECT on table

GRANT USE CATALOG ON CATALOG training_uc TO `alice@company.com`;
GRANT USE SCHEMA ON SCHEMA training_uc.bronze TO `alice@company.com`;
GRANT SELECT ON TABLE training_uc.bronze.orders_clean TO `alice@company.com`;
```

### Step 2 — Grant CREATE TABLE permission:

```sql
-- To create tables, group needs:
-- 1. USE CATALOG
-- 2. USE SCHEMA
-- 3. CREATE TABLE on schema

GRANT USE CATALOG ON CATALOG training_uc TO `de_team`;
GRANT USE SCHEMA ON SCHEMA training_uc.bronze TO `de_team`;
GRANT CREATE TABLE ON SCHEMA training_uc.bronze TO `de_team`;
```

### Step 3 — Grant ALL PRIVILEGES:

```sql
-- Grant full control over catalog
GRANT ALL PRIVILEGES ON CATALOG training_uc TO `bob@company.com`;

-- This includes: CREATE SCHEMA, USE CATALOG, and ownership
```

### Step 4 — Revoke permissions:

```sql
-- Revoke specific privilege
REVOKE SELECT ON TABLE training_uc.bronze.orders_clean FROM `alice@company.com`;

-- Revoke all privileges
REVOKE ALL PRIVILEGES ON CATALOG training_uc FROM `bob@company.com`;
```

### Step 5 — View granted permissions:

```sql
-- Show grants on a specific table
SHOW GRANTS ON TABLE training_uc.bronze.orders_clean;

-- Show grants for a specific user
SHOW GRANTS TO `alice@company.com`;
```

✅ **Expected outcome**: 
- Understand privilege hierarchy: catalog → schema → table
- Know minimum privileges required for each action
- Can grant/revoke to users and groups

⚠️ **Exam trap**: Thinking `SELECT` privilege alone is enough. Wrong! You MUST also grant `USE CATALOG` and `USE SCHEMA` for the user to actually query the table.

---

## Task 3 — External Locations and Storage Credentials

📖 **Context**: Unity Catalog governs external storage via Storage Credentials and External Locations. This is THE most tested governance feature.

🛠️ **Instructions**:

### Step 1 — Understand the relationship:

```
Storage Credential (auth)  →  External Location (path)  →  External Table
       ↓                              ↓                          ↓
   Azure SPN                  abfss://container@...      CREATE TABLE ... LOCATION
```

**Storage Credential**: Contains authentication (Azure Service Principal, AWS IAM role, etc.)
**External Location**: Maps a cloud storage path to a credential and grants access

### Step 2 — Create Storage Credential (admin only):

```sql
-- Azure example
CREATE STORAGE CREDENTIAL azure_spn
WITH (AZURE_SERVICE_PRINCIPAL
  DIRECTORY_ID = '<tenant-id>',
  APPLICATION_ID = '<client-id>',
  CLIENT_SECRET = '<client-secret>'
);
```

### Step 3 — Create External Location:

```sql
CREATE EXTERNAL LOCATION my_adls_bronze
URL 'abfss://bronze@mystorageaccount.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL azure_spn);

-- Grant access to data engineers
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION my_adls_bronze TO `de_team`;
```

### Step 4 — Create External Table:

```sql
CREATE EXTERNAL TABLE training_uc.bronze.external_orders (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  amount DECIMAL(10,2)
)
LOCATION 'abfss://bronze@mystorageaccount.dfs.core.windows.net/orders/';
```

### Step 5 — Key differences:

| Feature | Managed Table | External Table |
|---------|---------------|----------------|
| **Data location** | UC-managed | User-specified |
| **DROP behavior** | Deletes data AND metadata | Deletes metadata ONLY |
| **Use case** | Production tables | Raw data, data lake integration |
| **Requires** | Only catalog/schema | External Location + privileges |

✅ **Expected outcome**: 
- Storage Credential provides authentication
- External Location maps path + credential
- External tables point to user-managed storage
- DROP external table keeps data files

⚠️ **Exam trap**: Thinking you can directly read from ADLS without External Location. Wrong! Unity Catalog REQUIRES External Locations for governed access to external storage. Direct ADLS access bypasses governance.

---

## Task 4 — Dynamic Row and Column Masking

📖 **Context**: Unity Catalog supports row filters and column masks WITHOUT creating multiple views. The exam tests `current_user()` and `is_member()` functions.

🛠️ **Instructions**:

### Step 1 — Create row filter:

```sql
-- Create function that filters rows by region
CREATE FUNCTION training_uc.bronze.region_filter(user_region STRING)
RETURN IF(is_member('admin_group'), TRUE, user_region = 'EU');

-- Apply row filter to table
ALTER TABLE training_uc.bronze.orders 
SET ROW FILTER training_uc.bronze.region_filter(region);

-- Now queries automatically filter:
-- - Admins see ALL rows
-- - Non-admins see ONLY EU rows
SELECT * FROM training_uc.bronze.orders;
```

### Step 2 — Create column mask:

```sql
-- Create masking function
CREATE FUNCTION training_uc.bronze.email_mask(email STRING)
RETURN CASE 
  WHEN is_member('de_admin') THEN email
  ELSE '***REDACTED***'
END;

-- Apply mask to column
ALTER TABLE training_uc.bronze.orders
ALTER COLUMN customer_email 
SET MASK training_uc.bronze.email_mask;

-- Now queries automatically mask:
-- - Admins see real email
-- - Others see '***REDACTED***'
SELECT customer_email FROM training_uc.bronze.orders;
```

### Step 3 — Key functions:

| Function | Returns | Use Case |
|----------|---------|----------|
| `current_user()` | Email of current user | Row-level filtering by user |
| `is_member('group')` | Boolean | Check Azure AD group membership |
| `current_region()` | Custom | User-defined function for filtering |

✅ **Expected outcome**: 
- Row filters restrict which rows users see
- Column masks hide sensitive column data
- `is_member()` checks Azure AD groups automatically
- No need for separate views per user/group

⚠️ **Exam trap**: Thinking row filters return a STRING. Wrong! Row filters MUST return BOOLEAN. The function decides whether to INCLUDE the row (TRUE) or EXCLUDE it (FALSE).

---

## Task 5 — Data Lineage

📖 **Context**: Unity Catalog automatically captures column-level lineage. The exam tests when lineage IS and ISN'T captured.

🛠️ **Instructions**:

### Lineage Capture Matrix:

| Action | Lineage Captured? | Why |
|--------|------------------|-----|
| `CREATE TABLE new AS SELECT * FROM source` | ✅ YES | UC-managed operation |
| `df.write.saveAsTable("new")` in PySpark | ✅ YES | Writes through UC |
| `INSERT INTO target SELECT * FROM source` | ✅ YES | SQL operation through UC |
| DLT pipeline writing to UC table | ✅ YES | DLT integrates with UC |
| Direct ADLS write bypassing UC | ❌ NO | Bypasses UC governance |
| External tool (Fivetran, Airbyte) → UC | ✅ YES | Writes through UC APIs |
| `df.write.parquet("adls://...")` | ❌ NO | Direct write, no UC involvement |

### How to view lineage:

1. **Catalog Explorer UI**:
   - Go to Catalog Explorer
   - Select table
   - Click **Lineage** tab
   - See upstream sources and downstream dependencies

2. **SQL queries**:
```sql
-- View table lineage metadata
DESCRIBE EXTENDED training_uc.bronze.orders;

-- System tables (if available)
SELECT * FROM system.access.table_lineage 
WHERE target_table_name = 'orders';
```

✅ **Expected outcome**: 
- Lineage is AUTOMATIC (no configuration needed)
- Only captures operations through Unity Catalog
- Column-level lineage shows data flow
- Direct storage writes bypass lineage

⚠️ **Exam trap**: Thinking lineage tracks ALL data operations. Wrong! Lineage only tracks operations that go THROUGH Unity Catalog. Direct storage access or external tools that don't use UC APIs won't be tracked.

---

## Task 6 — Metastore and Account Structure

📖 **Context**: The exam tests your understanding of Unity Catalog's multi-tenancy architecture.

🛠️ **Instructions**:

### Unity Catalog hierarchy:

```
Databricks Account
    ↓
Metastore (per region)
    ↓
Workspaces (attached to metastore)
    ↓
Catalogs (within metastore)
    ↓
Schemas (within catalog)
    ↓
Tables, Views, Functions (within schema)
```

### Key concepts:

| Concept | Description | Quantity |
|---------|-------------|----------|
| **Account** | Top-level Databricks account | 1 per organization |
| **Metastore** | Logical container for metadata | 1 per region (can have multiple) |
| **Workspace** | Databricks workspace | Many per account |
| **Catalog** | Top-level data namespace | Many per metastore |
| **Schema** | Collection of tables | Many per catalog |

### Privileges hierarchy:

```sql
-- Catalog-level (inherits to all schemas/tables)
GRANT USE CATALOG ON CATALOG training_uc TO `data_analysts`;

-- Schema-level (inherits to all tables)
GRANT SELECT ON SCHEMA training_uc.bronze TO `data_analysts`;

-- Table-level (specific table only)
GRANT MODIFY ON TABLE training_uc.bronze.orders TO `de_team`;
```

**Privilege types:**
- `OWNER`: Full control (can grant to others)
- `ALL PRIVILEGES`: All permissions except ownership transfer
- `USE CATALOG`: Can see catalog exists
- `USE SCHEMA`: Can see schema exists
- `SELECT`: Read data
- `MODIFY`: Insert, update, delete
- `CREATE TABLE`: Create tables in schema

✅ **Expected outcome**: 
- Metastore is per-region, not per-workspace
- Multiple workspaces can share ONE metastore
- Privileges inherit down the hierarchy
- `OWNER` can transfer ownership

⚠️ **Exam trap**: Thinking each workspace has its own metastore. Wrong! Multiple workspaces typically SHARE one metastore per region. This enables cross-workspace data sharing.

---

## Task 7 — Concept Quiz

Answer these rapid-fire questions:

1. What are the three levels in Unity Catalog's namespace?
2. What THREE privileges are required to SELECT from a table?
3. What is the difference between a Storage Credential and an External Location?
4. What happens to data files when you DROP an External Table?
5. What does `is_member('group')` check?
6. Must row filter functions return BOOLEAN or STRING?
7. When is data lineage automatically captured?
8. How many metastores can exist in one region?
9. What is the difference between `OWNER` and `ALL PRIVILEGES`?
10. Can multiple workspaces share one metastore?

---

## Key Takeaways for the Exam

✅ **Three-Level Namespace:**
- Format: `catalog.schema.table`
- ALL references must use full namespace (unless defaults set)
- Hierarchy: Metastore → Catalog → Schema → Table

✅ **Privileges:**
- **Minimum for SELECT**: `USE CATALOG` + `USE SCHEMA` + `SELECT`
- **Privileges inherit** from catalog → schema → table
- **OWNER** can grant/revoke and transfer ownership
- **ALL PRIVILEGES** = everything except ownership transfer

✅ **External Storage:**
- **Storage Credential**: Contains authentication (SPN, IAM role)
- **External Location**: Maps path + credential
- **External Table**: References user-managed storage
- **DROP External Table**: Deletes metadata, KEEPS data files

✅ **Row/Column Security:**
- **Row Filter**: Function returning BOOLEAN (TRUE = include row)
- **Column Mask**: Function returning masked value
- **`is_member('group')`**: Checks Azure AD group membership
- **`current_user()`**: Returns user's email

✅ **Lineage:**
- **Automatic** for all UC operations
- **Column-level** granularity
- **NOT captured** for direct storage writes bypassing UC
- View in Catalog Explorer or via SQL

✅ **Metastore:**
- **One per region** (can have multiple)
- **Multiple workspaces** can share one metastore
- **Cross-workspace** data sharing enabled
- **Account-level** resource

---

## Next Steps

You've completed Day 6! You now understand Unity Catalog and Data Governance at a professional level. Tomorrow (Day 7), you'll cover Databricks Asset Bundles (DABs) and CI/CD.
