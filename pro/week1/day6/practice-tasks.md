# Day 6 Practice Tasks — Unity Catalog & Data Governance

## Task 1 — Namespace Navigation
Write SQL commands to:
1. List all catalogs in the metastore
2. List all schemas in the `training` catalog
3. List all tables in `training.prep`
4. Show the full lineage path for a table `training.prep.orders_clean`

```sql
-- Your SQL here
```

---

## Task 2 — Grant & Revoke
Write GRANT statements for:
1. Give analyst `alice@company.com` SELECT on `training.prep.orders_clean`
2. Give data engineer group `de_team` CREATE TABLE on schema `training.prep`
3. Give `bob@company.com` ALL PRIVILEGES on catalog `training`
4. Revoke SELECT from `alice@company.com` on `training.prep.orders_clean`

```sql
-- Your SQL here
```

---

## Task 3 — External Location & Storage Credentials
Answer the following:
1. What is the difference between a **Storage Credential** and an **External Location** in Unity Catalog?
2. Why can't you directly read from ADLS Gen2 without creating an External Location?
3. Write the SQL to create an External Location (fill in the placeholders):
```sql
CREATE EXTERNAL LOCATION my_adls
URL 'abfss://container@account.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL _____);
```

---

## Task 4 — Row & Column Security
1. Create a **dynamic view** `orders_masked` on `orders_clean` that:
   - Hides `customer_email` for all users except `de_admin` group
   - Filters rows to show only orders where `region = current_user_region()`

```sql
-- Your dynamic view here
```

---

## Task 5 — Data Lineage Quiz
For each action, state whether Unity Catalog **automatically captures** lineage:

| Action | Lineage Captured? |
|---|---|
| `CREATE TABLE new_table AS SELECT * FROM source_table` | |
| Writing via `df.write.saveAsTable("new_table")` in Python | |
| Direct ADLS write bypassing Unity Catalog | |
| DLT pipeline writing to a UC-managed table | |

---

## Task 6 — Concept Short-Answer
1. What is a **metastore** and how many can exist per Databricks account region?
2. What is the difference between `OWNER`, `USAGE`, and `SELECT` privileges?
3. What does `IS OWNER OF` mean in the context of Unity Catalog securable objects?
