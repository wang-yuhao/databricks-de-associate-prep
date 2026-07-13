# Day 3 Practice Tasks — Delta Lake Internals

> **Exam section:** Data Transformation, Cleansing, and Quality (10%), Data Ingestion (25%)
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

## Task 1 — Delta Table Inspection with DESCRIBE HISTORY & DETAIL

📖 **Context**: The Professional exam tests your ability to read Delta metadata. You must interpret `DESCRIBE HISTORY` and `DESCRIBE DETAIL` outputs.

🛠️ **Instructions**:

### Step 1 — Create a test Delta table:

```sql
CREATE OR REPLACE TABLE training_uc.bronze.orders_clean (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  amount DECIMAL(10,2),
  status STRING
) USING DELTA;

INSERT INTO training_uc.bronze.orders_clean VALUES
  ('ORD001', 'CUST123', '2025-01-01', 150.00, 'shipped'),
  ('ORD002', 'CUST456', '2025-01-02', 250.00, 'delivered');

UPDATE training_uc.bronze.orders_clean 
SET status = 'cancelled' 
WHERE order_id = 'ORD001';

DELETE FROM training_uc.bronze.orders_clean 
WHERE order_id = 'ORD002';
```

### Step 2 — Inspect metadata:

```sql
DESCRIBE HISTORY training_uc.bronze.orders_clean;
DESCRIBE DETAIL training_uc.bronze.orders_clean;
```

### Step 3 — Answer these:

1. What does the `operation` column show for each transaction?
2. What is in `operationParameters` for the UPDATE?
3. How does `numFiles` change after each operation?
4. What is the table's current `location` from DESCRIBE DETAIL?

✅ **Expected outcome**: 
- `DESCRIBE HISTORY` shows 4 operations: CREATE, WRITE, UPDATE, DELETE
- `operationParameters` for UPDATE shows `{"predicate": "[...]"}`
- `numFiles` increases with each write operation
- `DESCRIBE DETAIL` shows metadata like location, format (delta), and partition columns

⚠️ **Exam trap**: Thinking `DESCRIBE HISTORY` shows all versions forever. Wrong! History is retained based on `delta.logRetentionDuration` (default 30 days). After VACUUM, old history is removed.

---

## Task 2 — Time Travel with VERSION AS OF and TIMESTAMP AS OF

📖 **Context**: Time travel is THE most tested Delta Lake feature on the Professional exam. You must know VERSION AS OF vs TIMESTAMP AS OF.

🛠️ **Instructions**:

### Step 1 — Query historical versions:

```sql
-- Query version 0 (initial state)
SELECT * FROM training_uc.bronze.orders_clean VERSION AS OF 0;

-- Query version 1 (after first INSERT)
SELECT * FROM training_uc.bronze.orders_clean VERSION AS OF 1;

-- Query as of yesterday (replace with your timestamp)
SELECT * FROM training_uc.bronze.orders_clean TIMESTAMP AS OF '2025-01-15T10:00:00';
```

### Step 2 — Restore to previous version:

```sql
RESTORE TABLE training_uc.bronze.orders_clean TO VERSION AS OF 1;

SELECT * FROM training_uc.bronze.orders_clean;
```

✅ **Expected outcome**: 
- VERSION AS OF 0 shows empty table (right after CREATE)
- VERSION AS OF 1 shows 2 rows (after INSERT)
- RESTORE brings back the 2 original rows, undoing UPDATE and DELETE
- DESCRIBE HISTORY now shows a new RESTORE operation

⚠️ **Exam trap**: Thinking RESTORE deletes future versions. Wrong! RESTORE creates a NEW version that restores old data. The history is never deleted (until VACUUM runs).

---

## Task 3 — MERGE INTO for Upserts and Deletes

📖 **Context**: MERGE is critical for CDC (Change Data Capture) patterns. The Professional exam tests all three clauses: WHEN MATCHED UPDATE, WHEN NOT MATCHED INSERT, WHEN MATCHED DELETE.

🛠️ **Instructions**:

### Step 1 — Create update source:

```sql
CREATE OR REPLACE TEMP VIEW orders_updates AS
SELECT 'ORD001' as order_id, 'CUST123' as customer_id, '2025-01-01' as order_date, 180.00 as amount, 'shipped' as status
UNION ALL
SELECT 'ORD002', 'CUST456', '2025-01-02', 250.00, 'deleted'
UNION ALL
SELECT 'ORD003', 'CUST789', '2025-01-03', 99.00, 'placed';
```

### Step 2 — Write MERGE statement:

```sql
MERGE INTO training_uc.bronze.orders_clean AS target
USING orders_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.status = 'deleted' THEN DELETE
WHEN MATCHED AND source.status != 'deleted' THEN UPDATE SET
  target.customer_id = source.customer_id,
  target.order_date = source.order_date,
  target.amount = source.amount,
  target.status = source.status
WHEN NOT MATCHED AND source.status != 'deleted' THEN INSERT (
  order_id, customer_id, order_date, amount, status
) VALUES (
  source.order_id, source.customer_id, source.order_date, source.amount, source.status
);
```

### Step 3 — Verify results:

```sql
SELECT * FROM training_uc.bronze.orders_clean ORDER BY order_id;
DESCRIBE HISTORY training_uc.bronze.orders_clean;
```

✅ **Expected outcome**: 
- ORD001: amount updated from 150 to 180
- ORD002: deleted
- ORD003: inserted (new row)
- DESCRIBE HISTORY shows a MERGE operation

⚠️ **Exam trap**: The order of WHEN clauses matters! DELETE must come before UPDATE, or rows will be updated then deleted in the same operation. Also, forgetting `AND source.status != 'deleted'` in INSERT clause will try to insert deleted records.

---

## Task 4 — OPTIMIZE and Z-ORDER

📖 **Context**: OPTIMIZE is the #1 way to improve Delta table performance. The exam tests when to use ZORDER and what it does.

🛠️ **Instructions**:

### Step 1 — Check file stats before optimization:

```sql
DESCRIBE DETAIL training_uc.bronze.orders_clean;
```

Note the `numFiles` value.

### Step 2 — Run OPTIMIZE with ZORDER:

```sql
OPTIMIZE training_uc.bronze.orders_clean
ZORDER BY (customer_id, order_date);
```

### Step 3 — Check stats after:

```sql
DESCRIBE DETAIL training_uc.bronze.orders_clean;
DESCRIBE HISTORY training_uc.bronze.orders_clean;
```

### Step 4 — Run VACUUM:

```sql
-- First, check what would be removed (dry run)
VACUUM training_uc.bronze.orders_clean RETAIN 168 HOURS DRY RUN;

-- Then actually vacuum
VACUUM training_uc.bronze.orders_clean RETAIN 168 HOURS;
```

✅ **Expected outcome**: 
- OPTIMIZE reduces `numFiles` (compacts small files)
- ZORDER co-locates data by customer_id and order_date for faster queries
- VACUUM removes old Parquet files that are no longer referenced
- VACUUM output shows number of files deleted

⚠️ **Exam trap**: Running `VACUUM RETAIN 0 HOURS` breaks time travel! You cannot query old versions after VACUUM removes their files. Default retention is 7 days (168 hours). NEVER set to 0 in production.

---

## Task 5 — DEEP CLONE vs SHALLOW CLONE

📖 **Context**: The exam tests when to use DEEP vs SHALLOW clones. Key difference: DEEP copies data files, SHALLOW only copies metadata.

🛠️ **Instructions**:

### Step 1 — Create DEEP CLONE:

```sql
CREATE TABLE training_uc.bronze.orders_deep_clone
DEEP CLONE training_uc.bronze.orders_clean;

DESCRIBE DETAIL training_uc.bronze.orders_deep_clone;
```

### Step 2 — Create SHALLOW CLONE:

```sql
CREATE TABLE training_uc.bronze.orders_shallow_clone
SHALLOW CLONE training_uc.bronze.orders_clean;

DESCRIBE DETAIL training_uc.bronze.orders_shallow_clone;
```

### Step 3 — Compare locations:

Notice that DEEP clone has its own `location`, while SHALLOW clone points to the original files.

### Step 4 — Decision table:

| Scenario | Clone Type | Reason |
|----------|------------|--------|
| Create a UAT copy for testing that needs independent history | DEEP CLONE | Data is copied; changes to UAT don't affect production |
| Create a lightweight metadata snapshot for fast schema check | SHALLOW CLONE | No data copy; instant creation |
| Replicate table to another workspace for DR | DEEP CLONE | Must copy data to separate location |
| Create a quick dev snapshot for read-only testing | SHALLOW CLONE | Faster, cheaper, read-only is safe |

✅ **Expected outcome**: 
- DEEP clone takes longer and uses more storage
- SHALLOW clone is instant
- Both clones are independent tables in Unity Catalog

⚠️ **Exam trap**: Thinking SHALLOW clone is a "view". Wrong! It's a full table with its own transaction log. It just references the original data files. If you VACUUM the source, SHALLOW clone may break.

---

## Task 6 — Delta Transaction Log and Change Data Feed

📖 **Context**: The exam tests understanding of `_delta_log` directory and Change Data Feed (CDF).

🛠️ **Instructions**:

### Step 1 — Enable Change Data Feed:

```sql
ALTER TABLE training_uc.bronze.orders_clean
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### Step 2 — Make some changes:

```sql
INSERT INTO training_uc.bronze.orders_clean VALUES
  ('ORD004', 'CUST999', '2025-01-04', 500.00, 'placed');

UPDATE training_uc.bronze.orders_clean
SET status = 'shipped'
WHERE order_id = 'ORD004';

DELETE FROM training_uc.bronze.orders_clean
WHERE order_id = 'ORD001';
```

### Step 3 — Query Change Data Feed:

```sql
SELECT *
FROM table_changes('training_uc.bronze.orders_clean', 2)
ORDER BY _commit_version, _change_type;
```

✅ **Expected outcome**: 
- CDF output shows `_change_type` column: insert, update_preimage, update_postimage, delete
- Each row has `_commit_version` and `_commit_timestamp`
- You can see exactly what changed between versions

⚠️ **Exam trap**: CDF must be enabled BEFORE changes happen. It doesn't retroactively track old changes. Also, CDF adds storage overhead (~20-30%), so only enable when needed.

---

## Task 7 — Concept Quiz

Answer these rapid-fire questions:

1. What does `DESCRIBE HISTORY` show?
2. What is the difference between VERSION AS OF and TIMESTAMP AS OF?
3. Does RESTORE delete future versions?
4. In MERGE, which clause should come first: DELETE or UPDATE?
5. What does OPTIMIZE ZORDER BY do?
6. What is the default VACUUM retention period?
7. What happens if you VACUUM with RETAIN 0 HOURS?
8. When should you use DEEP CLONE vs SHALLOW CLONE?
9. Where is the Delta transaction log stored?
10. What is Change Data Feed (CDF) used for?

---

## Key Takeaways for the Exam

✅ **Delta Metadata:**
- `DESCRIBE HISTORY`: Shows operations (WRITE, UPDATE, DELETE, MERGE, OPTIMIZE, RESTORE)
- `DESCRIBE DETAIL`: Shows table metadata (location, format, partitions, numFiles)
- Transaction log is in `_delta_log/` directory (JSON files)

✅ **Time Travel:**
- `VERSION AS OF n`: Query specific version number
- `TIMESTAMP AS OF 'timestamp'`: Query at specific time
- `RESTORE TABLE`: Creates new version with old data (doesn't delete history)
- History retained for `delta.logRetentionDuration` (default 30 days)

✅ **MERGE:**
- Supports INSERT, UPDATE, DELETE in one atomic operation
- DELETE clause must come before UPDATE
- Use for CDC (Change Data Capture) patterns

✅ **Optimization:**
- `OPTIMIZE`: Compacts small files (bin-packing)
- `ZORDER BY`: Co-locates data by specified columns for faster queries
- `VACUUM`: Removes old data files no longer referenced
- Default VACUUM retention: 7 days (168 hours)
- NEVER use RETAIN 0 HOURS in production!

✅ **Cloning:**
- `DEEP CLONE`: Copies data files (independent table)
- `SHALLOW CLONE`: Copies metadata only (references original files)
- Use DEEP for production copies, SHALLOW for dev/test

✅ **Change Data Feed:**
- Must be enabled: `delta.enableChangeDataFeed = true`
- Tracks insert, update_preimage, update_postimage, delete
- Use `table_changes()` function to query changes
- Only tracks changes AFTER enablement

---

## Next Steps

You've completed Day 3! You now understand Delta Lake internals at a professional level. Tomorrow (Day 4), you'll build on this with Delta Live Tables (DLT).
