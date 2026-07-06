# Day 3 Practice Tasks — Delta Lake Internals

## Task 1 — Delta Table Inspection
For a Delta table `orders_clean`, run the following and interpret each output:
```sql
DESCRIBE HISTORY orders_clean;
DESCRIBE DETAIL orders_clean;
```
> What do the `operation`, `operationParameters`, and `numFiles` columns tell you?

---

## Task 2 — Time Travel
1. Query `orders_clean` as it was 2 versions ago using `VERSION AS OF`.
2. Query it as it was yesterday using `TIMESTAMP AS OF`.
3. Restore the table to version 1 using `RESTORE TABLE`.

```sql
-- Your SQL here
```

---

## Task 3 — MERGE INTO
Write a `MERGE INTO` statement to upsert records from `orders_updates` into `orders_clean`:
- Match on `order_id`
- When matched and `status` changed → UPDATE all fields
- When not matched → INSERT
- When matched and `status = 'deleted'` → DELETE

```sql
-- Your MERGE here
```

---

## Task 4 — Optimize & Z-Order
1. Run `OPTIMIZE orders_clean ZORDER BY (customer_id, order_date)` and note the output.
2. Run `VACUUM orders_clean RETAIN 168 HOURS` and explain what it removes.
3. Why is it dangerous to VACUUM with `RETAIN 0 HOURS`?

---

## Task 5 — Clone Scenarios
For each scenario, choose `DEEP CLONE` or `SHALLOW CLONE` and justify:

| Scenario | Clone Type | Reason |
|---|---|---|
| Create a UAT copy for testing that needs independent history | | |
| Create a lightweight metadata snapshot for fast schema check | | |
| Replicate table to another workspace for DR | | |

---

## Task 6 — Concept Short-Answer
1. What is the Delta transaction log (`_delta_log`) and why is it important?
2. What does `delta.deletedFileRetentionDuration` control?
3. What is Change Data Feed (CDF) and when would you enable it?
