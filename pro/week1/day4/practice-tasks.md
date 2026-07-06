# Day 4 — Practice Tasks: CDC with APPLY CHANGES INTO

Complete these tasks in order. Check the answers at the bottom only after attempting.

---

## Task 1 — Identify CDC Operation Types

Given a CDC source with column `op` containing values `I`, `U`, `D`, write the `APPLY CHANGES INTO` clauses that map these to INSERT/UPDATE/DELETE.

**Your answer:**
```sql
APPLY CHANGES INTO LIVE.target
FROM STREAM(LIVE.source)
KEYS (id)
APPLY AS DELETE WHEN op = '___'
SEQUENCE BY ts;
```

---

## Task 2 — SCD Type 1 vs Type 2

For each scenario below, pick SCD Type 1 or SCD Type 2:

| Scenario | SCD Type |
|----------|----------|
| Store only latest customer address | ? |
| Audit trail of all price changes | ? |
| Overwrite stock quantity updates | ? |
| Point-in-time employee records | ? |

---

## Task 3 — SCD Type 2 Query

You have a `customers_silver` table with SCD Type 2 columns `__START_AT` and `__END_AT`.

Write a query to return **all active (current) records**:

```sql
SELECT * FROM training.prep.customers_silver
WHERE ___;
```

---

## Task 4 — MERGE INTO for CDC

Write a `MERGE INTO` statement that:
- Updates `amount` and `customer` when `order_id` matches and operation is UPDATE
- Inserts a new row when `order_id` is new and operation is INSERT
- Deletes the row when operation is DELETE

```sql
MERGE INTO training.prep.orders_silver t
USING (
  SELECT * FROM cdc_source WHERE operation IN ('INSERT','UPDATE','DELETE')
) s
ON t.order_id = s.order_id
WHEN MATCHED AND s.operation = 'DELETE'   THEN ___
WHEN MATCHED AND s.operation = 'UPDATE'   THEN UPDATE SET ___
WHEN NOT MATCHED AND s.operation = 'INSERT' THEN ___;
```

---

## Task 5 — Python API

Convert this SQL `APPLY CHANGES INTO` to Python DLT API calls:

```sql
APPLY CHANGES INTO LIVE.orders_silver
FROM STREAM(LIVE.orders_raw)
KEYS (order_id)
APPLY AS DELETE WHEN op = 'D'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2;
```

```python
import dlt

dlt.create_streaming_table("orders_silver")

dlt.apply_changes(
    target=___,
    source=___,
    keys=___,
    sequence_by=___,
    apply_as_deletes=___,
    except_column_list=___,
    stored_as_scd_type=___
)
```

---

## Answers

<details>
<summary>Click to reveal</summary>

**Task 1:** `op = 'D'`

**Task 2:**
| Scenario | SCD Type |
|----------|----------|
| Latest address | SCD Type 1 |
| Price change audit | SCD Type 2 |
| Stock quantity | SCD Type 1 |
| Point-in-time employee | SCD Type 2 |

**Task 3:** `WHERE __END_AT IS NULL`

**Task 4:**
```sql
WHEN MATCHED AND s.operation = 'DELETE'   THEN DELETE
WHEN MATCHED AND s.operation = 'UPDATE'   THEN UPDATE SET t.amount = s.amount, t.customer = s.customer
WHEN NOT MATCHED AND s.operation = 'INSERT' THEN INSERT (order_id, customer, amount) VALUES (s.order_id, s.customer, s.amount)
```

**Task 5:**
```python
dlt.apply_changes(
    target="orders_silver",
    source="orders_raw",
    keys=["order_id"],
    sequence_by="event_ts",
    apply_as_deletes="op = 'D'",
    except_column_list=["op"],
    stored_as_scd_type=2
)
```

</details>
