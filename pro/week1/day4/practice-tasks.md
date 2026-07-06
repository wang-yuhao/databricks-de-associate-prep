# Day 4 Practice Tasks — Delta Live Tables (DLT)

## Task 1 — Bronze Table Definition
Write a DLT Python notebook cell that:
- Creates a **streaming live table** called `orders_bronze`
- Reads from Auto Loader path `dbfs:/FileStore/incoming/orders/`
- Adds metadata columns: `_ingest_timestamp` and `_source_file`

```python
# Your DLT cell here
```

---

## Task 2 — Silver Table with Expectations
Write a DLT cell for `orders_silver` that:
- Reads from `STREAM(LIVE.orders_bronze)`
- Applies `EXPECT` constraint on `order_id IS NOT NULL` → **drop** invalid rows
- Applies `EXPECT` constraint on `amount > 0` → **warn** (don't drop)
- Selects and casts key columns

```python
# Your DLT cell here
```

---

## Task 3 — Gold Aggregation Table
Create a materialized live table `orders_gold_daily` that:
- Aggregates `orders_silver` by `order_date` and `customer_segment`
- Computes `total_orders`, `total_revenue`, `avg_order_value`

```python
# Your DLT cell here
```

---

## Task 4 — Pipeline Modes
Answer the following:
1. When would you use **Triggered** mode vs **Continuous** mode for a DLT pipeline?
2. What happens to a DLT pipeline's tables if you switch from development to production mode?
3. Can a DLT pipeline contain both streaming and batch (materialized) tables? Explain.

---

## Task 5 — DLT Expectations Analysis
Given a DLT table with these constraints:
```python
@dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dlt.expect("positive_amount", "amount > 0")
@dlt.expect_or_fail("valid_status", "status IN ('placed','shipped','delivered','cancelled')")
```
For each row below, state whether it is **kept**, **dropped**, or causes a **pipeline failure**:

| order_id | amount | status | Result |
|---|---|---|---|
| ORD001 | 50.0 | placed | |
| NULL | 50.0 | placed | |
| ORD003 | -10.0 | shipped | |
| ORD004 | 20.0 | refunded | |
