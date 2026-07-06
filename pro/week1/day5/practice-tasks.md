# Day 5 — Practice Tasks: Advanced Structured Streaming

---

## Task 1 — Window Type Matching

Match each window type to its description:

| Window Type | Description |
|-------------|-------------|
| Tumbling    | ? |
| Sliding     | ? |
| Session     | ? |

Choices:
- A) Fixed-size, non-overlapping intervals
- B) Overlapping windows of fixed size, advancing at a smaller interval
- C) Variable size; closes after a period of inactivity

---

## Task 2 — Watermark Calculation

Your stream has `withWatermark("event_timestamp", "15 minutes")`. The latest event seen so far has timestamp `10:50`. What is the current watermark threshold?

A) 10:50  
B) 10:35  
C) 11:05  
D) Cannot be determined

---

## Task 3 — Output Mode Selection

For each requirement, pick the correct output mode:

| Requirement | Mode |
|-------------|------|
| Write only new events, no aggregations | ? |
| Running total per region, emit only changes | ? |
| Full leaderboard on every batch | ? |

---

## Task 4 — Trigger Selection

You want to process all backlog data incrementally using multiple micro-batches, then stop the query. Which trigger do you use?

```python
.trigger(___)
```

---

## Task 5 — Stream-Stream Join

You need to join an `orders_stream` with a `payments_stream` where the payment must arrive within 30 minutes of the order. Fill in the blanks:

```python
orders_wm   = orders_stream.withWatermark("order_time",   "___")
payments_wm = payments_stream.withWatermark("payment_time", "___")

joined = orders_wm.join(
    payments_wm,
    F.expr("""
        order_id = payment_order_id AND
        payment_time BETWEEN order_time AND order_time + INTERVAL ___ MINUTES
    """)
)
```

---

## Task 6 — Monitoring

After `query.awaitTermination()`, which attribute gives you metrics for the **last completed micro-batch**?

A) `query.status`  
B) `query.lastProgress`  
C) `query.metrics`  
D) `spark.streams.active`

---

## Answers

<details>
<summary>Click to reveal</summary>

**Task 1:** Tumbling = A, Sliding = B, Session = C

**Task 2:** B — `10:35` (10:50 − 15 min)

**Task 3:**
| Requirement | Mode |
|-------------|------|
| New events, no aggs | append |
| Running total, emit changes | update |
| Full leaderboard | complete |

**Task 4:** `availableNow=True`

**Task 5:** Both watermarks should be ≥ 30 min; `INTERVAL 30 MINUTES`.
Common safe choice: orders 10 min, payments 30 min.

**Task 6:** B — `query.lastProgress`

</details>
