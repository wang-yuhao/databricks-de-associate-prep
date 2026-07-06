# Day 5 Practice Tasks — Workflows & Job Orchestration

## Task 1 — Multi-Task Job Design
Design a Databricks Job with the following task DAG:
```
ingest_orders → validate_orders → transform_orders
                                        ↓
                              load_gold_table → notify_success
```
For each task, specify:
- Task type (notebook / DLT pipeline / SQL / Python script)
- Cluster type (job cluster vs existing cluster)
- Any `depends_on` relationships

> Write this as a JSON job definition or describe in detail.

---

## Task 2 — Repair Run
1. Simulate a job failure by intentionally raising an error in the `validate_orders` notebook.
2. Fix the error.
3. Use **Repair Run** to re-run only the failed task without re-running `ingest_orders`.

> Describe what happened and what Repair Run saved you.

---

## Task 3 — Schedule & Alerts
1. Schedule the job to run every day at 06:00 UTC using a CRON expression.
2. Add email notification on failure to `yuhao2804@gmail.com`.
3. What is the CRON expression for: _every Monday at 09:00 UTC_?

```
# CRON for daily 06:00 UTC:

# CRON for Monday 09:00 UTC:
```

---

## Task 4 — DLT Pipeline as Workflow Task
1. Add the `orders_pipeline` DLT pipeline (from Day 4) as a task in your job.
2. How do you pass a parameter from the job to a DLT pipeline task?
3. What task types can a Databricks Workflow task be?

---

## Task 5 — Concept Short-Answer
1. What is the difference between a **Job cluster** and an **All-Purpose cluster** in the context of jobs?
2. What does the **Run If** dependency condition allow you to do?
3. How does **Retry on failure** work at the task level, and when should you be careful using it?
