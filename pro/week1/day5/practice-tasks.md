# Day 5 Practice Tasks — Workflows & Job Orchestration

> **Exam section:** Debugging and Deploying (10%), Infrastructure & CI/CD (20%)
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

## Task 1 — Multi-Task Job Design with Dependencies

📖 **Context**: Databricks Workflows support multi-task jobs with dependencies. The Professional exam tests task dependency patterns and task types.

🛠️ **Instructions**:

### Step 1 — Design the task DAG:

You need to create a job with this dependency structure:

```
ingest_orders → validate_orders → transform_orders
                                          ↓
                                  load_gold_table → notify_success
```

### Step 2 — Create the job via UI:

1. Go to **Workflows** > **Jobs**
2. Click **Create Job**
3. Name: `orders_etl_pipeline`
4. Add these tasks:

**Task 1: ingest_orders**
- Type: Notebook
- Notebook: Create `/pro/week1/notebooks/ingest_orders`
- Cluster: New job cluster (Standard DS3 v2)
- Depends on: None

**Task 2: validate_orders**
- Type: Notebook
- Notebook: Create `/pro/week1/notebooks/validate_orders`
- Cluster: Same as Task 1
- Depends on: `ingest_orders`

**Task 3: transform_orders**
- Type: Notebook
- Notebook: Create `/pro/week1/notebooks/transform_orders`
- Cluster: Same as Task 1
- Depends on: `validate_orders`

**Task 4: load_gold_table**
- Type: SQL
- Query: `INSERT INTO training_uc.gold.orders_summary SELECT * FROM training_uc.silver.orders_transformed`
- Warehouse: Serverless SQL Warehouse
- Depends on: `transform_orders`

**Task 5: notify_success**
- Type: Webhook (email notification)
- URL: Your email webhook
- Depends on: `load_gold_table`

### Step 3 — Understand task types:

| Task Type | Use Case | Cluster Type |
|-----------|----------|-------------|
| **Notebook** | Python/Scala/R code | Job cluster or All-Purpose |
| **SQL** | SQL queries | SQL Warehouse (recommended) |
| **DLT Pipeline** | Declarative data pipelines | Managed by DLT |
| **Python script** | Standalone .py files | Job cluster |
| **JAR** | Java/Scala JARs | Job cluster |

✅ **Expected outcome**: 
- Job creates a DAG with 5 tasks
- Tasks run in correct dependency order
- Shared job cluster reduces costs (vs separate clusters)
- SQL task uses Serverless (no cluster startup time)

⚠️ **Exam trap**: Thinking tasks run in parallel by default. Wrong! Tasks run sequentially unless they have no dependencies. `ingest_orders` and `validate_orders` run sequentially, but if both depended on nothing, they'd run in parallel.

---

## Task 2 — Repair Run After Failure

📖 **Context**: Repair Run is THE most tested Workflow feature. It re-runs only failed tasks without re-running upstream tasks.

🛠️ **Instructions**:

### Step 1 — Simulate a failure:

In the `validate_orders` notebook, add this code:

```python
# Intentionally raise an error
raise ValueError("Validation failed: missing required columns")
```

### Step 2 — Run the job:

1. Click **Run Now**
2. Observe that `ingest_orders` succeeds
3. Observe that `validate_orders` fails
4. Observe that `transform_orders`, `load_gold_table`, and `notify_success` are SKIPPED

### Step 3 — Fix the error:

Remove the `raise ValueError()` line and add proper validation:

```python
from pyspark.sql.functions import col

# Read from Bronze
df = spark.read.table("training_uc.bronze.orders")

# Validate required columns exist
required_cols = ["order_id", "customer_id", "amount", "status"]
for col_name in required_cols:
    if col_name not in df.columns:
        raise ValueError(f"Missing required column: {col_name}")

# Write validated data to Silver
df.write.mode("overwrite").saveAsTable("training_uc.silver.orders_validated")

print("✅ Validation passed!")
```

### Step 4 — Use Repair Run:

1. Go to the failed run in the job history
2. Click **Repair Run**
3. Select **Re-run failed and skipped tasks**
4. Click **Repair Run**

✅ **Expected outcome**: 
- Repair Run SKIPS `ingest_orders` (already succeeded)
- Repair Run RE-RUNS `validate_orders` (failed)
- Repair Run RUNS `transform_orders`, `load_gold_table`, `notify_success` (were skipped)
- Total run time is much shorter (no ingestion re-run)

⚠️ **Exam trap**: Thinking Repair Run always re-runs ALL tasks. Wrong! Repair Run only re-runs failed and downstream tasks. Upstream successful tasks are NOT re-run.

---

## Task 3 — Schedule with CRON Expressions

📖 **Context**: The exam tests your ability to write CRON expressions for job scheduling.

🛠️ **Instructions**:

### Step 1 — Schedule daily at 06:00 UTC:

1. In your job, click **Add Trigger**
2. Select **Scheduled**
3. Set trigger: **CRON**
4. Enter expression: `0 0 6 * * ?`
5. Timezone: **UTC**

### Step 2 — CRON expression format:

```
CRON: <second> <minute> <hour> <day-of-month> <month> <day-of-week>

Examples:
0 0 6 * * ?     → Every day at 06:00 UTC
0 30 9 * * MON  → Every Monday at 09:30 UTC
0 0 12 1 * ?    → First day of every month at 12:00 UTC
0 0 */4 * * ?   → Every 4 hours
```

### Step 3 — Add email notification:

1. In job settings, click **Notifications**
2. **On Failure**: Add `yuhao2804@gmail.com`
3. **On Success**: (optional) Add email
4. **No Alert for Skipped Runs**: Check this box

✅ **Expected outcome**: 
- Job runs automatically at 06:00 UTC every day
- Email sent only on failure (unless success notifications enabled)
- `no_alert_for_skipped_runs` prevents noise from skipped tasks

⚠️ **Exam trap**: Forgetting the `?` for unused fields in CRON. `0 0 6 * * *` is INVALID. Use `0 0 6 * * ?` for "any day of week".

---

## Task 4 — Job Cluster vs All-Purpose Cluster

📖 **Context**: The exam tests when to use Job clusters vs All-Purpose clusters. This impacts cost significantly.

🛠️ **Instructions**:

### Decision Matrix:

| Scenario | Cluster Type | Reason |
|----------|--------------|--------|
| Production ETL job running daily | Job Cluster | Created per run, cheaper, terminates automatically |
| Ad-hoc notebook development | All-Purpose | Stays running, interactive mode |
| Scheduled report generation | Job Cluster | No need for persistent cluster |
| Interactive data exploration | All-Purpose | Need persistent session |
| CI/CD pipeline testing | Job Cluster | Clean environment per test |

### Key differences:

| Feature | Job Cluster | All-Purpose Cluster |
|---------|-------------|---------------------|
| **Created** | Per job run | Manually by user |
| **Lifetime** | Terminates after job | Stays running until manually stopped |
| **Cost** | ~50% cheaper (DBU cost) | Full DBU cost |
| **Sharing** | Single job | Multiple users/notebooks |
| **Use Case** | Production jobs | Development/exploration |

### Step 1 — Configure job cluster:

```json
{
  "new_cluster": {
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
    "autotermination_minutes": 15
  }
}
```

✅ **Expected outcome**: 
- Job cluster created when job starts
- Cluster terminates 15 minutes after job completes
- Significant cost savings vs All-Purpose cluster

⚠️ **Exam trap**: Using All-Purpose clusters for production jobs. Wrong! This costs ~2x more. Always use Job clusters for scheduled jobs.

---

## Task 5 — DLT Pipeline as Workflow Task

📖 **Context**: The exam tests integrating DLT pipelines into multi-task jobs.

🛠️ **Instructions**:

### Step 1 — Add DLT task to job:

1. In your job, click **Add Task**
2. Task type: **Delta Live Tables Pipeline**
3. Pipeline: Select `orders_dlt_pipeline` (from Day 4)
4. Depends on: `ingest_orders`

### Step 2 — Pass parameters to DLT:

DLT pipelines accept parameters via the job configuration:

```json
{
  "pipeline_task": {
    "pipeline_id": "<your-pipeline-id>",
    "full_refresh": false
  },
  "base_parameters": {
    "catalog": "training_uc",
    "schema": "bronze"
  }
}
```

Access parameters in DLT notebook:

```python
import dlt

catalog = spark.conf.get("catalog", "default_catalog")
schema = spark.conf.get("schema", "default_schema")

@dlt.table()
def my_table():
    return spark.read.table(f"{catalog}.{schema}.source_table")
```

### Step 3 — Understand task types:

| Task Type | Configuration | Output |
|-----------|---------------|--------|
| **Notebook Task** | Notebook path + parameters | Cell outputs |
| **DLT Pipeline** | Pipeline ID + full_refresh | Event log |
| **SQL Task** | Query or Dashboard | Query results |
| **Python Script** | .py file path | Script stdout |

✅ **Expected outcome**: 
- DLT pipeline runs as part of multi-task job
- Parameters passed from job to pipeline
- Job waits for DLT pipeline to complete before running downstream tasks

⚠️ **Exam trap**: Thinking DLT pipelines cannot be part of multi-task jobs. Wrong! DLT is a valid task type and can have dependencies like any other task.

---

## Task 6 — Conditional Task Execution with Run If

📖 **Context**: The Professional exam tests conditional task execution using **Run If** conditions.

🛠️ **Instructions**:

### Step 1 — Create conditional task:

1. Add a new task: `send_failure_alert`
2. Type: Webhook (email)
3. Depends on: `validate_orders`
4. **Run If**: `At least one upstream task failed`

### Step 2 — Understand Run If options:

| Run If Condition | When Task Runs |
|------------------|----------------|
| **All upstream tasks succeeded** | Only if ALL dependencies succeed |
| **At least one upstream task failed** | If ANY dependency fails |
| **All upstream tasks completed** | Regardless of success/failure |

### Example Use Cases:

```
ingest_data → validate_data → transform_data
                    ↓
            (Run If: Failed)
         send_failure_alert
```

- `send_failure_alert` runs ONLY if `validate_data` fails
- `transform_data` runs ONLY if `validate_data` succeeds

✅ **Expected outcome**: 
- Failure alert task runs only when validation fails
- This enables error handling without changing notebook code

⚠️ **Exam trap**: Thinking Run If applies to ALL upstream tasks. Wrong! Run If evaluates DIRECT dependencies only (tasks in `depends_on`).

---

## Task 7 — Retry on Failure Configuration

📖 **Context**: The exam tests when to enable retry and what the risks are.

🛠️ **Instructions**:

### Step 1 — Configure retry:

1. In task settings, expand **Advanced options**
2. **Max Retries**: 3
3. **Retry on Timeout**: Enabled

### Step 2 — When to use retry:

| Scenario | Retry? | Reason |
|----------|--------|--------|
| API call to external service | YES | Transient network errors |
| Writing to Delta table | YES | Concurrent write conflicts |
| Sending duplicate transactions | NO | Risk of duplicates |
| Idempotent ETL | YES | Safe to re-run |
| Non-idempotent operations (e.g., POST to external API) | NO | May create duplicates |

### Step 3 — Best practices:

```python
# ✅ GOOD: Idempotent write (safe to retry)
df.write.mode("overwrite").saveAsTable("target_table")

# ✅ GOOD: Merge is idempotent
target.merge(source, "id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# ❌ BAD: Append without deduplication (creates duplicates on retry)
df.write.mode("append").saveAsTable("target_table")

# ❌ BAD: Sending email on every retry
send_email("Job completed")  # May send multiple emails
```

✅ **Expected outcome**: 
- Failed task retries up to 3 times
- Idempotent operations are safe to retry
- Non-idempotent operations (like sending emails) should not use retry

⚠️ **Exam trap**: Enabling retry for ALL tasks. Wrong! Only enable retry for idempotent operations. Non-idempotent tasks (like sending notifications) should NOT retry.

---

## Task 8 — Concept Quiz

Answer these rapid-fire questions:

1. What is the difference between a Job cluster and an All-Purpose cluster?
2. What does **Repair Run** do?
3. What CRON expression runs every Monday at 09:00 UTC?
4. What task types can a Databricks Workflow contain?
5. How do you pass parameters to a DLT pipeline task?
6. What does **Run If: At least one upstream task failed** mean?
7. When should you enable **Retry on Failure**?
8. Do tasks run in parallel by default?
9. What is the cost difference between Job cluster and All-Purpose cluster?
10. What happens to a Job cluster after the job completes?

---

## Key Takeaways for the Exam

✅ **Job Design:**
- **Task Types**: Notebook, SQL, DLT Pipeline, Python script, JAR
- **Dependencies**: Use `depends_on` to create task DAGs
- **Parallelism**: Tasks with no dependencies run in parallel
- **Repair Run**: Re-runs failed and downstream tasks, SKIPS successful upstream tasks

✅ **Clusters:**
- **Job Cluster**: Created per run, ~50% cheaper, auto-terminates
- **All-Purpose Cluster**: Persistent, more expensive, for development
- **Always use Job clusters for production jobs**

✅ **Scheduling:**
- **CRON format**: `<second> <minute> <hour> <day> <month> <day-of-week>`
- **Examples**: `0 0 6 * * ?` (daily at 06:00), `0 30 9 * * MON` (Monday 09:30)
- **Notifications**: Email on failure, success, or both

✅ **Advanced Features:**
- **Run If**: Conditional task execution based on upstream status
- **Retry**: Enable only for idempotent operations
- **Parameters**: Pass via `base_parameters` in job config
- **DLT Integration**: DLT pipelines can be tasks in multi-task jobs

✅ **Best Practices:**
- Use shared job clusters to reduce costs
- Enable retry only for idempotent tasks
- Use `no_alert_for_skipped_runs` to reduce notification noise
- Use Repair Run instead of full re-runs

---

## Next Steps

You've completed Day 5! You now understand Databricks Workflows at a professional level. You've completed Week 1 of the Professional exam prep! 🎉

Next week (Days 6-7), you'll cover advanced topics and practice for the exam.
