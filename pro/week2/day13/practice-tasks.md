# Day 13 Practice Tasks — Security, Monitoring & Databricks CLI

> **Exam section:** Ensuring Data Security and Compliance (10%), Monitoring and Alerting (10%), Debugging and Deploying (10%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## Task 1 — Secrets Management with Databricks CLI

📖 **Context:**
Secrets management is critical for security. Professional exam tests CLI commands and best practices.

🛠️ **Instructions:**

**Step 1 — Install Databricks CLI:**

```bash
# Install Databricks CLI v2 (modern)
pip install databricks-cli

# Configure authentication
databricks configure --token
# Enter workspace URL: https://adb-xxx.azuredatabricks.net
# Enter PAT token: dapi...
```

**Step 2 — Create a Databricks-backed secret scope:**

```bash
# Create scope
databricks secrets create-scope my-secrets

# Put secrets
databricks secrets put-secret my-secrets db-password --string-value "MyS3cr3tP@ss"
databricks secrets put-secret my-secrets api-key --string-value "key_1234567890"

# List secrets in scope
databricks secrets list-secrets my-secrets

# List all scopes
databricks secrets list-scopes
```

**Step 3 — Use secrets in notebook:**

```python
# Read secret (output is automatically redacted)
password = dbutils.secrets.get(scope="my-secrets", key="db-password")
print(password)  # prints [REDACTED]

# Use in JDBC connection
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", dbutils.secrets.get("my-secrets", "db-password")) \
    .load()
```

✅ **Expected outcome:**
- Secrets stored securely
- Output is redacted when printed
- Can be used in connections without exposing

⚠️ **Exam trap:**
Secrets are ALWAYS redacted in notebook output. You cannot view them after creation. Databricks-backed vs Azure Key Vault-backed scopes have different use cases.

---

## Task 2 — Service Principals for CI/CD

📖 **Context:**
Service principals are the secure way to run jobs in production.

🛠️ **Instructions:**

**Step 1 — Create service principal (Azure Portal):**

1. Azure Portal → Microsoft Entra ID → App registrations → New registration
2. Name: `databricks-cicd-sp`
3. Copy: Application (client) ID, Tenant ID
4. Certificates & secrets → New client secret → Copy value

**Step 2 — Add SP to Databricks:**

```bash
# Add service principal to workspace
databricks service-principals create \
    --application-id <client-id> \
    --display-name "CI/CD Service Principal"
```

**Step 3 — Assign SP to job:**

In job configuration JSON:

```json
{
  "name": "ETL Job",
  "run_as": {
    "service_principal_name": "<client-id>@<tenant-id>"
  },
  "tasks": [...]
}
```

✅ **Expected outcome:**
- Job runs as service principal (not personal user)
- Logs show SP identity
- Best practice for production

⚠️ **Exam trap:**
NEVER use personal access tokens in production CI/CD. Always use service principals. Personal tokens expire and create security risks.

---

## Task 3 — Job Monitoring and Alerting

📖 **Context:**
Monitoring job health is essential for production data pipelines.

🛠️ **Instructions:**

**Step 1 — Configure job notifications:**

In job settings, add:

```json
"email_notifications": {
  "on_failure": ["alert@company.com"],
  "on_success": ["team@company.com"],
  "no_alert_for_skipped_runs": true
}
```

**Step 2 — Query job run history via Python SDK:**

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host="https://adb-xxx.azuredatabricks.net",
    token=dbutils.secrets.get("my-secrets", "databricks-token")
)

# List recent runs for a job
job_id = 123456
runs = w.jobs.list_runs(job_id=job_id, limit=10)

for run in runs:
    print(f"Run {run.run_id}: {run.state.result_state} at {run.start_time}")
    if run.state.result_state == "FAILED":
        print(f"Error: {run.state.state_message}")
```

**Step 3 — Monitor job run duration:**

```python
from datetime import datetime

for run in runs:
    if run.start_time and run.end_time:
        duration = (run.end_time - run.start_time) / 1000  # milliseconds to seconds
        print(f"Run {run.run_id}: {duration:.1f}s")
        
        if duration > 3600:  # Alert if > 1 hour
            print(f"⚠️ Long-running job detected: {duration/3600:.1f}h")
```

✅ **Expected outcome:**
- Email alerts sent on job failure
- Can programmatically monitor job health
- Identify performance degradation

⚠️ **Exam trap:**
`on_failure` alerts are most critical. Set `no_alert_for_skipped_runs=true` to avoid noise. Job run history is available via API for custom monitoring.

---

## Task 4 — Databricks CLI Job Management

📖 **Context:**
CLI is essential for automation and CI/CD.

🛠️ **Instructions:**

**List all jobs:**

```bash
databricks jobs list
```

**Get job details:**

```bash
databricks jobs get --job-id 123456
```

**Trigger job run:**

```bash
databricks jobs run-now --job-id 123456

# With parameters
databricks jobs run-now --job-id 123456 \
    --notebook-params '{"env": "prod", "date": "2026-07-13"}'
```

**Check run status:**

```bash
# Get run details
databricks runs get --run-id 789

# Get run output
databricks runs get-output --run-id 789

# Cancel a running job
databricks runs cancel --run-id 789
```

**Export/Import job definitions:**

```bash
# Export job definition
databricks jobs get --job-id 123456 > job_definition.json

# Create new job from definition
databricks jobs create --json-file job_definition.json
```

✅ **Expected outcome:**
- Can manage jobs entirely via CLI
- Suitable for CI/CD pipelines
- No need for UI for automation

⚠️ **Exam trap:**
`jobs run-now` triggers immediate run. `jobs reset` updates job definition. `runs get` shows run status. Know the difference.

---

## Task 5 — Databricks SDK for Python - Advanced

📖 **Context:**
SDK v2 is the modern way to interact with Databricks programmatically.

🛠️ **Instructions:**

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNowRequest, RunResultState
import time

w = WorkspaceClient(
    host="https://adb-xxx.azuredatabricks.net",
    token=dbutils.secrets.get("my-secrets", "databricks-token")
)

# Trigger job and wait for completion
job_id = 123456
run = w.jobs.run_now(job_id=job_id, notebook_params={"env": "staging"})
print(f"Started run: {run.run_id}")

# Poll for completion
while True:
    run_status = w.jobs.get_run(run.run_id)
    state = run_status.state.life_cycle_state
    
    print(f"Run state: {state}")
    
    if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
        break
    
    time.sleep(10)

# Check result
if run_status.state.result_state == RunResultState.SUCCESS:
    print("✅ Job succeeded")
else:
    print(f"❌ Job failed: {run_status.state.state_message}")

# List all clusters
for cluster in w.clusters.list():
    print(f"{cluster.cluster_id}: {cluster.cluster_name} - {cluster.state}")

# Start a cluster
w.clusters.start(cluster_id="abc-123-def456")

# Get pipeline info
pipeline = w.pipelines.get(pipeline_id="pipe-123")
print(f"Pipeline: {pipeline.name}, State: {pipeline.state}")
```

✅ **Expected outcome:**
- Full programmatic control of Databricks
- Can build custom orchestration/monitoring
- Type-safe Python SDK

⚠️ **Exam trap:**
SDK v2 (`databricks.sdk`) is different from legacy SDK (`databricks-connect`). Use v2 for all new code.

---

## Task 6 — Cluster Policies for Cost Control

📖 **Context:**
Cluster policies prevent cost overruns and enforce standards.

🛠️ **Instructions:**

**Step 1 — Create cluster policy (Admin Console):**

Go to **Admin Console** → **Compute** → **Policies** → **Create**

Example policy JSON:

```json
{
  "spark_version": {
    "type": "fixed",
    "value": "15.4.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["Standard_DS3_v2", "Standard_DS4_v2"]
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 60
  },
  "autoscale": {
    "type": "fixed",
    "value": {
      "min_workers": 1,
      "max_workers": 5
    }
  },
  "custom_tags.cost_center": {
    "type": "fixed",
    "value": "data-engineering"
  }
}
```

**Key policy types:**
- `fixed`: enforced value, user cannot change
- `range`: min/max boundaries
- `allowlist`: allowed values
- `forbidden`: disallowed values

✅ **Expected outcome:**
- Users can only create clusters within policy
- Prevents expensive configurations
- Enforces standards (tags, auto-termination)

⚠️ **Exam trap:**
`fixed` = no user control. `range` = user chooses within bounds. Policies apply at cluster creation, not retroactively.

---

## Task 7 — IP Access Lists

📖 **Context:**
IP whitelisting restricts workspace access.

🛠️ **Instructions:**

**Admin Console** → **Security** → **IP Access Lists** → **Add**

Example:
- Label: `Corporate VPN`
- IP addresses: `203.0.113.0/24, 198.51.100.0/24`
- Enabled: Yes

Once enabled:
- Only listed IPs can access workspace
- Applies to UI, API, CLI
- Service principals not affected

✅ **Expected outcome:**
- Workspace locked to specific IPs
- Enhanced security for sensitive environments

⚠️ **Exam trap:**
IP access lists can LOCK YOU OUT if misconfigured. Always test before enabling globally. Service principals bypass IP restrictions.

---

## Task 8 — Single User vs Shared Cluster Access Mode

📖 **Context:**
Access modes determine security and isolation.

🛠️ **Instructions:**

| Access Mode | Use Case | Isolation | Unity Catalog |
|-------------|----------|-----------|---------------|
| **Single User** | One user only, full cluster access | High | Required for Python UDFs, row/column filters |
| **Shared** | Multiple users, controlled access | Medium | Enforced, no root access |
| **No Isolation Shared** | Legacy, deprecated | Low | Not supported |

**When to use:**
- **Single User**: ML notebooks, admin tasks, Unity Catalog with Python UDFs
- **Shared**: SQL analytics, BI dashboards, multi-user environments

✅ **Expected outcome:**
Understand security trade-offs and when each mode is appropriate.

⚠️ **Exam trap:**
Unity Catalog with Python UDFs REQUIRES Single User mode. Shared mode blocks Python UDFs for security.

---

## Concept Quiz (Self-Check)

1. Where are secrets stored when using `databricks secrets put-secret`?
2. What is the difference between Databricks-backed and Azure Key Vault-backed secret scopes?
3. Why use service principals instead of personal tokens in CI/CD?
4. What CLI command triggers an immediate job run?
5. What SDK v2 class is the main entry point?
6. What cluster policy type prevents users from changing a setting?
7. Do IP access lists apply to service principals?
8. What access mode is required for Unity Catalog with Python UDFs?
9. How do you pass parameters to a job via CLI?
10. What happens to secrets when you `print()` them?

**Answers:**
1. Databricks control plane (encrypted at rest)
2. Databricks-backed = managed by Databricks; Azure KV-backed = references Azure Key Vault
3. Service principals don't expire, support automation, follow least-privilege principle
4. `databricks jobs run-now --job-id <id>`
5. `WorkspaceClient`
6. `fixed` - user has no control
7. No - service principals bypass IP restrictions
8. **Single User** mode
9. `--notebook-params '{"key": "value"}'` or `--python-params` or `--jar-params`
10. Output is automatically `[REDACTED]`

---

## Key Takeaways for Exam

✅ **Secrets are ALWAYS redacted in output - use `dbutils.secrets.get()`**
✅ **Service principals for production, NOT personal tokens**
✅ **`jobs run-now` = trigger; `runs get` = status; `runs get-output` = results**
✅ **SDK v2 = `databricks.sdk` with `WorkspaceClient`**
✅ **Cluster policies: `fixed` = no choice; `range` = bounded; `allowlist` = approved values**
✅ **IP access lists lock workspace, service principals bypass**
✅ **Single User mode required for Unity Catalog Python UDFs**
✅ **Auto-termination in cluster policies prevents runaway costs**
