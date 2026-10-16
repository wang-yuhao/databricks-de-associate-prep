# Day 13: Security, Monitoring & Databricks CLI

## Schedule
- Morning: Cluster security, network isolation, secrets
- Afternoon: Job monitoring, alerting, SLAs
- Evening: Databricks CLI deep dive and practice

---

## 13.1 Secrets Management

```bash
# Create secret scope (Databricks-backed)
databricks secrets create-scope my-scope

# Create secret scope (Azure Key Vault-backed)
databricks secrets create-scope \
  --scope my-akv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault> \
  --dns-name https://<vault>.vault.azure.net/

# Put a secret
databricks secrets put-secret my-scope db-password --string-value "my_password"

# List secrets
databricks secrets list-secrets my-scope

# Delete secret
databricks secrets delete-secret my-scope db-password
```

```python
# Use secrets in notebooks/jobs
password = dbutils.secrets.get(scope="my-scope", key="db-password")

# Secret is redacted in output
print(password)  # prints [REDACTED]

# Use in JDBC connection
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", dbutils.secrets.get("my-scope", "db-password")) \
    .load()
```

---

## 13.2 Cluster Security

```python
# Cluster policies enforce security settings
# Example policy JSON:
# {
#   "spark_version": {"type": "fixed", "value": "15.4.x-scala2.12"},
#   "node_type_id": {"type": "allowlist", "values": ["Standard_DS3_v2"]},
#   "autotermination_minutes": {"type": "range", "minValue": 10, "maxValue": 60},
#   "custom_tags.team": {"type": "fixed", "value": "data-engineering"}
# }

# IP Access Lists
# Whitelist specific IPs for workspace access
# Configured in Admin Console > Security > IP Access Lists

# Single User vs Shared clusters
# Single User: full isolation, can mount DBFS
# Shared: multi-tenant, no root access, Unity Catalog enforced
```

---

## 13.3 Service Principals

```bash
# Create service principal (AAD/Entra)
# Done in Azure portal, then add to Databricks

# Add SP to workspace
databricks service-principals create \
  --application-id <app-id> \
  --display-name "etl-service-principal"

# Generate SP token
databricks tokens create --comment "SP token for CI/CD" --lifetime-seconds 7776000

# Set SP as job owner (CLI)
databricks jobs update <job-id> \
  --json '{"run_as": {"service_principal_name": "etl-service-principal"}}'
```

---

## 13.4 Job Monitoring & Alerting

```python
# Query job run history via API / SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# List recent job runs
runs = w.jobs.list_runs(job_id=123456, limit=10)
for run in runs:
    print(f"Run {run.run_id}: {run.state.result_state} at {run.start_time}")

# Get run output
run_output = w.jobs.get_run_output(run_id=789)
print(run_output.error)

# Notification settings in job config (YAML/JSON)
# email_notifications:
#   on_failure: ["alert@company.com"]
#   on_success: []
#   no_alert_for_skipped_runs: true
# webhook_notifications:
#   on_failure:
#     - id: "slack-webhook-id"
```

---

## 13.5 Databricks CLI Deep Dive

```bash
# Install CLI v2
pip install databricks-cli  # legacy
brew install databricks/tap/databricks  # v2 (recommended)

# Configure authentication
databricks configure --token
# or via environment variables:
export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...

# Cluster operations
databricks clusters list
databricks clusters start --cluster-id abc123
databricks clusters delete --cluster-id abc123
databricks clusters get --cluster-id abc123

# Job operations
databricks jobs list
databricks jobs get --job-id 123
databricks jobs run-now --job-id 123
databricks runs get --run-id 456
databricks runs cancel --run-id 456

# DBFS operations
databricks fs ls dbfs:/data/
databricks fs cp local_file.csv dbfs:/data/local_file.csv
databricks fs rm dbfs:/data/old_file.csv

# Workspace operations
databricks workspace ls /Users/user@company.com
databricks workspace export /notebooks/etl.py ./etl.py
databricks workspace import ./etl.py /notebooks/etl.py --language PYTHON

# Pipeline operations
databricks pipelines list
databricks pipelines start --pipeline-id pipe123
databricks pipelines get --pipeline-id pipe123
```

---

## 13.6 Databricks SDK for Python

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNowRequest

w = WorkspaceClient(
    host="https://adb-xxx.azuredatabricks.net",
    token="dapi..."
)

# List clusters
for cluster in w.clusters.list():
    print(f"{cluster.cluster_id}: {cluster.cluster_name} - {cluster.state}")

# Trigger job
run = w.jobs.run_now(job_id=123, notebook_params={"env": "prod"})
print(f"Started run: {run.run_id}")

# Wait for completion
from databricks.sdk.service.jobs import RunResultState
result = w.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)
if result.state.result_state == RunResultState.SUCCESS:
    print("Job succeeded!")
```

---

## Key Exam Points

- Secrets: use `dbutils.secrets.get(scope, key)` - never printed in output
- Secret scopes: Databricks-backed or Azure Key Vault-backed
- Service principals: use for CI/CD, never personal tokens in prod
- Cluster policies: enforce resource limits, tags, and configurations
- Single User clusters: isolation mode, required for Unity Catalog with ML
- Shared clusters: enforced Unity Catalog, no DBFS root, no init scripts
- `databricks bundle` = DABs deployment | `databricks jobs run-now` = trigger
- SDK v2 (`databricks.sdk`): modern Python SDK with type safety

---

## Practice Tasks
- [ ] Create a secret scope and store a database password
- [ ] Use dbutils.secrets.get() in a notebook to connect to JDBC
- [ ] Configure job email notifications on failure
- [ ] Use Databricks CLI to list clusters and trigger a job run
- [ ] Use Python SDK to poll job run status until completion
