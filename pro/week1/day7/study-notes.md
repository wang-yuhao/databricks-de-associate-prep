# Day 7 — Databricks Asset Bundles (DABs) & CI/CD (~10% exam)

## Schedule
- **Morning (2h):** Read all sections below
- **Mid-morning (2h):** Work through `notebooks/pro_day7_dabs_cicd.py`
- **Afternoon (2h):** Complete `practice-tasks.md`
- **Evening (1h):** Review deployment and CLI questions

---

## 7.1 Databricks CLI

### Installation & Authentication
```bash
# Install CLI v2
pip install databricks-cli
# OR (recommended for v2)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Configure with Azure Databricks
databricks configure --host https://adb-<workspace-id>.<random>.azuredatabricks.net
# Enter personal access token (PAT) or use Azure service principal

# OR use environment variables
export DATABRICKS_HOST="https://adb-<id>.<random>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

# Verify
databricks workspace list
databricks clusters list
```

### Key CLI Commands
```bash
# Workspace
databricks workspace list /Users/yuhao2804@gmail.com
databricks workspace export /path/to/notebook.py ./local.py
databricks workspace import ./local.py /path/to/notebook.py

# Jobs
databricks jobs list
databricks jobs run-now --job-id 12345
databricks runs get --run-id 67890

# Clusters
databricks clusters list
databricks clusters start --cluster-id abc-123
databricks clusters delete --cluster-id abc-123

# Secrets
databricks secrets create-scope --scope azure-kv
databricks secrets put --scope azure-kv --key my-secret
databricks secrets list --scope azure-kv

# DBFS / Volumes
databricks fs ls dbfs:/FileStore/
databricks fs cp ./local_file.csv dbfs:/FileStore/uploads/
```

---

## 7.2 Databricks Asset Bundles (DABs)

DABs is the standard for **deploying Databricks resources as code**.

### Bundle Structure
```yaml
# databricks.yml
bundle:
  name: orders_etl_bundle

variables:
  catalog:
    default: training
  schema:
    default: prep
  env:
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-<workspace-id>.<random>.azuredatabricks.net
    variables:
      catalog: training
      schema: prep_dev

  staging:
    mode: development
    workspace:
      host: https://adb-<staging-id>.<random>.azuredatabricks.net
    variables:
      catalog: training
      schema: prep_staging

  prod:
    mode: production
    workspace:
      host: https://adb-<prod-id>.<random>.azuredatabricks.net
    variables:
      catalog: training
      schema: prep_prod

resources:
  jobs:
    orders_etl_job:
      name: orders_etl_${bundle.target}
      email_notifications:
        on_failure:
          - yuhao2804@gmail.com
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./notebooks/01_ingest.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          job_cluster_key: etl_cluster

        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./notebooks/02_transform.py
          job_cluster_key: etl_cluster

  job_clusters:
    etl_cluster:
      new_cluster:
        spark_version: 15.4.x-scala2.12
        node_type_id: Standard_DS3_v2
        num_workers: 2
        spark_conf:
          spark.databricks.delta.preview.enabled: true

  pipelines:
    orders_pipeline:
      name: orders_dlt_${bundle.target}
      target: ${var.schema}
      libraries:
        - notebook:
            path: ./notebooks/pipeline.py
      continuous: false
      development: ${bundle.target == 'dev'}
```

### DABs CLI Commands
```bash
# Validate bundle config
databricks bundle validate

# Deploy to target (default = dev)
databricks bundle deploy
databricks bundle deploy --target staging
databricks bundle deploy --target prod

# Run a job defined in bundle
databricks bundle run orders_etl_job
databricks bundle run orders_etl_job --target prod

# Destroy resources
databricks bundle destroy

# Show deployment status
databricks bundle summary
```

---

## 7.3 CI/CD with GitHub Actions

```yaml
# .github/workflows/deploy.yml
name: Deploy Databricks Bundle

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install -r requirements-dev.txt
      - name: Run unit tests
        run: pytest tests/ -v

  deploy-staging:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main
      - name: Deploy to Staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_DATABRICKS_TOKEN }}
        run: |
          databricks bundle deploy --target staging
          databricks bundle run orders_etl_job --target staging

  deploy-prod:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production   # requires manual approval
    steps:
      - uses: actions/checkout@v3
      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main
      - name: Deploy to Production
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_DATABRICKS_TOKEN }}
        run: databricks bundle deploy --target prod
```

---

## 7.4 REST API

```python
import requests

HOST = "https://adb-<workspace-id>.<random>.azuredatabricks.net"
TOKEN = dbutils.secrets.get(scope="azure-kv", key="pat-token")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# List jobs
response = requests.get(f"{HOST}/api/2.1/jobs/list", headers=HEADERS)
jobs = response.json()["jobs"]

# Run a job now
response = requests.post(
    f"{HOST}/api/2.1/jobs/run-now",
    headers=HEADERS,
    json={"job_id": 12345}
)
run_id = response.json()["run_id"]

# Get run status
response = requests.get(
    f"{HOST}/api/2.1/jobs/runs/get",
    headers=HEADERS,
    params={"run_id": run_id}
)
status = response.json()["state"]["life_cycle_state"]
print(status)  # PENDING, RUNNING, TERMINATED, etc.
```

---

## 7.5 Secrets Management

```bash
# Create scope backed by Azure Key Vault
databricks secrets create-scope \
  --scope azure-kv \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/.../resourceGroups/.../providers/Microsoft.KeyVault/vaults/my-kv \
  --dns-name https://my-kv.vault.azure.net/

# Create Databricks-backed scope (simpler but less secure)
databricks secrets create-scope --scope my-scope
databricks secrets put --scope my-scope --key my-password
```

```python
# Use secrets in notebooks
password = dbutils.secrets.get(scope="azure-kv", key="db-password")
# Databricks redacts the value in output
print(password)  # [REDACTED]

# Use in JDBC connection
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://server.database.windows.net") \
    .option("password", dbutils.secrets.get("azure-kv", "db-password")) \
    .load()
```

---

## Key Exam Points ✔️

- **DABs** (`databricks.yml`) is the standard IaC approach for Databricks deployments
- `databricks bundle deploy --target prod` deploys to a specific target
- DABs supports variables (`${var.catalog}`) and target-specific overrides
- **GitHub Actions** + DABs = standard CI/CD pattern for Databricks
- REST API endpoint: `/api/2.1/jobs/run-now` to trigger a job
- Secrets scope backed by Azure Key Vault is more secure than Databricks-backed
- `dbutils.secrets.get(scope, key)` retrieves secrets; values are always **redacted** in output
- `databricks bundle validate` checks bundle YAML without deploying
- `mode: production` in DABs enables retries, disables verbose logging
- `mode: development` marks jobs/pipelines with `[dev username]` prefix to avoid conflicts
