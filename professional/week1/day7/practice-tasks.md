# Day 7 Practice Tasks — Databricks Asset Bundles (DABs) & CI/CD

> **Exam section:** Infrastructure & CI/CD (20%), Debugging and Deploying (10%)
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

## Task 1 — Create Databricks Asset Bundle (DAB)

📖 **Context**: DABs are THE recommended way to deploy Databricks resources in production. The exam tests `databricks.yml` structure and target configuration.

🛠️ **Instructions**:

### Step 1 — Initialize bundle structure:

```bash
# Create directory structure
mkdir -p databricks-bundles/prep_etl_bundle
cd databricks-bundles/prep_etl_bundle

# Initialize with Databricks CLI
databricks bundle init
```

### Step 2 — Create `databricks.yml`:

```yaml
bundle:
  name: prep_etl_bundle

include:
  - resources/*.yml

variables:
  catalog:
    description: "Unity Catalog name"
    default: training_uc
  schema:
    description: "Schema name"
    default: bronze

targets:
  # Development target (default)
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-123456789.azuredatabricks.net
    variables:
      catalog: training_uc_dev
      schema: bronze_dev
    
  # Production target
  prod:
    mode: production
    workspace:
      host: https://adb-987654321.azuredatabricks.net
      root_path: /Workspace/production/prep_etl
    variables:
      catalog: training_uc
      schema: bronze
    run_as:
      service_principal_name: "sp-prod-etl@company.com"
```

### Step 3 — Create job resource (`resources/jobs.yml`):

```yaml
resources:
  jobs:
    daily_etl_job:
      name: "${bundle.target}-daily-etl"
      
      tasks:
        - task_key: ingest_data
          notebook_task:
            notebook_path: ../notebooks/ingest.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          new_cluster:
            spark_version: 13.3.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
            autotermination_minutes: 15
        
        - task_key: transform_data
          notebook_task:
            notebook_path: ../notebooks/transform.py
          depends_on:
            - task_key: ingest_data
          new_cluster:
            spark_version: 13.3.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
      
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
      
      email_notifications:
        on_failure:
          - yuhao2804@gmail.com
```

### Step 4 — Understand mode differences:

| Feature | Development Mode | Production Mode |
|---------|------------------|------------------|
| **Resource naming** | Prefixed with username | No prefix |
| **Root path** | `/Users/<username>/.bundle/...` | User-specified or default |
| **Error handling** | Lenient (allows some errors) | Strict (fails on errors) |
| **Run as** | Current user | Service principal (recommended) |
| **Use case** | Testing, iteration | Live production workloads |

✅ **Expected outcome**: 
- Bundle structure with `databricks.yml` and `resources/`
- Two targets: `dev` (default) and `prod`
- Job resource with variables
- Mode differences understood

⚠️ **Exam trap**: Thinking `mode: development` only adds username prefix. Wrong! It also changes error handling, path defaults, and resource lifecycle. Production mode is STRICT — errors that are warnings in dev become failures in prod.

---

## Task 2 — Bundle CLI Commands

📖 **Context**: The exam tests your knowledge of DAB CLI commands for validation, deployment, and management.

🛠️ **Instructions**:

### Essential DAB commands:

```bash
# 1. Validate bundle (check syntax, no deployment)
databricks bundle validate

# 2. Deploy to default target (dev)
databricks bundle deploy

# 3. Deploy to specific target
databricks bundle deploy --target prod

# 4. Show what would be deployed (dry run)
databricks bundle deploy --target prod --dry-run

# 5. Run a job from the bundle
databricks bundle run daily_etl_job --target dev

# 6. Show deployed resources
databricks bundle summary

# 7. Destroy all resources
databricks bundle destroy --target dev

# 8. Generate bundle from existing resources
databricks bundle generate job --job-id 12345
```

### Command workflow:

```
1. databricks bundle validate     →  Check syntax
2. databricks bundle deploy       →  Deploy to workspace
3. databricks bundle run          →  Run job/pipeline
4. databricks bundle destroy      →  Clean up resources
```

### Key flags:

| Flag | Purpose | Example |
|------|---------|----------|
| `--target` | Specify target environment | `--target prod` |
| `--var` | Override variable | `--var="catalog=test_uc"` |
| `--force` | Skip confirmation prompts | `--force` |
| `--dry-run` | Show changes without deploying | `--dry-run` |

✅ **Expected outcome**: 
- Know the purpose of each command
- Understand validate → deploy → run → destroy workflow
- Can deploy to different targets
- Understand dry-run for safety

⚠️ **Exam trap**: Thinking `databricks bundle validate` deploys resources. Wrong! `validate` ONLY checks syntax. You must run `deploy` to actually create resources in the workspace.

---

## Task 3 — GitHub Actions CI/CD Pipeline

📖 **Context**: The exam tests automated deployment via GitHub Actions. You must know how to set up secrets and deploy bundles.

🛠️ **Instructions**:

### Step 1 — Create workflow (`.github/workflows/deploy.yml`):

```yaml
name: Deploy Databricks Bundle

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install Databricks CLI
        run: |
          pip install databricks-cli
      
      - name: Run unit tests
        run: |
          pip install pytest
          pytest tests/
      
      - name: Validate bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks bundle validate
      
      - name: Deploy to staging
        if: github.event_name == 'pull_request'
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks bundle deploy --target staging
      
      - name: Deploy to production
        if: github.event_name == 'push' && github.ref == 'refs/heads/main'
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
        run: |
          databricks bundle deploy --target prod
```

### Step 2 — Set up GitHub Secrets:

1. Go to GitHub repo → **Settings** → **Secrets and variables** → **Actions**
2. Add secrets:
   - `DATABRICKS_HOST`: `https://adb-123456789.azuredatabricks.net`
   - `DATABRICKS_TOKEN`: Service Principal token (NOT personal token)
   - `DATABRICKS_PROD_HOST`: Production workspace URL
   - `DATABRICKS_PROD_TOKEN`: Production SP token

### Step 3 — Best practices:

✅ **DO:**
- Use Service Principal tokens (never personal tokens in CI/CD)
- Store tokens in GitHub Secrets (never in code)
- Run tests BEFORE deployment
- Use different secrets for staging vs production
- Deploy to staging on PR, production on merge

❌ **DON'T:**
- Hardcode tokens in workflow files
- Use personal access tokens (they expire)
- Deploy to production without testing
- Skip validation step

✅ **Expected outcome**: 
- Automated deployment on push to main
- Tests run before deployment
- Secrets properly configured
- Staging and production environments separated

⚠️ **Exam trap**: Using personal access tokens in CI/CD. Wrong! Personal tokens EXPIRE and are tied to a user. Always use Service Principal credentials in production CI/CD pipelines.

---

## Task 4 — Databricks Secrets Management

📖 **Context**: The exam tests your knowledge of secret scopes and `dbutils.secrets` API.

🛠️ **Instructions**:

### Step 1 — Create secret scope:

```bash
# Databricks-backed scope (simple, managed by Databricks)
databricks secrets create-scope --scope my_secrets

# Azure Key Vault-backed scope (enterprise, managed by Azure)
databricks secrets create-scope --scope kv_secrets \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/.../vaults/my-keyvault \
  --dns-name https://my-keyvault.vault.azure.net/
```

### Step 2 — Add secrets:

```bash
# Add secret to Databricks-backed scope
databricks secrets put --scope my_secrets --key db_password
# (opens editor to enter value)

# Add secret from file
echo "my-secret-value" | databricks secrets put-secret \
  --scope my_secrets --key api_key

# List scopes
databricks secrets list-scopes

# List secrets in scope
databricks secrets list --scope my_secrets
```

### Step 3 — Use secrets in notebooks:

```python
# Get secret (always returns [REDACTED] when printed)
db_password = dbutils.secrets.get(scope="my_secrets", key="db_password")

# Use in JDBC connection
jdbc_url = "jdbc:postgresql://db.example.com:5432/mydb"
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "orders") \
    .option("user", "dbuser") \
    .option("password", dbutils.secrets.get("my_secrets", "db_password")) \
    .load()

# Secrets are NEVER logged
print(f"Password: {db_password}")  # Output: [REDACTED]
```

### Step 4 — Secret scope types:

| Type | Managed By | Use Case | Cost |
|------|------------|----------|------|
| **Databricks-backed** | Databricks control plane | Simple secrets, dev/test | Free |
| **Azure Key Vault-backed** | Azure Key Vault | Enterprise, compliance, production | Azure KV pricing |

**Key differences:**
- **Databricks-backed**: Stored in Databricks, encrypted at rest, simple to use
- **Azure Key Vault-backed**: Stored in Azure KV, centralized secret management, audit logs, compliance

✅ **Expected outcome**: 
- Can create secret scopes
- Can add and retrieve secrets
- Understand that `dbutils.secrets.get()` ALWAYS returns `[REDACTED]` when printed
- Know when to use each scope type

⚠️ **Exam trap**: Thinking you can view secret values after creation. Wrong! Once stored, secrets can NEVER be viewed — they can only be retrieved programmatically via `dbutils.secrets.get()` and used in code.

---

## Task 5 — Databricks REST API for Job Management

📖 **Context**: The exam tests your ability to interact with Databricks programmatically via REST API.

🛠️ **Instructions**:

### Step 1 — Set up authentication:

```python
import requests
import time
import json

# Configuration
DATABRICKS_HOST = "https://adb-123456789.azuredatabricks.net"
DATABRICKS_TOKEN = dbutils.secrets.get("my_secrets", "databricks_token")

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}
```

### Step 2 — List all jobs:

```python
# List jobs
response = requests.get(
    f"{DATABRICKS_HOST}/api/2.1/jobs/list",
    headers=headers
)

jobs = response.json().get("jobs", [])
for job in jobs:
    print(f"Job ID: {job['job_id']}, Name: {job['settings']['name']}")
```

### Step 3 — Trigger job run:

```python
# Run job immediately
job_id = 12345

response = requests.post(
    f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
    headers=headers,
    json={
        "job_id": job_id,
        "notebook_params": {
            "catalog": "training_uc",
            "schema": "bronze"
        }
    }
)

run_id = response.json()["run_id"]
print(f"Triggered run: {run_id}")
```

### Step 4 — Poll run status:

```python
# Poll until completion
while True:
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
        headers=headers,
        params={"run_id": run_id}
    )
    
    run_info = response.json()
    state = run_info["state"]["life_cycle_state"]
    result_state = run_info["state"].get("result_state")
    
    print(f"Status: {state}, Result: {result_state}")
    
    if state == "TERMINATED":
        if result_state == "SUCCESS":
            print("✅ Job completed successfully")
        else:
            print(f"❌ Job failed with state: {result_state}")
        break
    
    time.sleep(10)  # Wait 10 seconds before checking again
```

### Step 5 — Common API endpoints:

| Endpoint | Method | Purpose |
|----------|--------|----------|
| `/api/2.1/jobs/list` | GET | List all jobs |
| `/api/2.1/jobs/run-now` | POST | Trigger job run |
| `/api/2.1/jobs/runs/get` | GET | Get run status |
| `/api/2.1/jobs/runs/cancel` | POST | Cancel running job |
| `/api/2.1/clusters/list` | GET | List clusters |
| `/api/2.1/dbfs/list` | GET | List DBFS files |

✅ **Expected outcome**: 
- Can authenticate with Databricks API
- Can list jobs programmatically
- Can trigger and monitor job runs
- Understand polling pattern for async operations

⚠️ **Exam trap**: Thinking API calls are synchronous. Wrong! `run-now` returns immediately with a `run_id`. You must POLL the run status to know when it completes.

---

## Task 6 — Unit Testing with pytest

📖 **Context**: The exam tests your knowledge of testing Databricks code before deployment.

🛠️ **Instructions**:

### Step 1 — Create test file (`tests/test_transformations.py`):

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

# Initialize local Spark session for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("unit-tests") \
        .getOrCreate()

def test_filter_valid_orders(spark):
    # Create test data
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount", DecimalType(10,2), True),
        StructField("status", StringType(), True)
    ])
    
    data = [
        ("ORD001", 100.00, "completed"),
        ("ORD002", -50.00, "completed"),  # Invalid: negative amount
        ("ORD003", 200.00, "completed")
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Apply transformation
    result_df = df.filter("amount > 0")
    
    # Assert
    assert result_df.count() == 2
    assert result_df.filter("order_id = 'ORD002'").count() == 0

def test_aggregate_by_status(spark):
    # Test aggregation logic
    data = [
        ("ORD001", 100.00, "completed"),
        ("ORD002", 150.00, "completed"),
        ("ORD003", 200.00, "pending")
    ]
    
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("amount", DecimalType(10,2), True),
        StructField("status", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Aggregate
    result_df = df.groupBy("status").sum("amount")
    
    # Assert
    completed_total = result_df.filter("status = 'completed'").collect()[0]["sum(amount)"]
    assert completed_total == 250.00
```

### Step 2 — Run tests:

```bash
# Install pytest
pip install pytest

# Run all tests
pytest tests/

# Run with verbose output
pytest -v tests/

# Run specific test
pytest tests/test_transformations.py::test_filter_valid_orders
```

### Step 3 — Key testing concepts:

| Concept | Purpose | Example |
|---------|---------|----------|
| **Unit test** | Test individual functions | Test data transformation logic |
| **Integration test** | Test end-to-end flow | Test full ETL pipeline |
| **Fixture** | Reusable test setup | Spark session for all tests |
| **Assert** | Verify expected outcome | `assert df.count() == 10` |

✅ **Expected outcome**: 
- Unit tests run locally (no Databricks cluster needed)
- Tests use local Spark (`master("local[*]")`)
- Can test transformations before deployment
- Tests are fast and repeatable

⚠️ **Exam trap**: Thinking unit tests require a Databricks cluster. Wrong! Unit tests should use LOCAL Spark for speed. Integration tests run on real clusters.

---

## Task 7 — Concept Quiz

Answer these rapid-fire questions:

1. What does `databricks bundle validate` do?
2. What is the difference between `mode: development` and `mode: production`?
3. What CLI command deploys a bundle to the `prod` target?
4. Should you use personal tokens or service principals in CI/CD?
5. What does `dbutils.secrets.get()` return when printed?
6. What is the difference between Databricks-backed and Azure Key Vault-backed secret scopes?
7. What REST API endpoint triggers a job run?
8. Do you need a Databricks cluster to run unit tests?
9. What is the purpose of `--dry-run` flag?
10. What happens when you run `databricks bundle destroy`?

---

## Key Takeaways for the Exam

✅ **Databricks Asset Bundles:**
- **Structure**: `databricks.yml` + `resources/` directory
- **Targets**: `dev` (development mode) vs `prod` (production mode)
- **Variables**: `${var.catalog}` for parameterization
- **Commands**: `validate` → `deploy` → `run` → `destroy`

✅ **CI/CD:**
- **Service Principals**: Always use in production (never personal tokens)
- **GitHub Secrets**: Store credentials securely
- **Workflow**: Test → Validate → Deploy
- **Staging vs Prod**: Deploy to staging on PR, prod on merge

✅ **Secrets:**
- **Databricks-backed**: Simple, free, stored in Databricks
- **Azure Key Vault-backed**: Enterprise, centralized, audit logs
- **`dbutils.secrets.get()`**: Returns `[REDACTED]` when printed
- **Never viewable**: Once stored, secrets can only be used, not viewed

✅ **REST API:**
- **Authentication**: Bearer token in Authorization header
- **Job management**: `run-now` (async) + polling for status
- **Common endpoints**: `/jobs/list`, `/jobs/run-now`, `/jobs/runs/get`

✅ **Testing:**
- **Unit tests**: Local Spark, fast, test logic
- **Integration tests**: Real cluster, test end-to-end
- **pytest**: Standard Python testing framework
- **Fixtures**: Reusable setup (like Spark session)

---

## Next Steps

Congratulations! 🎉 You've completed all 7 days of Week 1 (Professional track). You now have:

- ✅ Deep understanding of Delta Lake internals
- ✅ Mastery of Delta Live Tables
- ✅ Workflow and job orchestration skills
- ✅ Unity Catalog and data governance expertise
- ✅ CI/CD and deployment automation knowledge

**Next:** Move to Week 2 for advanced topics and mock exams!
