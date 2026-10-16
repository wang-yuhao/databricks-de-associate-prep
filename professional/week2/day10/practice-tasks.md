# Day 10 Practice Tasks — DABs & CI/CD

> **Exam section:** Debugging and Deploying (10%)
> **Prerequisite:** Read `study-notes.md` completely before starting these tasks.
> **Estimated time:** 2-3 hours
> **Difficulty:** 🔥🔥🔥 Professional Level

---

## Task 1 — Create Basic DABs Structure

📖 **Context:** Databricks Asset Bundles (DABs) are infrastructure-as-code for Databricks. Critical for Professional exam.

🛠️ **Instructions:**

**Step 1 — Initialize bundle structure:**
```bash
# Create project structure
mkdir -p my_de_project/{resources/jobs,resources/pipelines,src}
cd my_de_project

# Create root databricks.yml
cat > databricks.yml <<EOF
bundle:
  name: de_professional_prep

variables:
  catalog:
    default: dev_catalog
  env:
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-xxx.azuredatabricks.net
    variables:
      catalog: dev_catalog
      env: dev
  
  prod:
    mode: production
    workspace:
      host: https://adb-yyy.azuredatabricks.net
    variables:
      catalog: prod_catalog
      env: prod
    permissions:
      - group_name: data-engineers
        level: CAN_MANAGE_RUN
EOF
```

✅ **Expected:** Basic bundle structure with dev/prod targets

⚠️ **Exam trap:** `mode: development` prepends username to resources. `mode: production` enforces strict settings.

---

## Task 2 — Define Job in DABs

📖 **Context:** Jobs are defined in YAML and deployed via `databricks bundle deploy`.

🛠️ **Instructions:**

**Step 1 — Create job configuration:**
```yaml
# resources/jobs/etl_job.yml
resources:
  jobs:
    etl_pipeline:
      name: ETL Pipeline - ${var.env}
      tags:
        environment: ${var.env}
      
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: Europe/Berlin
        pause_status: UNPAUSED
      
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./src/01_ingest
            base_parameters:
              catalog: ${var.catalog}
              env: ${var.env}
          job_cluster_key: main_cluster
        
        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./src/02_transform
            base_parameters:
              catalog: ${var.catalog}
          job_cluster_key: main_cluster
        
        - task_key: dlt_pipeline
          depends_on:
            - task_key: transform
          pipeline_task:
            pipeline_id: ${resources.pipelines.silver_pipeline.id}
      
      job_clusters:
        - job_cluster_key: main_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
            spark_conf:
              spark.databricks.delta.optimizeWrite.enabled: "true"
```

✅ **Expected:** Job with 3 tasks (notebook, notebook, DLT pipeline)

⚠️ **Exam trap:** `pipeline_task` references pipeline by ID: `${resources.pipelines.name.id}`. Must match defined pipeline.

---

## Task 3 — Define DLT Pipeline in DABs

📖 **Context:** DLT pipelines defined in YAML, notebooks in src/

🛠️ **Instructions:**

```yaml
# resources/pipelines/silver_pipeline.yml
resources:
  pipelines:
    silver_pipeline:
      name: Silver Pipeline - ${var.env}
      target: ${var.catalog}.silver
      catalog: ${var.catalog}
      development: ${var.env == 'dev'}
      continuous: false
      channel: CURRENT
      
      libraries:
        - notebook:
            path: ./src/dlt_transformations
      
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4
          mode: ENHANCED
```

✅ **Expected:** DLT pipeline referencing notebook

⚠️ **Exam trap:** `development: true` in dev, `false` in prod. Affects pipeline behavior.

---

## Task 4 — Deploy Bundle

📖 **Context:** `databricks bundle` CLI commands deploy resources.

🛠️ **Instructions:**

```bash
# Validate bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy --target dev

# Show deployed resources
databricks bundle summary

# Run job
databricks bundle run etl_pipeline --target dev

# Deploy to prod
databricks bundle deploy --target prod

# Destroy resources
databricks bundle destroy --target dev
```

✅ **Expected:** Resources deployed, visible in Databricks UI

⚠️ **Exam trap:** `bundle validate` checks syntax. `bundle deploy` actually creates resources.

---

## Task 5 — CI/CD with GitHub Actions

📖 **Context:** Professional exam tests CI/CD pipeline setup.

🛠️ **Instructions:**

**Step 1 — Create GitHub Actions workflow:**
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
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: pip install databricks-sdk pytest
      
      - name: Run unit tests
        run: pytest tests/unit/ -v
      
      - name: Validate bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DEV_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DEV_TOKEN }}
        run: databricks bundle validate

  deploy-dev:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main
      
      - name: Deploy to dev
        env:
          DATABRICKS_HOST: ${{ secrets.DEV_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DEV_TOKEN }}
        run: databricks bundle deploy --target dev

  deploy-prod:
    needs: deploy-dev
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main
      
      - name: Deploy to prod
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
        run: databricks bundle deploy --target prod
```

✅ **Expected:** Automated deployment on push to main

⚠️ **Exam trap:** Use SERVICE PRINCIPAL tokens in prod, never personal tokens. Store in GitHub Secrets.

---

## Task 6 — Write Unit Tests

📖 **Context:** Testing strategies are tested in Professional exam.

🛠️ **Instructions:**

```python
# tests/unit/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from src.transformations import clean_data, aggregate_sales

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("unit-tests") \
        .getOrCreate()

def test_clean_data_removes_nulls(spark):
    data = [(1, "A", 100), (2, None, 50), (3, "B", None)]
    df = spark.createDataFrame(data, ["id", "name", "amount"])
    
    result = clean_data(df)
    
    assert result.count() == 1
    assert result.first()["id"] == 1

def test_aggregate_sales(spark):
    data = [
        ("2026-07-01", "A", 100),
        ("2026-07-01", "A", 150),
        ("2026-07-02", "B", 200)
    ]
    df = spark.createDataFrame(data, ["date", "product", "amount"])
    
    result = aggregate_sales(df)
    
    assert result.count() == 2
    assert result.filter("product = 'A'").first()["total"] == 250
```

✅ **Expected:** Tests pass locally and in CI

⚠️ **Exam trap:** Unit tests use local Spark (`master("local[*]")`). Integration tests use Databricks cluster.

---

## Task 7 — Bundle Variables and Parameters

📖 **Context:** Variables enable parameterized configs.

🛠️ **Instructions:**

```yaml
# databricks.yml
variables:
  catalog:
    default: dev_catalog
    description: "Target catalog name"
  
  min_workers:
    default: 2
    description: "Minimum cluster workers"
  
  notification_email:
    default: "alerts@company.com"

targets:
  prod:
    variables:
      catalog: prod_catalog
      min_workers: 4
      notification_email: "prod-alerts@company.com"

resources:
  jobs:
    my_job:
      name: Job - ${var.catalog}
      email_notifications:
        on_failure: ["${var.notification_email}"]
      job_clusters:
        - new_cluster:
            num_workers: ${var.min_workers}
```

✅ **Expected:** Variables change per target

⚠️ **Exam trap:** Variables accessed via `${var.variable_name}`. Resources via `${resources.type.name.property}`.

---

## Concept Quiz

1. What command validates DABs syntax?
2. What command deploys DABs resources?
3. What's difference between `mode: development` and `mode: production`?
4. How to reference DLT pipeline ID in job?
5. Where do you store DATABRICKS_TOKEN for CI/CD?
6. What runs unit tests locally?
7. How to access variable in DABs YAML?
8. What's difference between job cluster and all-purpose cluster?
9. Can you deploy same bundle to multiple workspaces?
10. What happens when you `bundle destroy`?

**Answers:**
1. `databricks bundle validate`
2. `databricks bundle deploy --target <target>`
3. Development: prepends username. Production: strict settings, no personal clusters
4. `${resources.pipelines.pipeline_name.id}`
5. GitHub Secrets (or Azure Key Vault)
6. `pytest` with local Spark
7. `${var.variable_name}`
8. Job cluster: created per run, cheaper. All-purpose: manual, for interactive work
9. Yes — different targets in databricks.yml
10. Deletes all resources created by bundle

---

## Key Takeaways

✅ **DABs = infrastructure-as-code** for Databricks resources  
✅ **`databricks bundle validate/deploy/run/destroy`** — core CLI commands  
✅ **`mode: development`** prepends username — avoids conflicts  
✅ **`mode: production`** enforces stricter settings  
✅ **Variables** via `${var.name}` — enable parameterization  
✅ **Resources** via `${resources.type.name.property}` — reference other resources  
✅ **CI/CD:** Use service principal tokens, never personal tokens  
✅ **GitHub Actions:** validate → deploy dev → deploy prod  
✅ **Unit tests:** local Spark, fast feedback  
✅ **Integration tests:** real Databricks cluster, slower but realistic
