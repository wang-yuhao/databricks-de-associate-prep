# Day 10: Databricks Asset Bundles (DABs) & CI/CD

## Schedule
- Morning: DABs structure, configuration, deployment
- Afternoon: CI/CD pipelines with GitHub Actions
- Evening: Testing strategies and practice

---

## 10.1 Databricks Asset Bundles Overview

- DABs = infrastructure-as-code for Databricks resources
- Define jobs, pipelines, clusters in YAML
- Deploy across environments (dev/staging/prod)
- CLI: `databricks bundle deploy`

```yaml
# databricks.yml - root bundle config
bundle:
  name: my_de_project

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
```

---

## 10.2 DABs - Jobs Configuration

```yaml
# resources/jobs/etl_job.yml
resources:
  jobs:
    etl_pipeline_job:
      name: ETL Pipeline - ${var.env}
      tags:
        env: ${var.env}

      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: Europe/Berlin
        pause_status: UNPAUSED

      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: ./notebooks/01_ingest
            base_parameters:
              catalog: ${var.catalog}
          job_cluster_key: main_cluster

        - task_key: transform
          depends_on:
            - task_key: ingest
          notebook_task:
            notebook_path: ./notebooks/02_transform
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

---

## 10.3 DABs - DLT Pipeline Configuration

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
            path: ./pipelines/silver_transformations

      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4
            mode: ENHANCED
```

---

## 10.4 DABs CLI Commands

```bash
# Initialize new bundle
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Deploy to target
databricks bundle deploy --target dev
databricks bundle deploy --target prod

# Run a job
databricks bundle run etl_pipeline_job --target dev

# Destroy resources
databricks bundle destroy --target dev

# Show deployed resources
databricks bundle summary
```

---

## 10.5 CI/CD with GitHub Actions

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

  deploy-staging:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}
        run: databricks bundle deploy --target staging

  deploy-prod:
    needs: deploy-staging
    runs-on: ubuntu-latest
    environment: production
    steps:
      - uses: actions/checkout@v3

      - name: Setup Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to production
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
        run: databricks bundle deploy --target prod
```

---

## 10.6 Testing Strategies

```python
# tests/unit/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from src.transformations import clean_orders, calculate_revenue

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("unit-tests") \
        .getOrCreate()

def test_clean_orders_removes_nulls(spark):
    data = [(1, "A", 100.0), (2, None, 50.0), (3, "B", None)]
    df = spark.createDataFrame(data, ["id", "customer", "amount"])
    result = clean_orders(df)
    assert result.count() == 1
    assert result.first()["id"] == 1

def test_calculate_revenue(spark):
    data = [(1, 100.0, 2), (2, 50.0, 3)]
    df = spark.createDataFrame(data, ["id", "price", "qty"])
    result = calculate_revenue(df)
    assert result.filter("id = 1").first()["revenue"] == 200.0
```

---

## Key Exam Points

- DABs use `databricks.yml` as root config
- `targets` define dev/staging/prod environments
- `variables` allow parameterized configs
- `databricks bundle deploy --target <env>` deploys resources
- CI/CD: use service principal tokens (not personal tokens) in prod
- `mode: development` prepends username to resource names to avoid conflicts
- `mode: production` enforces stricter settings (no personal clusters)
- DLT pipelines in DABs: `pipeline_task` references pipeline by ID
- GitHub Actions secrets store DATABRICKS_HOST and DATABRICKS_TOKEN

---

## Practice Tasks
- [ ] Create a DABs bundle with dev and prod targets
- [ ] Define a job with 3 tasks (ingest, transform, DLT)
- [ ] Write a GitHub Actions workflow for CI/CD
- [ ] Write unit tests using pytest and local Spark
- [ ] Run `bundle validate` and `bundle deploy`
