# Day 7 Practice Tasks — Databricks Asset Bundles (DABs) & CI/CD

## Task 1 — Bundle Initialization
1. Create a `databricks.yml` file in the root of this repo that defines:
   - A bundle named `prep_etl_bundle`
   - Two targets: `dev` (default) and `prod`
   - At least one job resource called `daily_etl_job` with one notebook task
   - A variable `catalog` with default value `training`

```yaml
# Your databricks.yml here
```

---

## Task 2 — Bundle Commands
Write the CLI commands to:
1. Validate the bundle without deploying
2. Deploy to the `dev` target
3. Run the `daily_etl_job` in the `dev` target
4. Deploy to `prod`
5. Tear down all resources in `dev`

```bash
# Your commands here
```

---

## Task 3 — GitHub Actions Workflow
Write a minimal GitHub Actions workflow (`.github/workflows/deploy.yml`) that:
- Triggers on push to `main`
- Runs unit tests with pytest
- Deploys to `staging` only if tests pass
- Uses `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from GitHub Secrets

```yaml
# Your workflow here
```

---

## Task 4 — Secrets Management
Answer the following:
1. What is the difference between a **Databricks-backed** secret scope and an **Azure Key Vault-backed** scope?
2. Why is `dbutils.secrets.get()` output always `[REDACTED]`?
3. Write Python code that reads a secret and uses it in a JDBC connection string.

```python
# Your Python here
```

---

## Task 5 — REST API
Write Python code using the `requests` library to:
1. List all jobs in a workspace
2. Trigger job ID `99999` immediately
3. Poll the run status every 10 seconds until it reaches `TERMINATED`

```python
# Your Python here
```

---

## Task 6 — Concept Short-Answer
1. What is the difference between `mode: development` and `mode: production` in a DABs target?
2. How do DABs variables (`${var.catalog}`) differ from Databricks Widgets?
3. What command would you run to see what changes a `bundle deploy` would make before actually deploying?
