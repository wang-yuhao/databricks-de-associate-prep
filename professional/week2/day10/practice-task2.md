# Day 10 Practice Tasks — Databricks Certified Data Engineer Associate (Updated for July 2026)

This document replaces an unclear task page with a complete, step-by-step lab and study guide for the Databricks Certified Data Engineer Associate exam. It starts from environment setup, explains each command and file, and aligns the workflow with current July 2026 Databricks terminology and documentation.[cite:2][cite:3][cite:4][cite:5][cite:17][cite:22]

## What changed in 2026

Databricks documentation now refers to Asset Bundles as **Declarative Automation Bundles**, and the current workflow is centered on the Databricks CLI `bundle` command group for validate, deploy, run, and destroy operations.[cite:2][cite:3][cite:5] The official certification page for the Data Engineer Associate exam lists 45 questions, a 90-minute time limit, and a $200 registration fee, so this guide is written to help build practical familiarity with the platform topics that commonly appear on the exam.[cite:4]

## Learning goals

By the end of this exercise, the learner should be able to:

- Set up the Databricks CLI and authenticate to a workspace.[cite:22][cite:32]
- Create a valid bundle project with `databricks.yml` at the root.[cite:7][cite:22]
- Define a job resource and understand how bundle resource keys are referenced.[cite:22]
- Validate, deploy, run, and destroy a dev deployment safely.[cite:3][cite:7]
- Distinguish development and production deployment targets.[cite:17]
- Connect this hands-on work to exam topics such as orchestration, governance, pipelines, and troubleshooting.[cite:18][cite:24][cite:28][cite:29]

## Before starting

This lab assumes access to a Databricks workspace, a terminal on macOS/Linux/WSL, Git, and permission to create jobs in a development workspace. The current Databricks docs recommend local development for bundles and support splitting configuration across multiple YAML files with a root `databricks.yml` file.[cite:7][cite:22]

Recommended local tools:

- Databricks CLI, current version.
- Python 3.10+ for optional local testing.
- VS Code or another editor with YAML support.
- GitHub account if CI/CD practice is included.

## Step 1 — Understand the exam context

Start by anchoring the exercise to the current certification. The official Databricks certification page confirms that the Databricks Certified Data Engineer Associate exam is still active and identifies the exam format and price.[cite:4] Third-party 2026 exam commentary indicates the exam blueprint changed in May 2026 to seven domains, so older study plans based on five domains should be treated carefully unless cross-checked against the current guide.[cite:18][cite:24]

### Why this matters

Exam preparation is easier when each hands-on task maps to a likely tested concept. This lab covers deployment automation, jobs, environment separation, and platform operations, which connect well to orchestration and troubleshooting topics.[cite:18][cite:24]

## Step 2 — Install the Databricks CLI

The Databricks CLI is required because bundle workflows are managed with CLI commands such as `bundle validate`, `bundle deploy`, `bundle run`, and `bundle destroy`.[cite:3][cite:7] The docs also note that bundle schema and initialization workflows require Databricks CLI version 0.218.0 or above.[cite:19][cite:22]

### Install

Choose the installation method appropriate for the operating system, then verify the installation:

```bash
databricks --version
```

### Explanation

This confirms that the executable is available in the shell path. If the command fails, fix installation or PATH issues before continuing, because every later step depends on the CLI.[cite:32]

## Step 3 — Authenticate to the workspace

Authenticate to a development workspace first, not production. Bundles are safest to learn in an isolated target where mistakes do not affect shared resources.[cite:7][cite:17]

### Command

```bash
databricks auth login --host https://<your-workspace-url>
```

### Explanation

This creates a local authentication profile the CLI can use to validate and deploy resources. If the organization uses multiple workspaces, create a separate profile for dev and prod so the target separation stays explicit.[cite:32]

### Verify authentication

```bash
databricks current-user me
```

If this returns the logged-in principal, authentication is working and the CLI can reach the workspace APIs.[cite:32]

## Step 4 — Create the project directory

Create a clean local project folder because bundle configuration expects a predictable structure rooted at `databricks.yml`.[cite:7][cite:22]

### Commands

```bash
mkdir de-associate-day10
cd de-associate-day10
mkdir -p src resources .databricks
```

### Explanation

- `src/` stores notebooks, Python files, or other source code.
- `resources/` stores modular YAML resource definitions.
- `.databricks/` can store local helper files such as the generated JSON schema.

This separation improves readability and makes validation errors easier to troubleshoot.

## Step 5 — Generate a bundle schema file

The Azure Databricks documentation recommends generating the bundle configuration JSON schema using CLI 0.218.0 or above.[cite:22] This improves editor validation and makes YAML authoring less error-prone.[cite:22]

### Command

```bash
databricks bundle schema > .databricks/bundle_config_schema.json
```

### Explanation

This writes the current configuration schema to a local file. In editors such as VS Code, attaching the schema gives auto-complete, type hints, and earlier detection of invalid keys.[cite:22]

## Step 6 — Create the root `databricks.yml`

The root configuration file is mandatory for a bundle project.[cite:7][cite:22] Place the following in `databricks.yml`:

```yaml
bundle:
  name: de-associate-day10

include:
  - resources/*.yml

workspace:
  host: https://<your-workspace-url>

artifacts:
  default:
    type: whl
    path: .

targets:
  dev:
    default: true
    mode: development
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}

  prod:
    mode: production
    workspace:
      root_path: /Workspace/Shared/.bundle/${bundle.name}/${bundle.target}
```

### Explanation

- `bundle.name` identifies the deployment package.
- `include` lets the root file pull in resource YAML files from `resources/`.[cite:7]
- `workspace.host` identifies the target workspace.
- `targets.dev` sets a default development target.
- `mode: production` in the prod target follows the documented production deployment mode pattern.[cite:17]

### Why two targets matter

The deployment mode docs distinguish development and production behavior, so using separate targets mirrors real engineering practice and helps reinforce exam concepts around safe deployment discipline.[cite:17]

## Step 7 — Add a job resource

Create `resources/job.yml` with a simple job definition:

```yaml
resources:
  jobs:
    hello_job:
      name: hello-job-${bundle.target}
      tasks:
        - task_key: hello_notebook_task
          notebook_task:
            notebook_path: ../src/hello_notebook.py
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 1
```

### Explanation

- `hello_job` is the resource key used by bundle commands.
- `name` is the deployed workspace object name and can include the target name.
- `tasks` defines the workload executed by the job.
- `notebook_task` points to the source file.
- `new_cluster` creates ephemeral compute for the run.

The exact cluster fields available depend on workspace and cloud, but the core point for the exam is understanding that jobs orchestrate tasks and that resource keys are how bundles reference managed objects.[cite:22]

## Step 8 — Add the notebook or Python task file

Create `src/hello_notebook.py`:

```python
# Databricks notebook source
print("Hello from the Day 10 bundle lab")
```

### Explanation

This file is intentionally simple. The goal is not advanced PySpark logic yet; the goal is to validate that the bundle can package code, deploy it, and run a Databricks job successfully.

## Step 9 — Validate the bundle

Before deploying anything, run validation. The docs place validation early in the workflow because it catches structural and schema problems before resources are created.[cite:3][cite:7]

### Command

```bash
databricks bundle validate -t dev
```

### Explanation

Validation checks the configuration against the schema and verifies that references are consistent. If this step fails, fix errors here instead of trying to debug them during deployment.

### Common validation issues

- Wrong indentation in YAML.
- Missing root `databricks.yml`.
- Resource files not matched by the `include` pattern.
- Unsupported or misspelled keys.
- Invalid workspace host value.

## Step 10 — Deploy to the development target

After validation succeeds, deploy to the dev target.[cite:3][cite:7]

### Command

```bash
databricks bundle deploy -t dev
```

### Explanation

This creates or updates the resources defined in the bundle. In this example, the job resource appears in the Databricks workspace under the dev target path specified in the configuration.[cite:7][cite:17]

### What to check in the workspace

- The job exists.
- The job name includes the target suffix.
- The workspace path matches the configured root path.
- No unexpected shared or production resources were touched.

## Step 11 — Run the job

Once deployed, run the job using the top-level resource key.[cite:22]

### Command

```bash
databricks bundle run hello_job -t dev
```

### Explanation

`hello_job` is not the display name of the job; it is the resource key from the YAML. This distinction is important and commonly confuses beginners, so the document should explain it clearly.[cite:22]

### Expected result

A job run starts in the dev workspace. If the compute configuration is valid and permissions are sufficient, the notebook task completes and prints the test message.

## Step 12 — Destroy dev resources when done

Bundles also support cleanup through `bundle destroy`.[cite:3][cite:7]

### Command

```bash
databricks bundle destroy -t dev
```

### Explanation

This removes resources managed by the bundle in the selected target. It is useful in training environments because it prevents orphaned jobs and teaches clean lifecycle management.

## Step 13 — Add a production target carefully

The Databricks docs describe production mode as an explicit target configuration.[cite:17] Keep production separate from development and deploy only after dev validation is stable.

### Good production practices

- Use a separate workspace or at least separate paths.
- Require pull request review before prod deploys.
- Use secrets instead of hard-coded credentials.
- Keep names deterministic and environment-scoped.
- Validate in CI before deployment.[cite:10][cite:17]

## Step 14 — Extend the bundle with variables

Many real deployments need environment-specific values. Add variables to avoid hard-coding data paths, catalog names, or schedule behavior.

Example snippet for `databricks.yml`:

```yaml
variables:
  catalog:
    default: dev_catalog
  schema:
    default: bronze
```

### Explanation

Variables make configuration more reusable. This is especially helpful when moving from local practice to prod-like structure, and it aligns with the bundle goal of applying software engineering practices to Databricks workflows.[cite:5]

## Step 15 — Understand pipelines in current terminology

Lakeflow pipelines are active current platform terminology in Databricks 2026 release notes, and recent releases mention features such as new flow syntax for streaming tables.[cite:29] A learner preparing in July 2026 should not rely solely on older Delta Live Tables wording without recognizing the current Lakeflow naming in docs and release notes.[cite:29][cite:31]

### Why this matters for the exam

The exam may still test concepts that originated under older names, but the platform vocabulary continues to evolve. Understanding both the historical term and the current term reduces confusion when reading docs, courses, and practice questions.[cite:29]

## Step 16 — Connect this lab to Unity Catalog

Unity Catalog remains central to Databricks governance. Current platform docs describe Unity Catalog as the account-level governance system for data, AI assets, and permissions across catalogs, schemas, and tables.[cite:34] Other docs also show that workspace enablement and cloud-storage-linked securables remain important operational concepts.[cite:28][cite:36]

### Exam relevance

Know the difference between:

- Catalog, schema, and table.
- Managed and external data patterns.
- Privilege inheritance and access control basics.
- Why governance affects job and pipeline behavior.

## Step 17 — Add CI validation with GitHub Actions

Community and partner guidance in 2026 still frames bundles as the recommended deployment approach for CI/CD on Databricks, with GitHub Actions used to validate and deploy bundle changes.[cite:10][cite:12] This is a practical extension for learners who already use GitHub professionally.

Example workflow outline:

```yaml
name: databricks-ci

on:
  pull_request:
  push:
    branches: [ main ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: |
          echo "Install CLI here"
      - name: Validate bundle
        run: databricks bundle validate -t dev

  deploy-prod:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    needs: validate
    steps:
      - uses: actions/checkout@v4
      - name: Deploy bundle
        run: databricks bundle deploy -t prod
```

### Explanation

The important exam-level concept is not memorizing YAML for GitHub Actions. It is understanding the operational flow: validate first, then deploy, and keep production gated behind version control and review.[cite:10][cite:17]

## Step 18 — Add local testing for Python logic

For bundle-managed Databricks projects, local testing helps separate code quality problems from deployment problems. Unit tests do not replace Databricks integration testing, but they reduce debugging time and reflect standard engineering discipline.

### Recommended approach

- Put reusable Python logic in importable modules.
- Test transformations locally with `pytest` where possible.
- Reserve workspace runs for integration validation.

### Why this matters

This helps learners build the right debugging order: local code correctness first, deployment correctness second, runtime permissions and compute issues third.

## Step 19 — Troubleshooting checklist

Use this checklist when the bundle does not work as expected.

| Problem | Likely cause | Fix |
|---|---|---|
| `bundle validate` fails | YAML indentation, unsupported key, bad include path | Check schema-backed YAML and file layout.[cite:22] |
| `bundle deploy` fails | Bad auth, missing permissions, invalid workspace config | Re-check login, host, and target configuration.[cite:7][cite:32] |
| `bundle run` fails | Wrong resource key, compute mismatch, path issue | Use the top-level resource key and verify cluster settings.[cite:22] |
| Prod deploy is risky | No target separation | Use distinct dev and prod targets with explicit mode settings.[cite:17] |
| Pipeline terminology is confusing | Old DLT-focused notes | Use current Lakeflow docs and release notes terminology.[cite:29][cite:31] |

## Step 20 — Suggested practice tasks

Complete the following in order:

1. Install the CLI and verify the version.[cite:22][cite:32]
2. Authenticate to a dev workspace.[cite:32]
3. Create the folder structure and generate the bundle schema.[cite:22]
4. Write `databricks.yml` and `resources/job.yml`.[cite:7][cite:17]
5. Validate the bundle.[cite:3]
6. Deploy to `dev`.[cite:7]
7. Run `hello_job`.[cite:22]
8. Add a second task to the job, such as a Python wheel or notebook task.
9. Add variables for catalog and schema.
10. Add a `prod` target with `mode: production`.[cite:17]
11. Destroy the `dev` deployment when finished.[cite:3]
12. Explain how this workflow supports CI/CD and safer promotion across environments.[cite:10][cite:12]

## Step 21 — What to memorize for the exam

Memorize the concepts, not only the commands:

- Bundles are Databricks’ infrastructure-as-code style deployment mechanism for workflows and related resources.[cite:5]
- Current docs use Declarative Automation Bundles terminology.[cite:2]
- `validate`, `deploy`, `run`, and `destroy` are the core bundle lifecycle commands.[cite:3][cite:7]
- `mode: production` is explicitly set in a target configuration for production behavior.[cite:17]
- Unity Catalog governs metadata and permissions across catalogs, schemas, and tables.[cite:34]
- Current pipeline vocabulary includes Lakeflow terminology in 2026 docs and release notes.[cite:29][cite:31]

## Step 22 — Final mini review

A strong Day 10 answer should show practical understanding, not just copied syntax. The learner should be able to explain what each file does, why dev and prod targets are separated, why validation comes before deployment, and how this deployment style fits modern Databricks engineering practice.[cite:5][cite:7][cite:10][cite:17]

## Appendix — Minimal file layout

```text
de-associate-day10/
├── databricks.yml
├── resources/
│   └── job.yml
├── src/
│   └── hello_notebook.py
└── .databricks/
    └── bundle_config_schema.json
```

## Appendix — Quick command recap

```bash
# verify CLI
databricks --version

# authenticate
databricks auth login --host https://<your-workspace-url>
databricks current-user me

# generate schema
databricks bundle schema > .databricks/bundle_config_schema.json

# validate
databricks bundle validate -t dev

# deploy
databricks bundle deploy -t dev

# run
databricks bundle run hello_job -t dev

# destroy
databricks bundle destroy -t dev
```

These are the core commands to practice until they feel routine, because they reinforce both real project workflow and exam-oriented platform understanding.[cite:3][cite:7][cite:22]
