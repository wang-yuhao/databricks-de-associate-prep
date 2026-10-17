# Course 4: Automated Deployment with Declarative Automation Bundles

DevOps for data engineering: describe jobs, pipelines and their code as a **bundle**, deploy it to several environments with the Databricks CLI, and automate it with CI/CD.

**Prerequisites:** Databricks jobs and pipelines, Python packaging basics, Git.

## Lesson map

| Official lesson | Practice here |
|---|---|
| DevOps Review · CI/CD Review | [`notebooks/4.1`](notebooks/4.1-demo-course-setup-and-authentication.ipynb) section C, [key concepts](#key-concepts) |
| **Demo:** Course Setup and Authentication · Deploying Databricks Projects | [`notebooks/4.1-demo-course-setup-and-authentication`](notebooks/4.1-demo-course-setup-and-authentication.ipynb) |
| Introduction to DABs · **Demo:** Deploying a Simple DAB | [`notebooks/4.2-demo-deploying-a-simple-dab`](notebooks/4.2-demo-deploying-a-simple-dab.ipynb) · [`bundles/01_simple_dab`](bundles/01_simple_dab) |
| **Lab:** Deploying a Simple DAB | [`notebooks/4.3-lab-deploying-a-simple-dab`](notebooks/4.3-lab-deploying-a-simple-dab.ipynb) · [`labs/4.3_simple_dab`](labs/4.3_simple_dab) · [`solutions/4.3_simple_dab`](solutions/4.3_simple_dab) |
| Variable Substitutions in DABs · **Demo:** Deploying a DAB to Multiple Environments | [`notebooks/4.4-demo-...`](notebooks/4.4-demo-deploying-a-dab-to-multiple-environments.ipynb) · [`bundles/02_multi_env_dab`](bundles/02_multi_env_dab) |
| **Lab:** Deploy a DAB to Multiple Environments | [`notebooks/4.5-lab-...`](notebooks/4.5-lab-deploy-a-dab-to-multiple-environments.ipynb) · [`labs/4.5_multi_env_dab`](labs/4.5_multi_env_dab) · [`solutions/4.5_multi_env_dab`](solutions/4.5_multi_env_dab) |
| DAB Project Templates Overview · **Lab:** Use a Databricks Default DAB Template | [`notebooks/4.6-lab-use-a-default-dab-template`](notebooks/4.6-lab-use-a-default-dab-template.ipynb) |
| CI/CD Project Overview with DABs · **Demo:** CI/CD with DABs · CI/CD Best Practices | [`notebooks/4.7-demo-ci-cd-with-dabs`](notebooks/4.7-demo-ci-cd-with-dabs.ipynb) · [`bundles/03_cicd_dab`](bundles/03_cicd_dab) |
| **Lab:** Adding ML to Engineering Workflows with DABs | [`notebooks/4.8-lab-...`](notebooks/4.8-lab-adding-ml-to-engineering-workflows.ipynb) · [`labs/4.8_ml_workflow`](labs/4.8_ml_workflow) · [`solutions/4.8_ml_workflow`](solutions/4.8_ml_workflow) |
| Developing Locally with VS Code · **Demo:** Using VS Code with Databricks | [`docs/4.9-using-vscode-with-databricks.md`](docs/4.9-using-vscode-with-databricks.md) |
| Next Steps: Automated Deployment with GitHub Actions | [`bundles/03_cicd_dab/ci/github-actions/bundle-ci-cd.yml`](bundles/03_cicd_dab/ci/github-actions/bundle-ci-cd.yml) |

## Offline vs. workspace
| Without a workspace | With a workspace (Databricks Free Edition is enough) |
|---|---|
| Read and edit bundles, `bundles.preview(dir, target)` (target applied, `${...}` resolved, dev-mode effects), `bundles.validate_schema(dir)` (the CLI's own JSON schema), unit tests, wheel build | `databricks bundle validate / deploy / run / summary / destroy`, templates (`bundle init`) |

Notebook cells that need a workspace detect it (`bundles.workspace_status()`) and print `SKIPPED` until you run `databricks auth login --host <url>` in a terminal.

> The notebooks only deploy when **you** run those cells, and only to the workspace of your CLI profile. Everything deployed by the course uses your user folder (dev) or `/Workspace/Shared/.bundle/...` (stage/prod) and can be removed with `databricks bundle destroy`.

## Key concepts
- **DevOps / DataOps**: version control, automated testing, automated deployment, and environments for data products. **CI** = every change is integrated and tested. **CD** = every tested change is deployable (delivery) or deployed (deployment).
- Deployment options: UI (manual), REST API, SDK, CLI (imperative), **bundles** (declarative, the recommended way to ship projects).
- **Bundle anatomy**: `databricks.yml` (`bundle`, `include`, `variables`, `artifacts`, `resources`, `targets`, `permissions`, `run_as`, `sync`), resource files under `resources/`, code under `src/`.
- **Targets and modes**: `mode: development` (name prefix `[dev user]`, schedules paused, per-user folder, development pipelines) vs `mode: production` (strict validation, shared root path, run as a service principal).
- **Substitutions**: `${var.x}`, `${bundle.name}`, `${bundle.target}`, `${workspace.current_user.userName}`, `${workspace.file_path}`, `${resources.jobs.<key>.id}`. Override variables with `--var="x=y"` or `BUNDLE_VAR_x`.
- **Lifecycle**: `init → validate → deploy → run → destroy`. `deploy` is idempotent: it computes and applies the difference. State lives in the workspace (`.bundle/<name>/<target>/state`).
- **Templates**: `default-python`, `default-sql`, `default-minimal`, `pydabs` (resources in Python), `dbt-sql`, `mlops-stacks`, or your organization's own template.
- **CI/CD**: unit tests on local Spark → `bundle validate` → deploy to stage with an **OAuth M2M service principal** → integration run → approved deploy to prod. Secrets live in the CI secret store and are referenced by name only.

## Run it on Databricks Free Edition
Step by step with every CLI command: [`../FREE_EDITION_GUIDE.md`](../FREE_EDITION_GUIDE.md) → Course 4. After each deploy, run [`databricks/4.x-inspect-deployments`](databricks/4.x-inspect-deployments.py) in the workspace to see the deployed jobs, pipelines, bundle folders, tables per environment, runs and models.
