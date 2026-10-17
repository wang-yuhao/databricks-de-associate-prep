# Databricks notebook source
# MAGIC %md
# MAGIC # Course 4 - Inspect what your bundles deployed (Databricks Free Edition)
# MAGIC Run this in the workspace after each `databricks bundle deploy` / `run` / `destroy` from your laptop.
# MAGIC It uses the Databricks SDK (preinstalled on serverless) with the notebook's own identity, no tokens needed.
# MAGIC Guide: `FREE_EDITION_GUIDE.md` → Course 4.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me().user_name
print("you are", me)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs and pipelines created by bundles
# MAGIC Dev-mode resources are prefixed `[dev <you>]` and their schedules are **paused**; production-mode resources keep
# MAGIC their name and schedule. Bundle-managed resources show a *"managed by a bundle"* banner in the UI and should not be
# MAGIC edited there (the next deploy overwrites UI edits).

# COMMAND ----------

rows = []
for j in w.jobs.list():
    s = j.settings
    if s is None:
        continue
    rows.append(("job", s.name, str(j.job_id), (s.schedule.pause_status.value if s.schedule and s.schedule.pause_status else "-"),
                 (s.deployment.kind.value if s.deployment else "UI/API"), j.creator_user_name))
for p in w.pipelines.list_pipelines():
    rows.append(("pipeline", p.name, p.pipeline_id, "-", "-", p.creator_user_name))
display(spark.createDataFrame(rows, "type STRING, name STRING, id STRING, schedule STRING, deployed_by STRING, creator STRING")
        .orderBy("type", "name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bundle deployment folders and state
# MAGIC dev: `/Workspace/Users/<you>/.bundle/<bundle>/<target>` · stage/prod (this course): `/Workspace/Shared/.bundle/...`.
# MAGIC `files/` holds the synced source, `state/` the remote deployment state that makes `deploy` idempotent
# MAGIC (typically `terraform.tfstate` with the default Terraform engine, or `resources.json` with the newer direct engine,
# MAGIC plus `deployment.json` / `metadata.json`; exact file names depend on the CLI version).
# MAGIC The Workspace API takes paths without the `/Workspace` prefix (`/Users/...`, `/Shared/...`).

# COMMAND ----------

from databricks.sdk.service.workspace import ObjectType


def tree(path, depth=0, max_depth=3):
    try:
        for o in sorted(w.workspace.list(path), key=lambda o: o.path):
            print("  " * depth + ("[d] " if o.object_type == ObjectType.DIRECTORY else "    ") + o.path.split("/")[-1])
            if o.object_type == ObjectType.DIRECTORY and depth < max_depth and not o.path.endswith("/files"):
                tree(o.path, depth + 1, max_depth)
    except Exception as e:
        print("  " * depth + f"(nothing at {path}: {type(e).__name__})")


for root in (f"/Users/{me}/.bundle", "/Shared/.bundle"):
    print("=====", root)
    tree(root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables per environment
# MAGIC The multi-environment bundles write the same tables into a different schema per target
# MAGIC (`orders_dev`, `orders_stage`, `orders`, `orders_cicd_dev`, ...).

# COMMAND ----------

display(spark.sql("""SELECT table_schema, table_name, table_type, created
                     FROM workspace.information_schema.tables
                     WHERE table_schema LIKE 'orders%' ORDER BY table_schema, table_name"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest job runs

# COMMAND ----------

from itertools import islice

# list_runs auto-paginates; `limit` is only the page size, so cap the total with islice.
runs = [(r.run_name, str(r.run_id), r.state.result_state.value if r.state and r.state.result_state else
         (r.state.life_cycle_state.value if r.state else "-"), r.run_page_url) for r in islice(w.jobs.list_runs(limit=15), 15)]
display(spark.createDataFrame(runs, "job STRING, run_id STRING, result STRING, url STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Models registered by the ML lab (4.8)

# COMMAND ----------

# The API requires schema_name whenever catalog_name is given, so list per schema.
found = 0
for sch in w.schemas.list(catalog_name="workspace"):
    if not sch.name.startswith("orders"):
        continue
    try:
        for m in w.registered_models.list(catalog_name="workspace", schema_name=sch.name):
            print(m.full_name)
            found += 1
    except Exception as e:
        print(f"could not list models in workspace.{sch.name}:", str(e)[:150])
if not found:
    print("no registered models yet in workspace.orders* schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - Core loop: `databricks bundle validate` -> `deploy -t <target>` -> `run -t <target> <resource_key>` -> `destroy -t <target>`; `bundle summary` shows deployed resources and URLs.
# MAGIC - `mode: development`: names prefixed `[dev <user>]` plus a `dev` tag, schedules/triggers paused, pipelines `development: true`, concurrent job runs allowed, deployment lock disabled; default root `/Workspace/Users/<user>/.bundle/<bundle>/<target>`.
# MAGIC - `mode: production`: no prefix, schedules kept; validates pipelines are `development: false`, the git branch matches the target's, and (when not using a service principal) that paths are not user-specific and `run_as` + `permissions` are set.
# MAGIC - Variables: declared under top-level `variables`, referenced as `${var.name}`; precedence is `--var` flag > `BUNDLE_VAR_<name>` env var > `.databricks/bundle/<target>/variable-overrides.json` > target `variables` > declared `default`.
# MAGIC - Target settings (workspace, variables, resources, `run_as`, `permissions`) override/merge with the top-level ones; `run_as` sets the job identity, `permissions` are applied to every resource in the bundle.
# MAGIC - `artifacts` (e.g. `type: whl` with a `build` command) are built and uploaded on `deploy` and referenced by tasks as libraries.
# MAGIC - Deployment state lives in the workspace under `<root_path>/state`, so any machine (CI included) deploying the same bundle+target updates the same resources; a deployment lock (on by default outside dev mode) prevents concurrent deploys.
# MAGIC - Bundle-deployed jobs report `deployment.kind = BUNDLE` and show a "managed by a bundle" banner; UI edits are overwritten by the next deploy.
