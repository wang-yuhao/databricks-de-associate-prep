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
# MAGIC `files/` holds the synced source, `state/` the deployment state (`terraform.tfstate` or `resources.json`,
# MAGIC `deployment.json`) that makes `deploy` idempotent.

# COMMAND ----------

from databricks.sdk.service.workspace import ObjectType


def tree(path, depth=0, max_depth=3):
    try:
        for o in sorted(w.workspace.list(path), key=lambda o: o.path):
            print("  " * depth + ("📁 " if o.object_type == ObjectType.DIRECTORY else "   ") + o.path.split("/")[-1])
            if o.object_type == ObjectType.DIRECTORY and depth < max_depth and not o.path.endswith("/files"):
                tree(o.path, depth + 1, max_depth)
    except Exception as e:
        print("  " * depth + f"(nothing at {path}: {type(e).__name__})")


for root in (f"/Workspace/Users/{me}/.bundle", "/Workspace/Shared/.bundle"):
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

runs = [(r.run_name, str(r.run_id), r.state.result_state.value if r.state and r.state.result_state else
         (r.state.life_cycle_state.value if r.state else "-"), r.run_page_url) for r in w.jobs.list_runs(limit=15)]
display(spark.createDataFrame(runs, "job STRING, run_id STRING, result STRING, url STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Models registered by the ML lab (4.8)

# COMMAND ----------

try:
    for m in w.registered_models.list(catalog_name="workspace"):
        print(m.full_name)
except Exception as e:
    print("no registered models yet:", str(e)[:150])
