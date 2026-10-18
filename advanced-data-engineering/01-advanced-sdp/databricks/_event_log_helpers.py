# Databricks notebook source
# MAGIC %md
# MAGIC # Event log helpers (included with `%run ./_event_log_helpers`)
# MAGIC Every pipeline of this course publishes its event log to `<schema>.event_log`
# MAGIC (*Pipeline settings → Advanced → Event logs → Publish event log to metastore*).
# MAGIC Without publishing, use `event_log('<pipeline-id>')` instead of the table name.


# COMMAND ----------

def _table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


def locate(expected_schema, marker_table, event_log=None):
    """Find the schema that really holds the pipeline's tables and a usable event log source.

    The pipeline's default catalog/schema decide where its tables land. If they differ from the expected schema
    (e.g. the pipeline was created with schema `default`), look the marker table up in information_schema.
    Returns (schema, event_log) and runs USE <schema>. Never raises, so cells that don't need tables still run."""
    schema = expected_schema
    if not _table_exists(f"{expected_schema}.{marker_table}"):
        found = []
        for info in ("system.information_schema.tables", "workspace.information_schema.tables"):
            try:
                found = [f"{r.table_catalog}.{r.table_schema}" for r in spark.sql(
                    f"SELECT table_catalog, table_schema FROM {info} WHERE table_name = '{marker_table}'").collect()]
                break
            except Exception:
                continue
        if len(found) == 1:
            schema = found[0]
            print(f"NOTE: `{marker_table}` is in {schema}, not {expected_schema}: using {schema}. "
                  f"(Set the pipeline's default schema to {expected_schema.split('.')[-1]}, or the `schema` widget to {schema}.)")
        elif len(found) > 1:
            print(f"`{marker_table}` exists in several schemas: {found}. Set the `schema` widget to the one this pipeline "
                  f"publishes to (pipeline Settings -> Default schema).")
        else:
            print(f"`{marker_table}` not found yet. Run the pipeline first. If it already ran, open its Settings and check "
                  f"Default catalog / schema = {expected_schema.replace('.', ' / ')}.")
    spark.sql(f"USE {schema}")

    if event_log and _table_exists(event_log) and event_log.rsplit(".", 1)[0] == schema:
        return schema, event_log
    if _table_exists(f"{schema}.event_log"):
        return schema, f"{schema}.event_log"
    if _table_exists(f"{schema}.{marker_table}"):
        # Event log not published to a table: read it through the table-valued function instead (owner only).
        print(f"Event log table not found: using event_log(TABLE({schema}.{marker_table})). To publish it, see "
              "pipeline Settings -> Advanced -> Event logs.")
        return schema, f"event_log(TABLE({schema}.{marker_table}))"
    return schema, event_log or f"{schema}.event_log"


def latest_update_id(event_log):
    row = spark.sql(f"""SELECT origin.update_id FROM {event_log} WHERE event_type = 'create_update'
                        ORDER BY timestamp DESC LIMIT 1""").first()
    if row is None:
        raise ValueError(f"No create_update event in {event_log}: run a pipeline update first "
                         "(and check that the event log is published to this table).")
    return row[0]


def updates(event_log):
    """All updates of the pipeline, newest first, with their final state."""
    return spark.sql(f"""
        SELECT origin.update_id, min(timestamp) AS started, max(timestamp) AS ended,
               max_by(details:update_progress.state::STRING, timestamp) AS state
        FROM {event_log}
        WHERE event_type IN ('create_update', 'update_progress')
        GROUP BY 1 ORDER BY started DESC""")


def flow_rows(event_log, update_id=None):
    """Rows written per flow in one update (default: the latest). Cross-check with the pipeline UI."""
    uid = update_id or latest_update_id(event_log)
    return spark.sql(f"""
        SELECT origin.flow_name,
               sum(details:flow_progress.metrics.num_output_rows::BIGINT) AS output_rows,
               sum(details:flow_progress.data_quality.dropped_records::BIGINT) AS dropped_records,
               collect_set(details:flow_progress.status::STRING) AS statuses
        FROM {event_log}
        WHERE event_type = 'flow_progress' AND origin.update_id = '{uid}'
        GROUP BY origin.flow_name ORDER BY 1""")


def expectation_metrics(event_log, update_id=None, all_updates=False):
    """Passed / failed records per expectation (latest update, or every update for a trend)."""
    where = "" if all_updates else f"AND origin.update_id = '{update_id or latest_update_id(event_log)}'"
    return spark.sql(f"""
        SELECT update_id, min(ts) AS update_time, e.dataset, e.name AS expectation,
               sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
        FROM (SELECT origin.update_id, timestamp AS ts,
                     explode(from_json(details:flow_progress.data_quality.expectations,
                     'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
              FROM {event_log}
              WHERE event_type = 'flow_progress' {where})
        GROUP BY update_id, e.dataset, e.name ORDER BY update_time, dataset, expectation""")


def errors(event_log, limit=10):
    return spark.sql(f"""SELECT timestamp, origin.flow_name, message FROM {event_log}
                         WHERE level = 'ERROR' ORDER BY timestamp DESC LIMIT {limit}""")


print("helpers: locate, latest_update_id, updates, flow_rows, expectation_metrics, errors  (arg: event log table)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam notes (Data Engineer Professional)
# MAGIC - Event log columns: `id`, `sequence`, `origin` (struct: `pipeline_id`, `update_id`, `flow_name`, `dataset_name`, ...), `timestamp`, `message`, `level`, `error`, `details`, `event_type`.
# MAGIC - `details` is a JSON **string**: read it with `details:flow_progress.metrics.num_output_rows` and cast with `::BIGINT`; arrays such as `details:flow_progress.data_quality.expectations` need `from_json(..., 'array<struct<name,dataset,passed_records,failed_records>>')` + `explode`.
# MAGIC - Key `event_type`s: `create_update` (one per update; source of the latest `origin.update_id`), `update_progress` (`details:update_progress.state`), `flow_progress` (row counts, data quality, backlog, `status`).
# MAGIC - `passed_records` / `failed_records` come per expectation from `flow_progress` events; `dropped_records` counts rows removed by drop expectations. An `expect_or_fail` violation fails the update and shows up as an `ERROR`-level event (`level`, `message`, `error`).
# MAGIC - Query the log with the `event_log('<pipeline-id>')` table-valued function, or publish it to a Unity Catalog table in the pipeline settings; Databricks recommends sharing it through a view, not the table itself.
