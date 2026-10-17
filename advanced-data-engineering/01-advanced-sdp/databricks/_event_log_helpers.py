# Databricks notebook source
# MAGIC %md
# MAGIC # Event log helpers (included with `%run ./_event_log_helpers`)
# MAGIC Every pipeline of this course publishes its event log to `<schema>.event_log`
# MAGIC (*Pipeline settings → Advanced → Event logs → Publish event log to metastore*).
# MAGIC Without publishing, use `event_log('<pipeline-id>')` instead of the table name.


# COMMAND ----------

def latest_update_id(event_log):
    return spark.sql(f"""SELECT origin.update_id FROM {event_log} WHERE event_type = 'create_update'
                         ORDER BY timestamp DESC LIMIT 1""").first()[0]


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


print("helpers: latest_update_id, updates, flow_rows, expectation_metrics, errors  (arg: event log table)")
