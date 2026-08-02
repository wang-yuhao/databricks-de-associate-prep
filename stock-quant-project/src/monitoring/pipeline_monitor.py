"""Lightweight observability for the pipeline.

On Databricks, Section 5 of the exam (Monitoring and Alerting) is about system tables,
Query Profiler, Jobs API notifications, and DLT event logs. We don't have those
locally, so this module gives you the same *concepts* in miniature:
  - log_run()   -> appends a row to a `monitoring.pipeline_runs` Delta table every
                   time a pipeline stage completes (rows in/out/quarantined, duration).
                   This is your stand-in for a system table you'd query for observability.
  - maybe_alert()-> the local equivalent of a Databricks SQL Alert: logs (and can be
                   wired to email/Slack) a warning when a data-quality threshold is breached.
"""
import time
from contextlib import contextmanager

from pyspark.sql import functions as F

from src.utils.config import resolve_path


def log_run(spark, cfg, stage, rows_in, rows_out, rows_quarantined, duration_seconds=None):
    monitoring_root = resolve_path(cfg, "delta.warehouse_path")
    table_path = f"{monitoring_root}/monitoring/pipeline_runs"

    row = spark.createDataFrame(
        [
            {
                "stage": stage,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "rows_quarantined": rows_quarantined,
                "duration_seconds": duration_seconds,
            }
        ]
    ).withColumn("run_ts", F.current_timestamp())

    row.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs USING DELTA LOCATION '{table_path}'")


def maybe_alert(cfg, stage, quarantine_rate):
    threshold = cfg["quality"]["quarantine_rate_alert_pct"]
    if quarantine_rate > threshold:
        # Stand-in for a Databricks SQL Alert firing. Wire this to smtplib / a Slack
        # webhook / whatever you use for real notifications -- kept as a print+log here
        # so the project has no hard dependency on your notification stack.
        print(
            f"[ALERT] {stage}: quarantine rate {quarantine_rate:.2f}% exceeds "
            f"threshold {threshold}% -- investigate upstream data quality."
        )
        return True
    return False


@contextmanager
def timed_stage(spark, cfg, stage, rows_in_fn=None):
    """Context manager version -- use when you want duration tracked automatically:

        with timed_stage(spark, cfg, "gold:features") as ctx:
            ... do work ...
            ctx["rows_out"] = out_df.count()
    """
    start = time.time()
    ctx = {"rows_in": 0, "rows_out": 0, "rows_quarantined": 0}
    yield ctx
    duration = time.time() - start
    log_run(
        spark,
        cfg,
        stage=stage,
        rows_in=ctx["rows_in"],
        rows_out=ctx["rows_out"],
        rows_quarantined=ctx["rows_quarantined"],
        duration_seconds=round(duration, 2),
    )
