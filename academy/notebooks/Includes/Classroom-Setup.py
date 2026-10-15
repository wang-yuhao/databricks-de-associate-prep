"""
Local stand-in for the Databricks Academy `%run ./Includes/Classroom-Setup` notebook.

Run it at the top of every lesson notebook:

    %run ./Includes/Classroom-Setup.py

It gives you a Databricks-like notebook environment:
    spark          -> a Delta-enabled SparkSession (like the cluster's preconfigured `spark`)
    display(df)    -> renders a DataFrame as an HTML table (like Databricks `display`)
    %%sql          -> cell magic that runs Spark SQL and displays the result (like a %sql cell)
    DA             -> the course helper object: DA.paths, DA.reset(), DA.land_new_files(), ...
"""
import json
import os
import random
import shutil
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace

_PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..")) \
    if os.path.basename(os.getcwd()) == "notebooks" else os.getcwd()
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from IPython import get_ipython                      # noqa: E402
from IPython.display import HTML, display as _ipy_display  # noqa: E402
from pyspark.sql import DataFrame                     # noqa: E402

from spark_session import build_spark                 # noqa: E402

_DEFAULT_ROOT = "/tmp" if os.name != "nt" else os.path.join(_PROJECT_ROOT, "tmp")
_ROOT = os.environ.get("DEMO_ROOT", _DEFAULT_ROOT)
_STATUSES = ["ACTIVE", "INACTIVE", "PENDING", "ERROR"]

spark = build_spark("databricks-academy-lab", warehouse_dir=os.path.join(_ROOT, "warehouse"))


# --------------------------------------------------------------------------- #
# display() and %%sql -- the two things every Databricks notebook relies on
# --------------------------------------------------------------------------- #
def display(obj, limit: int = 1000):
    """Databricks-style display(): DataFrames render as an HTML table (first `limit` rows)."""
    if isinstance(obj, DataFrame):
        pdf = obj.limit(limit).toPandas()
        _ipy_display(HTML(
            f"<div style='max-height:420px;overflow:auto'>{pdf.to_html(index=False)}</div>"
            f"<small>{len(pdf)} row(s){' (truncated)' if len(pdf) == limit else ''}</small>"))
    else:
        _ipy_display(obj)


def _sql_magic(line, cell):
    """%%sql cell magic: runs each ;-separated statement, displays the last result."""
    result = None
    for stmt in [s.strip() for s in cell.split(";") if s.strip()]:
        result = spark.sql(stmt)
    if result is not None and result.columns:
        display(result)


_ip = get_ipython()
if _ip is not None:
    _ip.register_magic_function(_sql_magic, magic_kind="cell", magic_name="sql")


# --------------------------------------------------------------------------- #
# DA helper object (mirrors the course's DBAcademyHelper)
# --------------------------------------------------------------------------- #
class _DBAcademyHelper:
    schema_name = "sdp_demo"
    tables = ["bronze_events", "silver_events", "silver_quarantine", "gold_daily_metrics"]

    def __init__(self, root: str):
        self.paths = SimpleNamespace(
            working_dir=root,
            source_flow_a=os.path.join(root, "source_flow_a"),
            source_flow_b=os.path.join(root, "source_flow_b"),
            lake=os.path.join(root, "lake"),
            checkpoints=os.path.join(root, "checkpoints"),
        )
        self._rng = random.Random(42)

    # ---- data management ----
    def reset(self, files_per_flow: int = 3, rows_per_file: int = 50) -> None:
        """Delete all lesson data and land a fresh, dirty starting dataset."""
        spark.sql("USE default")
        spark.sql(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
        for d in ("source_flow_a", "source_flow_b", "source_flow_c", "lake", "checkpoints"):
            shutil.rmtree(os.path.join(self.paths.working_dir, d), ignore_errors=True)
        self._rng = random.Random(42)
        for flow in ("a", "b"):
            for _ in range(files_per_flow):
                self.land_new_files(flow, rows_per_file, quiet=True)
        print(f"Reset complete. Source files landed in {self.paths.working_dir}")
        self.list_source_files()

    def land_new_files(self, flow: str = "a", rows: int = 50, bad_ratio: float = 0.2,
                       null_event_id: bool = False, quiet: bool = False) -> str:
        """Simulate a new file arriving in a landing zone (like the course's `DA.load(...)`)."""
        src = getattr(self.paths, f"source_flow_{flow}")
        os.makedirs(src, exist_ok=True)
        idx = len([f for f in os.listdir(src) if f.endswith(".json")])
        base, rng, out = datetime(2026, 10, 1), self._rng, []
        for r in range(rows):
            row = {"event_id": f"{flow}-{idx}-{r}",
                   "device_id": f"dev-{rng.randint(1, 20):03d}",
                   "status": rng.choice(_STATUSES),
                   "amount": round(rng.uniform(1, 500), 2),
                   "event_ts": (base + timedelta(days=rng.randint(0, 4),
                                                 minutes=rng.randint(0, 1440))).strftime("%Y-%m-%d %H:%M:%S")}
            roll = rng.random()
            if roll < bad_ratio * 0.40:
                row["event_ts"] = None
            elif roll < bad_ratio * 0.75:
                row["status"] = rng.choice(["UNKNOWN", "", "active!"])
            elif roll < bad_ratio:
                row["amount"] = -row["amount"]
            out.append(row)
        if null_event_id:
            out.append({"event_id": None, "device_id": "dev-999", "status": "ACTIVE",
                        "amount": 1.0, "event_ts": "2026-10-01 00:00:00"})
        path = os.path.join(src, f"part-{idx:03d}.json")
        with open(path, "w") as fh:
            fh.write("\n".join(json.dumps(x) for x in out))
        if not quiet:
            print(f"Landed {len(out)} rows -> {path}")
        return path

    def list_source_files(self) -> None:
        for flow in ("a", "b"):
            src = getattr(self.paths, f"source_flow_{flow}")
            files = sorted(os.listdir(src)) if os.path.isdir(src) else []
            print(f"  source_flow_{flow}: {files}")

    # ---- catalog: make tables queryable by name, like Unity Catalog ----
    def register_tables(self, pipeline) -> None:
        """Register the pipeline's Delta paths as `sdp_demo.<table>` so %%sql can query them."""
        from delta.tables import DeltaTable
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
        paths = {"bronze_events": pipeline.bronze_path, "silver_events": pipeline.silver_path,
                 "silver_quarantine": pipeline.quarantine_path, "gold_daily_metrics": pipeline.gold_path}
        registered = []
        for name, path in paths.items():
            if DeltaTable.isDeltaTable(spark, path):
                spark.sql(f"CREATE TABLE IF NOT EXISTS {self.schema_name}.{name} USING DELTA LOCATION '{path}'")
                registered.append(name)
        spark.sql(f"USE {self.schema_name}")
        print(f"Tables available in schema `{self.schema_name}`: {registered}")

    def event_log(self, pipeline) -> DataFrame:
        """Data-quality metrics as a DataFrame, like querying the SDP event log."""
        if not pipeline.dq.metrics:
            return spark.createDataFrame([], "batch_id long, expectation string, action string, "
                                             "total long, failed long")
        return spark.createDataFrame(pipeline.dq.metrics).select(
            "batch_id", "expectation", "action", "total", "failed")


DA = _DBAcademyHelper(_ROOT)

print("=" * 72)
print("Classroom setup complete")
print(f"  Spark {spark.version}  |  Spark UI: {spark.sparkContext.uiWebUrl}")
print(f"  DA.paths.working_dir = {DA.paths.working_dir}")
print("  Available: spark, display(df), %%sql, DA.reset(), DA.land_new_files(), "
      "DA.register_tables(), DA.event_log()")
print("=" * 72)
