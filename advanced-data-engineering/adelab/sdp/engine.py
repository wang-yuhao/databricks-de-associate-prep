"""Local Spark Declarative Pipelines engine.

A `Pipeline` plays the role of the Databricks pipeline object (the "Pipelines" UI):

    pipe = DA.pipeline("multi_flow", source="../pipelines/multi_flow",
                       configuration={"source_a": DA.paths.landing + "/a"})
    pipe.validate()                 # "Dry run": load sources, resolve the dataflow graph
    pipe.run()                      # "Start": one triggered update
    pipe.run(full_refresh=True)     # "Full refresh all"
    display(pipe.event_log())       # the pipeline event log
"""
from __future__ import annotations

import contextlib
import json
import shutil
import sys
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.readwriter import DataFrameReader
from pyspark.sql.session import SparkSession as _SessionCls
from pyspark.sql.streaming.readwriter import DataStreamReader

from ..classroom import _local_path
from ..spark import file_uri
from . import api, autoloader, cdc
from .expectations import ExpectationViolation, evaluate, merge_metrics


class PipelineUpdateFailed(Exception):
    pass


class _Deferred(Exception):
    def __init__(self, missing: str):
        super().__init__(missing)
        self.missing = missing


@dataclass
class FlowProgress:
    flow: str
    target: str
    kind: str
    status: str = "QUEUED"
    num_output_rows: int = 0
    data_quality: dict | None = None
    error: str | None = None
    seconds: float = 0.0


@dataclass
class UpdateResult:
    update_id: str
    state: str
    flows: list[FlowProgress] = field(default_factory=list)
    full_refresh: bool = False

    def __repr__(self) -> str:
        return f"UpdateResult(update_id={self.update_id[:8]}, state={self.state}, flows={len(self.flows)})"


EVENT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("sequence", T.LongType()),
    T.StructField("timestamp", T.TimestampType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("level", T.StringType()),
    T.StructField("message", T.StringType()),
    T.StructField("origin", T.StructType([
        T.StructField("pipeline_name", T.StringType()),
        T.StructField("update_id", T.StringType()),
        T.StructField("flow_name", T.StringType()),
        T.StructField("dataset_name", T.StringType()),
    ])),
    T.StructField("details", T.StringType()),
    T.StructField("error", T.StringType()),
])

DQ_DETAILS_SCHEMA = ("struct<flow_progress:struct<status:string,metrics:struct<num_output_rows:long>,"
                     "data_quality:struct<dropped_records:long,expectations:array<struct<name:string,"
                     "dataset:string,action:string,passed_records:long,failed_records:long>>>>>")


def _shape(schema: T.StructType) -> list[tuple[str, str]]:
    return [(f.name.lower(), f.dataType.simpleString()) for f in schema.fields]


# --------------------------------------------------------------------------- source loading helpers


@contextlib.contextmanager
def _pipelines_module_shim():
    """Make `from pyspark import pipelines as dp` and `import dlt` resolve to the local API."""
    import pyspark

    saved_mod = {k: sys.modules.get(k) for k in ("pyspark.pipelines", "dlt")}
    saved_attr = getattr(pyspark, "pipelines", None)
    sys.modules["pyspark.pipelines"] = api
    sys.modules["dlt"] = api
    pyspark.pipelines = api
    try:
        yield
    finally:
        for k, v in saved_mod.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
        if saved_attr is None:
            with contextlib.suppress(AttributeError):
                del pyspark.pipelines
        else:
            pyspark.pipelines = saved_attr


@contextlib.contextmanager
def _record_reads(sink: set[str]):
    """Record every table name a query function reads, to infer the dataflow graph."""
    originals = (DataFrameReader.table, DataStreamReader.table, _SessionCls.table)

    def wrap(fn):
        def inner(self, name, *a, **kw):
            sink.add(name)
            return fn(self, name, *a, **kw)
        return inner

    DataFrameReader.table, DataStreamReader.table, _SessionCls.table = (wrap(f) for f in originals)
    try:
        yield
    finally:
        DataFrameReader.table, DataStreamReader.table, _SessionCls.table = originals


@contextlib.contextmanager
def _spark_conf(spark: SparkSession, conf: dict[str, str]):
    previous = {}
    for k, v in conf.items():
        previous[k] = spark.conf.get(k, None)
        spark.conf.set(k, str(v))
    try:
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                spark.conf.unset(k)
            else:
                spark.conf.set(k, v)


# --------------------------------------------------------------------------- pipeline


class Pipeline:
    def __init__(self, spark: SparkSession, *, name: str, source, schema: str, storage,
                 configuration: dict | None = None):
        self.spark = spark
        self.name = name
        self.sources = [Path(s).resolve() for s in (source if isinstance(source, (list, tuple)) else [source])]
        self.schema = schema
        self.storage = Path(storage).resolve()
        self.configuration = {k: str(v) for k, v in (configuration or {}).items()}
        self.registry: api.Registry | None = None
        self.last_update: UpdateResult | None = None
        self._events: list[dict] = []
        self._seq = 0

    # ------------------------------------------------------------------ paths & names
    @property
    def event_log_path(self) -> str:
        return file_uri(self.storage / "system" / "events")

    def _checkpoint(self, flow: api.Flow) -> str:
        return str(self.storage / "checkpoints" / flow.target.lower() / flow.name.lower())

    def _txn_app_id(self, flow: api.Flow) -> str:
        """Idempotent-write id for a flow. A new id per checkpoint, because a reset checkpoint restarts
        batch ids at 0 and Delta would skip them as already committed (relevant for sinks, whose data
        survives a full refresh)."""
        f = Path(self._checkpoint(flow) + ".txn_app_id")
        if not f.exists():
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(f"{self.name}.{flow.name}.{uuid.uuid4().hex[:8]}")
        return f.read_text().strip()

    def _cdc_state(self, flow: api.Flow) -> Path:
        return self.storage / "autocdc_state" / flow.target.lower()

    def fqn(self, name: str) -> str:
        return f"{self.schema}.{name}"

    def _norm(self, name: str) -> str:
        parts = [p.strip("`").lower() for p in name.split(".")]
        if len(parts) >= 2 and parts[-2] == self.schema.lower():
            return parts[-1]
        return ".".join(parts)

    def _session_conf(self) -> dict[str, str]:
        return {**self.configuration, "adelab.autoloader.schemaRoot": str(self.storage / "autoloader")}

    def _source_files(self) -> list[Path]:
        files = []
        for s in self.sources:
            if s.is_file():
                files.append(s)
            elif s.is_dir():
                files += sorted(p for p in s.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
            else:
                raise FileNotFoundError(f"Pipeline source not found: {s}")
        return files

    # ------------------------------------------------------------------ load
    def _load(self) -> api.Registry:
        reg = api.Registry()
        api._active = reg
        autoloader.install()
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.spark.sql(f"USE {self.schema}")
        try:
            with _pipelines_module_shim(), _spark_conf(self.spark, self._session_conf()):
                for f in self._source_files():
                    if f.suffix == ".py":
                        code = compile(f.read_text(encoding="utf-8"), str(f), "exec")
                        exec(code, {"spark": self.spark, "__file__": str(f), "__name__": f"pipeline.{f.stem}"})
                    elif f.suffix == ".sql":
                        print(f"[adelab] skipping {f.name}: SQL pipeline sources run on Databricks only "
                              "(see the databricks/ folder of the course).")
        finally:
            api._active = None
        for flow in reg.flows.values():
            if flow.target.lower() not in reg.datasets:
                raise ValueError(f"Flow '{flow.name}' targets '{flow.target}', which is not defined in the "
                                 "pipeline. Declare it with dp.create_streaming_table() or dp.create_sink().")
            if flow.kind == "auto_cdc" and reg.datasets[flow.target.lower()].kind != "streaming_table":
                raise ValueError(f"AUTO CDC flow '{flow.name}' must target a streaming table declared with "
                                 "dp.create_streaming_table() (sinks accept append flows only).")
        self.registry = reg
        return reg

    # ------------------------------------------------------------------ graph
    def _discover(self, reg: api.Registry) -> dict[str, set[str]]:
        """Call each query function once (lazily) and record which datasets it reads."""
        deps: dict[str, set[str]] = {}
        with _pipelines_module_shim(), _spark_conf(self.spark, self._session_conf()):
            pending = list(reg.flows.values())
            for _ in range(len(pending) + 1):
                progressed, still = False, []
                for flow in pending:
                    names: set[str] = set()
                    if flow.kind == "auto_cdc":
                        names.add(flow.cdc["source"])
                        deps[flow.name.lower()] = {self._norm(n) for n in names}
                        progressed = True
                        continue
                    try:
                        with _record_reads(names), _spark_conf(self.spark, flow.spark_conf):
                            df = flow.func()
                        if reg.datasets[flow.target.lower()].kind == "temporary_view" and df is not None:
                            df.createOrReplaceTempView(flow.target)
                        deps[flow.name.lower()] = {self._norm(n) for n in names}
                        progressed = True
                    except AnalysisException:
                        deps[flow.name.lower()] = {self._norm(n) for n in names}
                        still.append(flow)
                pending = still
                if not pending or not progressed:
                    break
        for flow in reg.flows.values():
            deps.setdefault(flow.name.lower(), set())
        return deps

    def _order(self, reg: api.Registry, deps: dict[str, set[str]]) -> list[api.Flow]:
        producers: dict[str, list[str]] = {}
        for f in reg.flows.values():
            producers.setdefault(f.target.lower(), []).append(f.name.lower())
        order, done, remaining = [], set(), list(reg.flows.values())
        while remaining:
            ready = [f for f in remaining
                     if all(p in done or p == f.name.lower()
                            for d in deps[f.name.lower()] if d in reg.datasets
                            for p in producers.get(d, []))]
            if not ready:
                cyc = ", ".join(f.name for f in remaining)
                raise ValueError(f"Cycle detected in the dataflow graph between flows: {cyc}")
            for f in ready:
                order.append(f)
                done.add(f.name.lower())
                remaining.remove(f)
        return order

    def validate(self, show: bool = True) -> list[api.Flow]:
        """Dry run: load the source files, resolve the graph and print it. Writes nothing."""
        reg = self._load()
        deps = self._discover(reg)
        order = self._order(reg, deps)
        if show:
            self._print_graph(reg, deps, order)
        return order

    def graph(self) -> None:
        self.validate(show=True)

    def _print_graph(self, reg, deps, order) -> None:
        kinds = {"table": "table", "streaming_table": "streaming table", "materialized_view": "materialized view",
                 "temporary_view": "temporary view", "sink": "sink"}
        print(f"Pipeline '{self.name}'  ->  schema `{self.schema}`")
        print("Datasets:")
        for ds in reg.datasets.values():
            extra = []
            if ds.cluster_by:
                extra.append(f"CLUSTER BY ({', '.join(ds.cluster_by)})")
            if ds.expectations:
                extra.append(f"{len(ds.expectations)} expectation(s)")
            print(f"  - {ds.name:<28} {kinds[ds.kind]:<18} {'  '.join(extra)}")
        print("Flows (execution order):")
        for f in order:
            src = sorted(d for d in deps[f.name.lower()])
            print(f"  {f.name:<28} {f.kind:<8} {', '.join(src) or '(external source)':<40} -> {f.target}")

    # ------------------------------------------------------------------ events
    def _event(self, event_type: str, message: str, update_id: str, level: str = "INFO",
               flow: str | None = None, dataset: str | None = None, details: dict | None = None,
               error: str | None = None) -> None:
        self._seq += 1
        self._events.append({
            "id": str(uuid.uuid4()), "sequence": self._seq, "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
            "event_type": event_type, "level": level, "message": message,
            "origin": (self.name, update_id, flow, dataset),
            "details": json.dumps(details) if details is not None else None, "error": error})

    def _flush_events(self) -> None:
        if not self._events:
            return
        rows = [(e["id"], e["sequence"], e["timestamp"], e["event_type"], e["level"], e["message"],
                 e["origin"], e["details"], e["error"]) for e in self._events]
        self.spark.createDataFrame(rows, EVENT_SCHEMA).write.format("delta").mode("append").save(self.event_log_path)
        self._events = []

    def event_log(self) -> DataFrame:
        """The pipeline event log (same columns as Databricks' `event_log()` table-valued function)."""
        if not DeltaTable.isDeltaTable(self.spark, self.event_log_path):
            return self.spark.createDataFrame([], EVENT_SCHEMA)
        return self.spark.read.format("delta").load(self.event_log_path).orderBy("timestamp", "sequence")

    def expectation_metrics(self, latest_only: bool = True) -> DataFrame:
        """Data quality metrics per expectation, parsed from `flow_progress` events."""
        log = self.event_log().filter("event_type = 'flow_progress'")
        if latest_only and self.last_update is not None:
            log = log.filter(F.col("origin.update_id") == self.last_update.update_id)
        parsed = log.select("origin.update_id", "origin.flow_name",
                            F.from_json("details", DQ_DETAILS_SCHEMA).alias("d"))
        return (parsed.select("update_id", "flow_name",
                              F.explode("d.flow_progress.data_quality.expectations").alias("e"))
                .select("update_id", "flow_name", "e.dataset", F.col("e.name").alias("expectation"),
                        "e.action", "e.passed_records", "e.failed_records")
                .orderBy("dataset", "expectation"))

    # ------------------------------------------------------------------ table management
    def _table_exists(self, name: str) -> bool:
        return self.spark.catalog.tableExists(self.fqn(name))

    def _drop_dataset(self, ds: api.Dataset) -> None:
        if ds.kind == "temporary_view":
            return
        if ds.kind == "sink":
            # Databricks: a full refresh resets the sink's flows but never deletes data already in the sink,
            # so the reprocessed records are appended again.
            shutil.rmtree(self.storage / "checkpoints" / ds.name.lower(), ignore_errors=True)
            return
        t = self.fqn(ds.name)
        if self._table_exists(ds.name):
            loc = self.spark.sql(f"DESCRIBE DETAIL {t}").first()["location"]
            self.spark.sql(f"DROP TABLE IF EXISTS {t}")
            path = _local_path(loc) if loc else None
            if path is not None and path.exists():
                shutil.rmtree(path, ignore_errors=True)
        shutil.rmtree(self.storage / "checkpoints" / ds.name.lower(), ignore_errors=True)
        shutil.rmtree(self.storage / "autocdc_state" / ds.name.lower(), ignore_errors=True)

    def _sync_clustering(self, ds: api.Dataset) -> None:
        """Changing cluster_by in the code re-clusters the existing table (no full refresh needed)."""
        if ds.cluster_by is None or not self._table_exists(ds.name):
            return
        current = list(self.spark.sql(f"DESCRIBE DETAIL {self.fqn(ds.name)}").first()["clusteringColumns"] or [])
        if [c.lower() for c in current] != [c.lower() for c in ds.cluster_by]:
            self.spark.sql(f"ALTER TABLE {self.fqn(ds.name)} CLUSTER BY ({', '.join(ds.cluster_by)})")
            print(f"[adelab] {ds.name}: clustering keys changed {current} -> {ds.cluster_by}")

    def _create_table(self, ds: api.Dataset, schema: T.StructType) -> None:
        if self._table_exists(ds.name):
            self._sync_clustering(ds)
            return
        if ds.cluster_by_auto and not ds.cluster_by:
            print(f"[adelab] {ds.name}: cluster_by_auto (automatic Liquid Clustering) is Databricks-only; "
                  "creating the table without clustering keys.")
        if ds.schema is not None:
            schema = ds.schema if isinstance(ds.schema, T.StructType) else T._parse_datatype_string(ds.schema)
        b = DeltaTable.createIfNotExists(self.spark).tableName(self.fqn(ds.name)).addColumns(schema)
        if ds.comment:
            b = b.comment(ds.comment)
        if ds.cluster_by:
            b = b.clusterBy(*ds.cluster_by)
        elif ds.partition_cols:
            b = b.partitionedBy(*ds.partition_cols)
        for k, v in ds.table_properties.items():
            b = b.property(k, str(v))
        b.execute()

    # ------------------------------------------------------------------ flow execution
    def _resolve(self, flow: api.Flow, produced: set[str]) -> DataFrame:
        names: set[str] = set()
        try:
            with _record_reads(names):
                return flow.func()
        except AnalysisException as e:
            params = e.getMessageParameters() or {}
            missing = self._norm(params.get("relationName", "").strip("`")) if params else ""
            if missing and missing in self.registry.datasets and missing not in produced:
                raise _Deferred(missing) from e
            raise

    def _write_stream(self, flow: api.Flow, df: DataFrame, handler, prog: FlowProgress) -> None:
        errors: list[BaseException] = []

        def batch(bdf: DataFrame, batch_id: int) -> None:
            try:
                handler(bdf, batch_id)
            except BaseException as exc:  # keep the original exception, Spark wraps it otherwise
                errors.append(exc)
                raise

        writer = (df.writeStream.foreachBatch(batch)
                  .option("checkpointLocation", self._checkpoint(flow))
                  .queryName(f"{self.name}.{flow.name}"))
        writer = writer.trigger(availableNow=True)
        try:
            writer.start().awaitTermination()
        except Exception:
            if errors:
                raise errors[0] from None
            raise

    def _run_flow(self, flow: api.Flow, ds: api.Dataset, prog: FlowProgress, produced: set[str],
                  update_id: str) -> None:
        reg = self.registry
        target = self.fqn(ds.name)
        exps = [] if ds.kind == "sink" else ds.expectations

        if flow.kind == "auto_cdc":
            src = flow.cdc["source"]
            src_df = (self.spark.readStream.table(src) if not flow.once else self.spark.read.table(src))
            if not self._table_exists(ds.name):
                self._create_table(ds, cdc.target_schema(src_df, flow.cdc))

            def handler(bdf, batch_id):
                clean, m, release = evaluate(bdf, exps, flow.name, ds.name)
                if flow.cdc["scd_type"] == 1:
                    prog.num_output_rows += cdc.apply_scd1(self.spark, clean, target, self._cdc_state(flow), flow.cdc)
                else:
                    prog.num_output_rows += cdc.apply_scd2(self.spark, clean, target, flow.cdc)
                release()
                prog.data_quality = merge_metrics(prog.data_quality, m) if exps else None

            if src_df.isStreaming:
                self._write_stream(flow, src_df, handler, prog)
            else:
                handler(src_df, 0)
            return

        df = self._resolve(flow, produced)
        if df is None:
            raise ValueError(f"Query function for flow '{flow.name}' returned None; it must return a DataFrame.")

        if ds.kind == "temporary_view":
            if exps:
                df = self._apply_streaming_safe_expectations(df, exps, flow, ds, prog)
            df.createOrReplaceTempView(ds.name)
            prog.status = "COMPLETED"
            return

        is_mv = ds.kind == "materialized_view" or (ds.kind == "table" and not df.isStreaming)
        if is_mv:
            if df.isStreaming:
                raise ValueError(f"Materialized view '{ds.name}' must be defined by a batch query.")
            clean, m, release = evaluate(df, exps, flow.name, ds.name)
            clean = clean.localCheckpoint(eager=True)  # recompute fully before replacing the table
            release()
            if self._table_exists(ds.name) and _shape(self.spark.table(target).schema) != _shape(clean.schema):
                self._drop_dataset(ds)  # query changed shape: recreate, keeping cluster keys and properties
            if not self._table_exists(ds.name):
                self._create_table(ds, clean.schema)
            clean.write.format("delta").mode("overwrite").saveAsTable(target)
            prog.num_output_rows = clean.count()
            prog.data_quality = m if exps else None
            return

        # streaming table or sink
        sink_path = sink_table = None
        if ds.kind == "sink":
            sink_path = ds.sink_options.get("path")
            sink_table = ds.sink_options.get("tableName")
            if not (sink_path or sink_table):
                raise ValueError(f"Sink '{ds.name}' needs a 'path' or 'tableName' option.")
        app_id = self._txn_app_id(flow)

        def handler(bdf, batch_id):
            clean, m, release = evaluate(bdf, exps, flow.name, ds.name)
            n = clean.count()
            w = (clean.write.format("delta").mode("append").option("mergeSchema", "true")
                 .option("txnAppId", app_id).option("txnVersion", batch_id))
            if ds.kind == "sink":
                w.save(file_uri(sink_path)) if sink_path else w.saveAsTable(sink_table)
            else:
                if not self._table_exists(ds.name):
                    self._create_table(ds, clean.schema)
                w.saveAsTable(target)
            release()
            prog.num_output_rows += n
            prog.data_quality = merge_metrics(prog.data_quality, m) if exps else None

        if ds.kind != "sink":
            self._sync_clustering(ds)
        if df.isStreaming:
            self._write_stream(flow, df, handler, prog)
        else:
            if not flow.once:
                raise ValueError(f"Flow '{flow.name}' reads a batch source into streaming table '{ds.name}'. "
                                 "Use a streaming read, or @dp.append_flow(once=True) for a one-time backfill.")
            marker = Path(self._checkpoint(flow)) / "_once_done"
            if marker.exists():
                prog.status = "SKIPPED (once flow already ran)"
                return
            handler(df, 0)
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(update_id)

    def _apply_streaming_safe_expectations(self, df, exps, flow, ds, prog):
        """Views are not materialized: apply drop/fail rules row-wise (no metrics are recorded)."""
        for e in exps:
            ok = F.coalesce(F.expr(e.constraint), F.lit(False))
            if e.action == "drop":
                df = df.filter(ok)
            elif e.action == "fail":
                df = df.withColumn("__chk", F.when(~ok, F.raise_error(F.lit(
                    f"Expectation '{e.name}' violated on view '{ds.name}': {e.constraint}")))).drop("__chk")
        return df

    # ------------------------------------------------------------------ run
    def run(self, full_refresh: bool = False, full_refresh_selection: list[str] | None = None,
            refresh_selection: list[str] | None = None, raise_on_failure: bool = True,
            quiet: bool = False) -> UpdateResult:
        """Run one triggered pipeline update (the "Start" button)."""
        update_id = str(uuid.uuid4())
        reg = self._load()
        selected_full = {n.lower() for n in (full_refresh_selection or [])}
        result = UpdateResult(update_id=update_id, state="RUNNING", full_refresh=full_refresh)
        self._event("create_update", f"Update {update_id[:8]} started"
                    + (" (full refresh)" if full_refresh else ""), update_id,
                    details={"create_update": {"full_refresh": full_refresh,
                                               "full_refresh_selection": sorted(selected_full),
                                               "refresh_selection": refresh_selection or []}})

        if full_refresh or selected_full:
            for ds in reg.datasets.values():
                if full_refresh or ds.name.lower() in selected_full:
                    self._drop_dataset(ds)

        t0 = time.time()
        failed_datasets: set[str] = set()
        try:
            with _pipelines_module_shim(), _spark_conf(self.spark, self._session_conf()):
                deps = self._discover(reg)
                order = self._order(reg, deps)
                selection = {n.lower() for n in refresh_selection} if refresh_selection else None
                produced: set[str] = set()
                queue = list(order)
                progs = {f.name.lower(): FlowProgress(f.name, f.target, f.kind) for f in order}
                result.flows = []
                deferrals = 0
                while queue:
                    flow = queue.pop(0)
                    prog = progs[flow.name.lower()]
                    ds = reg.datasets[flow.target.lower()]
                    if prog not in result.flows:
                        result.flows.append(prog)
                    upstream_failed = [d for d in deps[flow.name.lower()] if d in failed_datasets]
                    if upstream_failed:
                        prog.status = "SKIPPED"
                        failed_datasets.add(ds.name.lower())
                        self._event("flow_progress", f"Flow '{flow.name}' is SKIPPED (upstream failed)",
                                    update_id, "WARN", flow.name, ds.name,
                                    {"flow_progress": {"status": "SKIPPED"}})
                        continue
                    if selection is not None and ds.name.lower() not in selection and ds.kind != "temporary_view":
                        prog.status = "EXCLUDED"
                        continue
                    self._event("flow_progress", f"Flow '{flow.name}' is RUNNING", update_id,
                                flow=flow.name, dataset=ds.name, details={"flow_progress": {"status": "RUNNING"}})
                    start = time.time()
                    try:
                        with _spark_conf(self.spark, flow.spark_conf):
                            self._run_flow(flow, ds, prog, produced, update_id)
                    except _Deferred as d:
                        result.flows.remove(prog)
                        deferrals += 1
                        if deferrals > len(order) * 2:
                            raise ValueError(f"Could not resolve dependency '{d.missing}' of '{flow.name}'")
                        queue.insert(next((i + 1 for i, q in enumerate(queue)
                                           if q.target.lower() == d.missing), len(queue)), flow)
                        continue
                    except Exception as exc:  # noqa: BLE001 - recorded in the event log, re-raised below
                        prog.status = "FAILED"
                        prog.error = f"{type(exc).__name__}: {exc}"
                        prog.seconds = time.time() - start
                        failed_datasets.add(ds.name.lower())
                        self._event("flow_progress", f"Flow '{flow.name}' has FAILED", update_id, "ERROR",
                                    flow.name, ds.name, {"flow_progress": {"status": "FAILED",
                                                                           "data_quality": prog.data_quality}},
                                    error="".join(traceback.format_exception_only(type(exc), exc)).strip())
                        continue
                    prog.seconds = time.time() - start
                    if prog.status in ("QUEUED", "RUNNING"):
                        prog.status = "COMPLETED"
                    produced.add(ds.name.lower())
                    self._event("flow_progress", f"Flow '{flow.name}' has {prog.status}", update_id,
                                flow=flow.name, dataset=ds.name,
                                details={"flow_progress": {"status": prog.status,
                                                           "metrics": {"num_output_rows": prog.num_output_rows},
                                                           "data_quality": prog.data_quality}})
        except Exception as exc:
            result.state = "FAILED"
            self._event("update_progress", f"Update {update_id[:8]} is FAILED", update_id, "ERROR",
                        details={"update_progress": {"state": "FAILED"}}, error=str(exc))
            self._flush_events()
            self.last_update = result
            raise

        result.state = "FAILED" if any(p.status == "FAILED" for p in result.flows) else "COMPLETED"
        self._event("update_progress", f"Update {update_id[:8]} is {result.state}", update_id,
                    "ERROR" if result.state == "FAILED" else "INFO",
                    details={"update_progress": {"state": result.state}})
        self._flush_events()
        self.last_update = result
        if not quiet:
            self._print_summary(result, time.time() - t0)
        if result.state == "FAILED" and raise_on_failure:
            errs = "\n".join(f"  - {p.flow}: {p.error}" for p in result.flows if p.status == "FAILED")
            raise PipelineUpdateFailed(f"Pipeline update {update_id[:8]} FAILED:\n{errs}")
        return result

    def _print_summary(self, result: UpdateResult, seconds: float) -> None:
        print(f"Update {result.update_id[:8]} {result.state} in {seconds:.1f}s"
              + ("  (full refresh)" if result.full_refresh else ""))
        print(f"  {'flow':<28} {'target':<26} {'status':<10} {'rows':>7}  {'dropped':>7}  {'warned':>7}")
        for p in result.flows:
            dq = p.data_quality or {}
            warned = sum(e["failed_records"] for e in dq.get("expectations", []) if e["action"] == "warn")
            dropped = dq.get("dropped_records", 0) if dq else 0
            print(f"  {p.flow:<28} {p.target:<26} {p.status:<10} {p.num_output_rows:>7}  "
                  f"{(dropped if dq else '-'):>7}  {(warned if dq else '-'):>7}")
            if p.error:
                print(f"      ! {p.error[:400]}")

    # ------------------------------------------------------------------ maintenance
    def maintenance(self) -> None:
        """What Databricks runs automatically for pipeline tables: OPTIMIZE (clusters LC tables)."""
        reg = self.registry or self._load()
        for ds in reg.datasets.values():
            if ds.kind in ("temporary_view", "sink") or not self._table_exists(ds.name):
                continue
            self.spark.sql(f"OPTIMIZE {self.fqn(ds.name)}")
            print(f"OPTIMIZE {self.fqn(ds.name)}")
