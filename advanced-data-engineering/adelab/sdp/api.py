"""Databricks-compatible `pyspark.pipelines` API for the local engine.

Pipeline source files keep the Databricks import unchanged:

    from pyspark import pipelines as dp

While the local engine loads a pipeline, that import resolves to this module (as does the legacy
`import dlt`), so the same file runs here and in a Databricks pipeline.
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Callable

# --------------------------------------------------------------------------- model


@dataclass
class Expectation:
    name: str
    constraint: str
    action: str  # "warn" | "drop" | "fail"


@dataclass
class Dataset:
    name: str
    kind: str  # "table" (streaming table or MV, decided at run time) | "streaming_table" | "materialized_view" | "temporary_view" | "sink"
    comment: str | None = None
    table_properties: dict[str, str] = field(default_factory=dict)
    partition_cols: list[str] | None = None
    cluster_by: list[str] | None = None
    cluster_by_auto: bool = False
    schema: Any = None
    private: bool = False
    expectations: list[Expectation] = field(default_factory=list)
    sink_format: str | None = None
    sink_options: dict[str, str] = field(default_factory=dict)
    defined_at: str = ""


@dataclass
class Flow:
    name: str
    target: str
    func: Callable[[], Any] | None
    kind: str  # "query" (dataset's own query) | "append" | "auto_cdc"
    once: bool = False
    spark_conf: dict[str, str] = field(default_factory=dict)
    cdc: dict[str, Any] = field(default_factory=dict)
    defined_at: str = ""


class Registry:
    def __init__(self) -> None:
        self.datasets: dict[str, Dataset] = {}
        self.flows: dict[str, Flow] = {}

    def add_dataset(self, ds: Dataset) -> None:
        key = ds.name.lower()
        if key in self.datasets:
            raise ValueError(f"Dataset '{ds.name}' is defined more than once "
                             f"({self.datasets[key].defined_at} and {ds.defined_at}).")
        self.datasets[key] = ds

    def add_flow(self, flow: Flow) -> None:
        key = flow.name.lower()
        if key in self.flows:
            raise ValueError(f"Flow '{flow.name}' is defined more than once.")
        self.flows[key] = flow


_active: Registry | None = None


def _registry() -> Registry:
    if _active is None:
        raise RuntimeError("Pipeline definitions can only be evaluated by a pipeline update. "
                           "Run them with DA.pipeline(...).run() instead of executing the file directly.")
    return _active


def _where(depth: int = 3) -> str:
    frame = inspect.stack()[depth]
    return f"{frame.filename}:{frame.lineno}"


def _norm_list(v) -> list[str] | None:
    if v is None:
        return None
    return [v] if isinstance(v, str) else list(v)


def _pending_expectations(func) -> list[Expectation]:
    return list(getattr(func, "_adelab_expectations", []))


# --------------------------------------------------------------------------- datasets


def _dataset_decorator(kind: str, query_function=None, *, name=None, comment=None, spark_conf=None,
                       table_properties=None, path=None, partition_cols=None, cluster_by_auto=None,
                       cluster_by=None, schema=None, row_filter=None, private=None, **unsupported):
    if unsupported:
        print(f"[adelab] ignoring unsupported arguments for local run: {sorted(unsupported)}")
    if path is not None:
        print("[adelab] 'path' is ignored locally; tables are created in the pipeline schema.")
    if row_filter is not None:
        print("[adelab] 'row_filter' is a Unity Catalog feature; ignored locally (see Course 2).")

    def outer(func):
        ds = Dataset(
            name=name or func.__name__, kind=kind, comment=comment,
            table_properties=dict(table_properties or {}), partition_cols=_norm_list(partition_cols),
            cluster_by=_norm_list(cluster_by), cluster_by_auto=bool(cluster_by_auto), schema=schema,
            private=bool(private), expectations=_pending_expectations(func), defined_at=_where(2))
        reg = _registry()
        reg.add_dataset(ds)
        reg.add_flow(Flow(name=ds.name, target=ds.name, func=func, kind="query",
                          spark_conf=dict(spark_conf or {}), defined_at=ds.defined_at))
        func._adelab_dataset = ds
        return func

    if callable(query_function):
        return outer(query_function)
    return outer


def table(query_function=None, **kwargs):
    """Streaming table (query returns a streaming DataFrame) or materialized view (batch DataFrame)."""
    return _dataset_decorator("table", query_function, **kwargs)


def materialized_view(query_function=None, **kwargs):
    return _dataset_decorator("materialized_view", query_function, **kwargs)


def temporary_view(query_function=None, *, name=None, comment=None, spark_conf=None, **unsupported):
    return _dataset_decorator("temporary_view", query_function, name=name, comment=comment,
                              spark_conf=spark_conf, **unsupported)


view = temporary_view  # legacy DLT name


def create_streaming_table(name, *, comment=None, spark_conf=None, table_properties=None, path=None,
                           partition_cols=None, cluster_by_auto=None, cluster_by=None, schema=None,
                           expect_all=None, expect_all_or_drop=None, expect_all_or_fail=None,
                           row_filter=None, private=None):
    """Declare a streaming table that is fed by append flows or an AUTO CDC flow."""
    exps = ([Expectation(k, v, "warn") for k, v in (expect_all or {}).items()]
            + [Expectation(k, v, "drop") for k, v in (expect_all_or_drop or {}).items()]
            + [Expectation(k, v, "fail") for k, v in (expect_all_or_fail or {}).items()])
    if row_filter is not None:
        print("[adelab] 'row_filter' is a Unity Catalog feature; ignored locally (see Course 2).")
    _registry().add_dataset(Dataset(
        name=name, kind="streaming_table", comment=comment, table_properties=dict(table_properties or {}),
        partition_cols=_norm_list(partition_cols), cluster_by=_norm_list(cluster_by),
        cluster_by_auto=bool(cluster_by_auto), schema=schema, private=bool(private),
        expectations=exps, defined_at=_where(2)))


create_target_table = create_streaming_table  # legacy DLT name


def create_sink(name, format, options=None):  # noqa: A002 - matches the Databricks signature
    """External sink (Delta path or table) that streaming append flows can write to."""
    if format != "delta":
        raise NotImplementedError(f"Local engine supports only Delta sinks (got format='{format}').")
    _registry().add_dataset(Dataset(name=name, kind="sink", sink_format=format,
                                    sink_options=dict(options or {}), defined_at=_where(2)))


# --------------------------------------------------------------------------- flows


def append_flow(*, target, name=None, spark_conf=None, once=False, comment=None):
    """Additional flow that appends into an existing streaming table or sink."""
    def outer(func):
        if _pending_expectations(func):
            raise ValueError(_APPEND_FLOW_EXPECTATIONS.format(name or func.__name__))
        _registry().add_flow(Flow(name=name or func.__name__, target=target, func=func, kind="append",
                                  once=bool(once), spark_conf=dict(spark_conf or {}), defined_at=_where(2)))
        func._adelab_flow = name or func.__name__
        return func
    return outer


_APPEND_FLOW_EXPECTATIONS = ("Append flow '{}': expectations cannot be defined on @dp.append_flow (Databricks "
                             "rejects them too). Define them on the target: dp.create_streaming_table(..., "
                             "expect_all={{...}}, expect_all_or_drop={{...}}, expect_all_or_fail={{...}}).")


def create_auto_cdc_flow(target, source, keys, sequence_by, ignore_null_updates=False,
                         apply_as_deletes=None, apply_as_truncates=None, column_list=None,
                         except_column_list=None, stored_as_scd_type=1, track_history_column_list=None,
                         track_history_except_column_list=None, name=None, once=False):
    """AUTO CDC (formerly APPLY CHANGES): apply a CDC feed to a streaming table as SCD type 1 or 2."""
    if column_list is not None and except_column_list is not None:
        raise ValueError("Only one of column_list and except_column_list can be specified.")
    if track_history_column_list is not None and track_history_except_column_list is not None:
        raise ValueError("Only one of track_history_column_list and track_history_except_column_list "
                         "can be specified.")
    scd = int(stored_as_scd_type)
    if scd not in (1, 2):
        raise ValueError("stored_as_scd_type must be 1 or 2")
    if apply_as_truncates is not None and scd == 2:
        raise ValueError("apply_as_truncates is only supported for SCD type 1.")
    _registry().add_flow(Flow(
        name=name or target, target=target, func=None, kind="auto_cdc", once=bool(once),
        defined_at=_where(2),
        cdc=dict(source=source, keys=_norm_list(keys), sequence_by=sequence_by,
                 ignore_null_updates=ignore_null_updates, apply_as_deletes=apply_as_deletes,
                 apply_as_truncates=apply_as_truncates, column_list=_norm_list(column_list),
                 except_column_list=_norm_list(except_column_list), scd_type=scd,
                 track_history_column_list=_norm_list(track_history_column_list),
                 track_history_except_column_list=_norm_list(track_history_except_column_list))))


apply_changes = create_auto_cdc_flow  # legacy DLT name


# --------------------------------------------------------------------------- expectations


def _attach(func, exps: list[Expectation]):
    ds = getattr(func, "_adelab_dataset", None)
    flow_name = getattr(func, "_adelab_flow", None)
    if ds is not None:               # expectation decorator placed above @dp.table
        ds.expectations.extend(exps)
    elif flow_name is not None:      # placed above @dp.append_flow
        raise ValueError(_APPEND_FLOW_EXPECTATIONS.format(flow_name))
    else:                            # placed below: picked up when the dataset decorator runs
        func._adelab_expectations = exps + list(getattr(func, "_adelab_expectations", []))
    return func


def expect(name, constraint):
    return lambda f: _attach(f, [Expectation(name, constraint, "warn")])


def expect_or_drop(name, constraint):
    return lambda f: _attach(f, [Expectation(name, constraint, "drop")])


def expect_or_fail(name, constraint):
    return lambda f: _attach(f, [Expectation(name, constraint, "fail")])


def expect_all(expectations: dict[str, str]):
    return lambda f: _attach(f, [Expectation(k, v, "warn") for k, v in expectations.items()])


def expect_all_or_drop(expectations: dict[str, str]):
    return lambda f: _attach(f, [Expectation(k, v, "drop") for k, v in expectations.items()])


def expect_all_or_fail(expectations: dict[str, str]):
    return lambda f: _attach(f, [Expectation(k, v, "fail") for k, v in expectations.items()])


# --------------------------------------------------------------------------- legacy readers


def read(name: str):
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession().read.table(name)


def read_stream(name: str):
    from pyspark.sql import SparkSession

    return SparkSession.getActiveSession().readStream.table(name)
