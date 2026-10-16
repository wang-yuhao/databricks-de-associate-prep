"""Course 3 helpers: time queries and read the same metrics the Spark UI shows, via its REST API."""
from __future__ import annotations

import json
import time
import urllib.request
from contextlib import contextmanager

import pandas as pd
from pyspark.sql import DataFrame, SparkSession


def _ui(spark: SparkSession) -> str:
    url = spark.sparkContext.uiWebUrl
    if not url:
        raise RuntimeError("Spark UI is disabled")
    return url.replace("localhost", "127.0.0.1")


def _get(spark: SparkSession, path: str):
    app = spark.sparkContext.applicationId
    with urllib.request.urlopen(f"{_ui(spark)}/api/v1/applications/{app}/{path}", timeout=30) as r:
        return json.loads(r.read())


@contextmanager
def timer(label: str = "elapsed"):
    t = time.perf_counter()
    yield
    print(f"{label}: {time.perf_counter() - t:.2f}s")


def run(df: DataFrame, label: str | None = None, spark: SparkSession | None = None) -> dict:
    """Execute a DataFrame (without collecting it to the driver), then return wall time + plan metrics."""
    spark = spark or df.sparkSession
    tag = f"adelab.perf.run {time.time_ns()}"
    spark.sparkContext.setJobDescription(tag)
    t = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    elapsed = time.perf_counter() - t
    spark.sparkContext.setJobDescription(None)
    m = last_query_metrics(spark, description=tag)
    m["seconds"] = round(elapsed, 2)
    if label:
        m["query"] = label
    return m


_NUM = str.maketrans("", "", ",")


def _value(v: str) -> float:
    """Parse UI metric strings like '1,234', '12.3 MiB', 'total (min, med, max)\\n10.0 MiB (...)'."""
    s = v.split("\n")[-1] if "\n" in v else v
    s = s.split("(")[0].strip().translate(_NUM)
    units = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "ms": 1e-3, "s": 1, "m": 60, "h": 3600}
    parts = s.split()
    try:
        if len(parts) == 2 and parts[1] in units:
            return float(parts[0]) * units[parts[1]]
        return float(parts[0])
    except (ValueError, IndexError):
        return float("nan")


METRICS_OF_INTEREST = {
    "number of files read": "files_read",
    "size of files read": "bytes_read",
    "number of output rows": "rows",
    "shuffle bytes written": "shuffle_bytes_written",
    "shuffle records written": "shuffle_records_written",
    "spill size": "spill_bytes",
    "peak memory": "peak_memory_bytes",
    "number of partitions read": "partitions_read",
}


_WRITE_NODES = ("OverwriteByExpression", "AppendData", "WriteToDataSourceV2")  # the noop write of run()


def last_query_metrics(spark: SparkSession, description: str | None = None) -> dict:
    """Metrics of the most recent user query (what the SQL / DataFrame tab shows for it).

    Delta runs internal queries (log replay, stats) under the same job description; those are skipped
    by picking the execution that actually scans the table's data files.
    """
    done = []
    for _ in range(40):  # the UI store is updated asynchronously by a listener
        execs = _get(spark, "sql?details=true&planDescription=true&offset=0&length=100000")
        done = [e for e in execs if e.get("status") == "COMPLETED"
                and (description is None or e.get("description") == description)
                and not any(n["nodeName"].startswith("Scan ExistingRDD Delta Table State")
                            for n in e.get("nodes", []))]
        if done:
            break
        time.sleep(0.25)
    if not done:
        raise RuntimeError("No completed SQL execution found in the Spark UI")
    e = max(done, key=lambda x: (any(n["nodeName"].startswith(_WRITE_NODES) for n in x["nodes"]), x["id"]))
    out = {"execution_id": e["id"]}
    scans, exchanges = [], 0
    for node in e.get("nodes", []):
        name = node["nodeName"]
        if name.startswith("Exchange"):  # shuffle exchanges only, not BroadcastExchange
            exchanges += 1
        for m in node.get("metrics", []):
            key = METRICS_OF_INTEREST.get(m["name"])
            if key == "rows" and "Join" in name:
                key = "join_output_rows"
            elif key == "rows" and not name.startswith("Scan"):
                continue
            if not key:
                continue
            out[key] = out.get(key, 0) + _value(m["value"])
        if name.startswith("Scan"):
            scans.append(name)
    out["scans"] = len(scans)
    out["shuffle_exchanges"] = exchanges
    plan = e.get("planDescription", "")
    joins = [j for j in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin")
             if j in plan]
    out["join_strategy"] = ",".join(joins) if joins else None
    out["skew_join"] = "skew=true" in plan
    return {k: (int(v) if isinstance(v, float) and v == v and v.is_integer() else v) for k, v in out.items()}


def compare(*results: dict) -> pd.DataFrame:
    """Side-by-side table of several run() results."""
    cols = ["query", "seconds", "files_read", "bytes_read", "rows", "join_output_rows", "shuffle_bytes_written",
            "spill_bytes", "shuffle_exchanges", "join_strategy", "skew_join"]
    df = pd.DataFrame(results)
    df = df[[c for c in cols if c in df.columns]]
    num = df.select_dtypes("number").columns
    df[num] = df[num].fillna(0)
    return df


def stage_summary(spark: SparkSession, last: int = 5) -> pd.DataFrame:
    """Recent stages with task-duration skew (max / median) and spill, like the Stages tab."""
    stages = sorted(_get(spark, "stages?status=complete"), key=lambda s: s["stageId"])[-last:]
    rows = []
    for s in stages:
        q = _get(spark, f"stages/{s['stageId']}/{s['attemptId']}/taskSummary?quantiles=0.5,1.0")
        med, mx = (q.get("executorRunTime") or [0, 0])[:2]
        rows.append({
            "stage": s["stageId"], "name": s["name"][:40], "tasks": s["numTasks"],
            "median_task_ms": med, "max_task_ms": mx, "skew_ratio": round(mx / med, 1) if med else None,
            "shuffle_read_MB": round(s.get("shuffleReadBytes", 0) / 2**20, 2),
            "shuffle_write_MB": round(s.get("shuffleWriteBytes", 0) / 2**20, 2),
            "spill_memory_MB": round(s.get("memoryBytesSpilled", 0) / 2**20, 2),
            "spill_disk_MB": round(s.get("diskBytesSpilled", 0) / 2**20, 2),
        })
    return pd.DataFrame(rows)


def table_files(spark: SparkSession, table: str) -> dict:
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    n, size = d["numFiles"], d["sizeInBytes"]
    return {"table": table, "numFiles": n, "sizeMB": round(size / 2**20, 2),
            "avgFileKB": round(size / n / 1024, 1) if n else 0,
            "partitionColumns": list(d["partitionColumns"] or []),
            "clusteringColumns": list(d["clusteringColumns"] or [])}


def plot(results: list[dict] | pd.DataFrame, metric: str = "seconds", title: str | None = None,
         highlight: str | None = None, note: str | None = None):
    """One horizontal bar chart of one metric across run() results; `highlight` names the bar to mark as the target."""
    import matplotlib.pyplot as plt

    df = results if isinstance(results, pd.DataFrame) else pd.DataFrame(results)
    labels = [str(q) for q in df.get("query", df.index)]
    values = pd.to_numeric(df.get(metric, pd.Series(0, index=df.index)), errors="coerce").fillna(0).tolist()
    if metric.endswith("bytes") or "_bytes_" in metric:
        values, unit, fmt = [v / 2**20 for v in values], "MB", "{:,.1f} MB"
    elif metric == "seconds":
        unit, fmt = None, "{:,.2f} s"
    else:
        unit, fmt = "count", "{:,.0f}"
    colors = ["#1baf7a" if q == highlight else "#2a78d6" for q in labels]

    plt.rcParams.update({"font.family": "sans-serif", "font.sans-serif": ["Segoe UI", "Arial", "DejaVu Sans"]})
    fig, ax = plt.subplots(figsize=(7, 0.5 * len(labels) + 1.5), facecolor="#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    y = range(len(labels))[::-1]  # first result on top
    top = max(values, default=0) or 1
    ax.barh(list(y), [max(v, top * 0.006) if v else 0 for v in values], height=0.6, color=colors)  # keep tiny bars visible
    for yi, v in zip(y, values):
        label = "< 0.1 MB" if unit == "MB" and 0 < v < 0.05 else fmt.format(v)
        ax.text(v + top * 0.01, yi, label, va="center", fontsize=11, color="#0b0b0b")
    ax.set_yticks(list(y), labels, color="#0b0b0b", fontsize=11)
    ax.set_xlim(0, top * 1.18)
    ax.set_xlabel(f"{metric} ({unit})" if unit else metric, color="#52514e", fontsize=11)
    ax.xaxis.set_major_locator(plt.MaxNLocator(5))
    ax.xaxis.set_major_formatter(lambda x, _: f"{x / 1e6:g}M" if x >= 1e6 else f"{x / 1e3:g}k" if x >= 1e4 else f"{x:,g}")
    ax.set_title(title or metric, loc="left", fontsize=12, fontweight="semibold", color="#0b0b0b")
    ax.grid(axis="x", color="#e4e2dc", linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c8c6bf")
    ax.tick_params(colors="#52514e", length=0, labelsize=11)
    footnote = highlight and note and f"Green bar ({highlight}): {note}"
    if footnote:
        fig.text(0.01, 0.01, footnote, fontsize=11, color="#52514e")
    fig.tight_layout(rect=(0, 0.06 if footnote else 0, 1, 1))
    plt.show()
    return fig
