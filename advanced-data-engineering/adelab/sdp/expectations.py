"""Expectation evaluation with the same actions and metrics as Databricks pipelines."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .api import Expectation


class ExpectationViolation(Exception):
    """An expect_or_fail rule was violated: the flow (and the pipeline update) fails."""


def _flag(i: int) -> str:
    return f"__dq_{i}"


def evaluate(df: DataFrame, expectations: list[Expectation], flow: str, dataset: str
             ) -> tuple[DataFrame, dict, callable]:
    """Return (clean_df, metrics, release).

    A rule whose result is NULL is counted as a violation, so write rules that are explicit about
    NULLs (for example `col IS NOT NULL AND col > 0`). `release()` unpersists the cached input.
    """
    if not expectations:
        return df, {"dropped_records": 0, "expectations": []}, lambda: None

    flagged = df.select("*", *[F.coalesce(F.expr(e.constraint), F.lit(False)).alias(_flag(i))
                               for i, e in enumerate(expectations)]).persist()
    drop_idx = [i for i, e in enumerate(expectations) if e.action == "drop"]
    any_drop_fails = reduce(lambda a, b: a | b, [~F.col(_flag(i)) for i in drop_idx], F.lit(False))
    aggs = [F.count(F.lit(1)).alias("total"),
            F.sum(any_drop_fails.cast("long")).alias("dropped")]
    aggs += [F.sum((~F.col(_flag(i))).cast("long")).alias(f"f{i}") for i in range(len(expectations))]
    row = flagged.agg(*aggs).first()
    total = row["total"] or 0

    metrics = {"dropped_records": row["dropped"] or 0, "expectations": []}
    for i, e in enumerate(expectations):
        failed = row[f"f{i}"] or 0
        metrics["expectations"].append({"name": e.name, "dataset": dataset, "action": e.action,
                                        "passed_records": total - failed, "failed_records": failed})
        if e.action == "fail" and failed:
            sample = (flagged.filter(~F.col(_flag(i))).drop(*[_flag(j) for j in range(len(expectations))])
                      .select(F.to_json(F.struct("*"), {"ignoreNullFields": "false"})).limit(1).first()[0])
            flagged.unpersist()
            raise ExpectationViolation(
                f"Flow '{flow}' failed: expectation '{e.name}' ({e.constraint}) was violated by "
                f"{failed} record(s) on dataset '{dataset}'. Example record: {sample}")

    keep = reduce(lambda a, b: a & b, [F.col(_flag(i)) for i in drop_idx], F.lit(True))
    clean = flagged.filter(keep).drop(*[_flag(i) for i in range(len(expectations))])
    return clean, metrics, flagged.unpersist


def merge_metrics(acc: dict | None, new: dict) -> dict:
    if acc is None:
        return {"dropped_records": new["dropped_records"],
                "expectations": [dict(e) for e in new["expectations"]]}
    acc["dropped_records"] += new["dropped_records"]
    by_name = {e["name"]: e for e in acc["expectations"]}
    for e in new["expectations"]:
        if e["name"] in by_name:
            by_name[e["name"]]["passed_records"] += e["passed_records"]
            by_name[e["name"]]["failed_records"] += e["failed_records"]
        else:
            acc["expectations"].append(dict(e))
    return acc
