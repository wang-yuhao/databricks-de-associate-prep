"""Cost & Performance Optimization exercises -- this is your weakest exam section
(37%), so the point of this file is to make you actually run these operations and
look at their effect, not just read about them.

What's here:
  optimize_table()       -- file compaction (`OPTIMIZE`) + Z-ORDER BY a given column
  vacuum_table()         -- remove old, unreferenced files past the retention window
  compare_query_plans()  -- run the same filter query before/after optimization and
                            print both physical plans so you can see the difference
                            in file/partition pruning

Two Databricks-only features this can't reproduce locally, so you understand the gap:
  - Liquid Clustering (Databricks-managed clustering that replaces manual Z-Ordering
    for new tables) -- open-source delta-spark only supports classic Z-Ordering via
    `OPTIMIZE ... ZORDER BY`.
  - Unity Catalog system tables for query/billing history -- there's no equivalent
    catalog locally; monitoring/pipeline_monitor.py's `pipeline_runs` Delta table is
    the closest local stand-in.
"""
import argparse
import time

from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark


def optimize_table(spark, table_path: str, zorder_by: list = None):
    zorder_clause = f"ZORDER BY ({', '.join(zorder_by)})" if zorder_by else ""
    sql = f"OPTIMIZE delta.`{table_path}` {zorder_clause}"
    print(f"[optimize] {sql}")
    start = time.time()
    result = spark.sql(sql)
    result.show(truncate=False)
    print(f"[optimize] done in {time.time() - start:.1f}s")


def vacuum_table(spark, table_path: str, retain_hours: int = 168):
    # 168h = 7 days, Delta's default safety minimum unless you disable the check.
    sql = f"VACUUM delta.`{table_path}` RETAIN {retain_hours} HOURS"
    print(f"[vacuum] {sql}")
    spark.sql(sql).show(truncate=False)


def compare_query_plans(spark, table_path: str, filter_col: str, filter_value):
    df = spark.read.format("delta").load(table_path)
    print(f"\n--- Physical plan: filter {filter_col} = {filter_value!r} ---")
    df.filter(f"{filter_col} = '{filter_value}'").explain(mode="formatted")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="gold/features")
    parser.add_argument("--zorder-by", nargs="+", default=["symbol"])
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--retain-hours", type=int, default=168)
    parser.add_argument("--explain-col", default="symbol")
    parser.add_argument("--explain-value", default="AAPL")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)

    warehouse_root = resolve_path(cfg, "delta.warehouse_path")
    table_path = f"{warehouse_root}/{args.table}"

    print("### BEFORE optimize ###")
    compare_query_plans(spark, table_path, args.explain_col, args.explain_value)

    optimize_table(spark, table_path, zorder_by=args.zorder_by)

    print("### AFTER optimize ###")
    compare_query_plans(spark, table_path, args.explain_col, args.explain_value)

    if args.vacuum:
        vacuum_table(spark, table_path, retain_hours=args.retain_hours)

    spark.stop()


if __name__ == "__main__":
    main()
