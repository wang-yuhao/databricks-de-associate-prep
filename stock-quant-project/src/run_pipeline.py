"""Runs the full pipeline end to end, in dependency order -- the local equivalent of
a multi-task Databricks Job:

    ingest (bronze) -> silver (bars, quotes, trades) -> gold (features) -> train -> backtest

Each stage is a separate task with a clear dependency on the previous one finishing
successfully, same as you'd configure task dependencies in the Jobs UI/API. If a
stage raises, we stop immediately rather than continuing on to a stage whose input
doesn't exist yet -- mirrors a Job's default "stop on task failure" behavior.

Usage:
  python -m src.run_pipeline --mode synthetic     # generates fake data first, then runs everything
  python -m src.run_pipeline --mode postgres      # assumes bronze is already populated from Postgres
"""
import argparse
import sys
import time

from src.ingestion import postgres_to_bronze
from src.ml import train_model
from src.transform import bronze_to_silver, silver_to_gold
from src.utils.config import load_config
from src.utils.spark_session import get_spark


def _run_stage(name, fn):
    print(f"\n{'=' * 60}\n TASK: {name}\n{'=' * 60}")
    start = time.time()
    try:
        result = fn()
    except Exception:
        print(f"\n!! TASK '{name}' FAILED after {time.time() - start:.1f}s -- stopping pipeline.")
        raise
    print(f"-- TASK '{name}' completed in {time.time() - start:.1f}s")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["synthetic", "postgres"], default="synthetic")
    parser.add_argument("--years", type=int, default=3, help="years of synthetic history (synthetic mode only)")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)

    try:
        if args.mode == "synthetic":
            _run_stage("ingest:synthetic", lambda: postgres_to_bronze.ingest_synthetic(spark, cfg, years=args.years))
        else:
            for table in ["bars", "quotes", "trades"]:
                _run_stage(f"ingest:postgres:{table}", lambda t=table: postgres_to_bronze.ingest_postgres(spark, cfg, t))

        for table in ["bars", "quotes", "trades"]:
            _run_stage(f"silver:{table}", lambda t=table: bronze_to_silver.run(spark, cfg, table_key=t))

        _run_stage("gold:features", lambda: silver_to_gold.run(spark, cfg))

    finally:
        spark.stop()

    # train_model.py and backtester.py manage their own Spark sessions since they're
    # also meant to be run standalone -- run them as subprocess-style stages here too.
    _run_stage("train:models", lambda: _run_module(train_model))
    from src.backtest import backtester
    _run_stage("backtest:strategy", lambda: _run_module(backtester))

    print("\nPipeline complete. See ./data/lakehouse for bronze/silver/gold tables, "
          "./mlruns for MLflow experiment tracking, and ./data/models for saved models.")


def _run_module(module):
    old_argv = sys.argv
    sys.argv = [old_argv[0]]  # each module's main() parses its own argv; keep it clean
    try:
        module.main()
    finally:
        sys.argv = old_argv


if __name__ == "__main__":
    main()
