"""
End-to-end driver: clean -> generate dirty data -> Bronze -> Silver -> Gold -> verify clustering.

    python run_demo.py                 # normal run (warn + drop rules fire)
    python run_demo.py --inject-fatal  # adds a NULL event_id row -> expect_or_fail aborts
"""
import argparse
import json
import os
import random
import shutil
from datetime import datetime, timedelta

from delta.tables import DeltaTable

from pipeline import ExpectationFailed, MultiFlowPipeline
from spark_session import build_spark

# Matches the course's /tmp layout; on Windows "/tmp" would land on the drive root,
# so we default to a project-local ./tmp instead. Override with DEMO_ROOT.
DEFAULT_ROOT = "/tmp" if os.name != "nt" else os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")
ROOT = os.environ.get("DEMO_ROOT", DEFAULT_ROOT)
MANAGED_DIRS = ["source_flow_a", "source_flow_b", "lake", "checkpoints"]


def clean_workspace() -> None:
    for d in MANAGED_DIRS:
        shutil.rmtree(os.path.join(ROOT, d), ignore_errors=True)
    print(f"[setup] cleaned demo dirs under {ROOT}")


def generate_mock_data(inject_fatal: bool, files_per_flow: int = 3, rows_per_file: int = 50) -> None:
    rng = random.Random(42)
    base = datetime(2026, 10, 1)
    statuses = ["ACTIVE", "INACTIVE", "PENDING", "ERROR"]
    expected = {"good": 0, "null_ts": 0, "bad_status": 0, "neg_amount": 0}

    for flow in ("a", "b"):
        out_dir = os.path.join(ROOT, f"source_flow_{flow}")
        os.makedirs(out_dir, exist_ok=True)
        for f_idx in range(files_per_flow):
            rows = []
            for r in range(rows_per_file):
                ts = base + timedelta(days=rng.randint(0, 4), minutes=rng.randint(0, 1440))
                row = {
                    "event_id": f"{flow}-{f_idx}-{r}",
                    "device_id": f"dev-{rng.randint(1, 20):03d}",
                    "status": rng.choice(statuses),
                    "amount": round(rng.uniform(1, 500), 2),
                    "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                }
                roll = rng.random()
                if roll < 0.08:
                    row["event_ts"] = None;            expected["null_ts"] += 1      # -> dropped
                elif roll < 0.15:
                    row["status"] = rng.choice(["UNKNOWN", "", "active!"]); expected["bad_status"] += 1  # -> dropped
                elif roll < 0.20:
                    row["amount"] = -row["amount"];    expected["neg_amount"] += 1   # -> warn only, kept
                else:
                    expected["good"] += 1
                rows.append(row)
            if inject_fatal and flow == "b" and f_idx == files_per_flow - 1:
                rows.append({"event_id": None, "device_id": "dev-999", "status": "ACTIVE",
                             "amount": 1.0, "event_ts": "2026-10-01 00:00:00"})
            with open(os.path.join(out_dir, f"part-{f_idx:03d}.json"), "w") as fh:
                fh.write("\n".join(json.dumps(x) for x in rows))
    print(f"[setup] generated mock data: {expected}"
          + ("  (+1 NULL event_id row)" if inject_fatal else ""))


def verify_liquid_clustering(spark, path: str) -> None:
    detail = DeltaTable.forPath(spark, path).detail()
    cols = detail.columns
    show = [c for c in ("format", "numFiles", "clusteringColumns", "tableFeatures",
                        "minReaderVersion", "minWriterVersion") if c in cols]
    print("\n=== DeltaTable.detail() for silver_events ===")
    detail.select(*show).show(truncate=False)

    row = detail.first()
    clustering = list(row["clusteringColumns"]) if "clusteringColumns" in cols else []
    features = list(row["tableFeatures"] or []) if "tableFeatures" in cols else []
    assert clustering == MultiFlowPipeline.CLUSTER_KEYS, f"unexpected clustering columns: {clustering}"
    assert "clustering" in features, f"'clustering' table feature missing: {features}"
    print(f"[verify] Liquid Clustering ACTIVE on {clustering}; table features = {features}")

    print("\n=== DESCRIBE HISTORY (latest ops) ===")
    (spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
          .select("version", "operation", "operationParameters").show(5, truncate=80))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inject-fatal", action="store_true",
                        help="add a NULL event_id row to trigger expect_or_fail")
    args = parser.parse_args()

    clean_workspace()
    generate_mock_data(args.inject_fatal)

    spark = build_spark()
    pipe = MultiFlowPipeline(spark, ROOT)
    try:
        print("\n[run] BRONZE (2 append flows -> bronze_events)")
        pipe.run_bronze()
        bronze = spark.read.format("delta").load(pipe.bronze_path)
        bronze.groupBy("source_flow").count().orderBy("source_flow").show()

        print("[run] SILVER (expectations + liquid clustering)")
        try:
            pipe.run_silver()
        except Exception as exc:  # foreachBatch wraps the Python exception
            if isinstance(exc, ExpectationFailed) or "ExpectationFailed" in str(exc):
                reason = next((l for l in str(exc).splitlines() if "expect_or_fail '" in l), str(exc))
                print(f"\n[FAIL] pipeline update aborted by expect_or_fail:\n  {reason.strip()}")
                return
            raise

        print("=== Expectation metrics (per micro-batch) ===")
        spark.createDataFrame(pipe.dq.metrics) \
             .groupBy("expectation", "action").sum("total", "failed").orderBy("expectation").show()

        silver = spark.read.format("delta").load(pipe.silver_path)
        quarantine = spark.read.format("delta").load(pipe.quarantine_path)
        print(f"bronze={bronze.count()}  silver={silver.count()}  quarantine={quarantine.count()}")
        print("=== Quarantine sample ===")
        quarantine.select("event_id", "status", "event_ts", "failed_expectations").show(5, truncate=False)

        verify_liquid_clustering(spark, pipe.silver_path)

        print("\n[run] GOLD (daily metrics by status)")
        pipe.run_gold().orderBy("ingest_date", "status").show(40)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
