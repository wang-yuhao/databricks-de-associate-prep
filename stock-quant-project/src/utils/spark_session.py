"""Build a local SparkSession wired up for Delta Lake, with a real catalog
(backed by a local Derby warehouse dir) so we can register bronze/silver/gold
tables under proper database namespaces -- this gives us somewhere to attach
table comments, run SQL DDL, and mimic the catalog.schema.table structure
you'd get from Unity Catalog on Databricks.
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

import shutil
from pathlib import Path


def _cleanup_stale_spark_temp_dirs(local_dir: str) -> None:
    """Best-effort cleanup of leftover spark-*/userFiles-* dirs from prior runs.

    On Windows, the JVM can hold file locks on JARs copied into Spark's temp
    dir (e.g. antlr4-runtime-*.jar) even after SparkContext.stop() returns,
    so Spark itself fails to delete them ("Failed to delete: ...jar") and logs
    a WARN on every run. By the time a *new* run starts, the previous JVM has
    fully exited and released those locks, so it's safe to sweep them here.
    """
    root = Path(local_dir)
    if not root.exists():
        return
    for entry in root.glob("spark-*"):
        try:
            shutil.rmtree(entry, ignore_errors=True)
        except Exception:
            pass

# Java 17+/21 locks down internals that Arrow (used by Pandas UDFs, applyInPandas,
# and toPandas()) needs reflective access to. Without these --add-opens flags you'll
# hit "sun.misc.Unsafe or java.nio.DirectByteBuffer.<init> not available" the first
# time any Arrow-based code path runs. Harmless on Java 11 if you're on an older JDK.
_JAVA17_ARROW_OPENS = " ".join(
    [
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ]
)


def get_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg["spark"]
    warehouse_dir = cfg["delta"]["spark_warehouse_dir"]

    local_tmp_dir = str(Path(warehouse_dir).parent / "spark_tmp")
    Path(local_tmp_dir).mkdir(parents=True, exist_ok=True)
    _cleanup_stale_spark_temp_dirs(local_tmp_dir)

    builder = (
        SparkSession.builder.appName(spark_cfg["app_name"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(spark_cfg["shuffle_partitions"]))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", spark_cfg["driver_memory"])
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.local.dir", local_tmp_dir)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.extraJavaOptions", _JAVA17_ARROW_OPENS)
        .config("spark.executor.extraJavaOptions", _JAVA17_ARROW_OPENS)
        # Arrow-based toPandas()/applyInPandas() hit a well-known reflection
        # incompatibility on Java 17+/21 (org.apache.arrow.memory.util.MemoryUtil).
        # Disabled by default so the project runs the same regardless of your JDK.
        # If you're on Java 8/11 with a working Arrow setup, flip this to "true" --
        # it meaningfully speeds up the feature-engineering step on large datasets.
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for db in ["bronze", "silver", "gold", "monitoring"]:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    return spark
