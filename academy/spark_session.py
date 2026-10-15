from __future__ import annotations

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _load_windows_user_env() -> None:
    """Terminals opened before JAVA_HOME / HADOOP_HOME were set (e.g. a long-running VS Code)
    don't see them. Fall back to the values persisted in the user's registry environment."""
    if os.name != "nt":
        return
    import winreg
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            for var in ("JAVA_HOME", "HADOOP_HOME"):
                if not os.environ.get(var):
                    try:
                        os.environ[var] = winreg.QueryValueEx(key, var)[0]
                    except FileNotFoundError:
                        continue
                bin_dir = os.path.join(os.environ[var], "bin")
                if bin_dir.lower() not in os.environ["PATH"].lower():
                    os.environ["PATH"] = bin_dir + os.pathsep + os.environ["PATH"]
    except OSError:
        pass


def build_spark(app_name: str = "local-sdp-demo", warehouse_dir: str | None = None) -> SparkSession:
    _load_windows_user_env()
    # Make Spark's Python workers use this same interpreter / venv (important on Windows).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (SparkSession.builder
               .appName(app_name)
               .master("local[*]")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", "4")
               # Pin the driver to localhost (otherwise Docker/VPN adapters can leak into URLs).
               .config("spark.driver.host", "localhost")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.ui.showConsoleProgress", "false"))
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    # Adds the matching io.delta:delta-spark jar (downloaded via Ivy on first run).
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
