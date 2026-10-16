"""Local Spark 4.2 + Delta Lake 4.4 session that behaves as close to a Databricks cluster as OSS allows."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession

SPARK_VERSION = "4.2"
DELTA_VERSION = "4.4.1"
DELTA_PACKAGES = [
    f"io.delta:delta-spark_{SPARK_VERSION}_2.13:{DELTA_VERSION}",
    # UniForm: lets Delta tables publish Iceberg metadata (Course 1, Multiplex + Iceberg reads).
    f"io.delta:delta-iceberg_2.13:{DELTA_VERSION}",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("ADELAB_DATA_ROOT", PROJECT_ROOT / ".lab-data")).resolve()

# Windows reserves TCP 4033-4832 for Hyper-V/WinNAT on many machines, which makes Spark's default
# UI port 4040 unbindable. 24040 sits outside the usual exclusion ranges.
UI_PORT = os.environ.get("ADELAB_SPARK_UI_PORT", "24040")


def _load_windows_user_env() -> None:
    """Terminals/IDEs opened before JAVA_HOME or HADOOP_HOME were set don't see them; read the registry."""
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


def _spark_submit() -> Path:
    import pyspark

    bin_dir = Path(pyspark.__file__).parent / "bin"
    return bin_dir / ("spark-submit.cmd" if os.name == "nt" else "spark-submit")


def resolve_jars(refresh: bool = False) -> list[Path]:
    """Resolve the Delta packages (and dependencies) once with Ivy and cache the list of local jars.

    Jars are then passed as `local:` URIs, which Spark loads from disk. With `spark.jars.packages`
    the local executor instead downloads each jar from the driver's file server over loopback at
    startup, which intermittently stalls on Windows and hangs SparkContext creation.
    """
    jar_dir = DATA_ROOT / "jars"
    jar_dir.mkdir(parents=True, exist_ok=True)
    manifest = jar_dir / f"manifest-spark{SPARK_VERSION}-delta{DELTA_VERSION}.txt"
    if manifest.exists() and not refresh:
        jars = [Path(j) for j in manifest.read_text(encoding="utf-8").splitlines() if j.strip()]
        if jars and all(j.exists() for j in jars):
            return jars

    import subprocess

    probe = jar_dir / "_print_resolved_jars.py"
    probe.write_text(
        "from pyspark import SparkConf, SparkContext\n"
        "SparkContext._ensure_initialized()\n"
        "print('ADELAB_JARS=' + SparkConf().get('spark.jars', ''))\n", encoding="utf-8")
    env = {**os.environ, "PYSPARK_PYTHON": sys.executable, "PYSPARK_DRIVER_PYTHON": sys.executable}
    print("[adelab] resolving Delta Lake jars (first run only, needs internet) ...")
    out = subprocess.run([str(_spark_submit()), "--packages", ",".join(DELTA_PACKAGES), str(probe)],
                         env=env, capture_output=True, text=True, timeout=900)
    line = next((l for l in out.stdout.splitlines() if l.startswith("ADELAB_JARS=")), None)
    if not line or not line.split("=", 1)[1].strip():
        raise RuntimeError("Could not resolve Delta Lake jars. Check internet access / proxy.\n"
                           + out.stdout[-2000:] + out.stderr[-4000:])
    jars = []
    for uri in line.split("=", 1)[1].split(","):
        p = uri.strip()
        p = p[len("file:"):] if p.startswith("file:") else p
        if os.name == "nt":
            p = re.sub(r"^/+(?=[A-Za-z]:)", "", p)  # file:///C:/x, file://C:/x, file:/C:/x -> C:/x
        jars.append(Path(p))
    manifest.write_text("\n".join(str(j) for j in jars), encoding="utf-8")
    return jars


def jars_conf() -> str:
    return ",".join("local:" + Path(j).resolve().as_posix() if os.name != "nt"
                    else "local:/" + Path(j).resolve().as_posix() for j in resolve_jars())


def file_uri(path: str | os.PathLike) -> str:
    """Qualified `file:/C:/...` URI. Delta treats `C:\\x` and `file:/C:/x` as different locations on Windows."""
    return "file:/" + Path(path).resolve().as_posix().lstrip("/")


def build_spark(app_name: str = "adelab", *, shuffle_partitions: int = 8, driver_memory: str = "4g",
                extra_conf: dict[str, str] | None = None) -> SparkSession:
    """Return the (single) local SparkSession, creating it on first call."""
    _load_windows_user_env()
    # Always use this interpreter for Python workers: a PYSPARK_PYTHON inherited from another project
    # (different PySpark / pandas versions) makes pandas UDFs and applyInPandas crash.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")  # skip reverse-DNS of the local IP

    warehouse = DATA_ROOT / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars", jars_conf())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.warehouse.dir", file_uri(warehouse))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.port", UI_PORT)
        .config("spark.ui.showConsoleProgress", "false")
        # Keep history of finished SQL queries for the Spark UI and adelab.perf metrics.
        .config("spark.sql.ui.retainedExecutions", "500")
        # On Windows each Python worker keeps this auth timeout as its socket read timeout (there is no
        # worker daemon), so a pause of more than 15s in a long UDF job kills the worker. Raise it.
        .config("spark.python.authenticate.socketTimeout", "10min")
        # Delta's default of 50 partitions for log replay is tuned for clusters; locally it only adds latency.
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
