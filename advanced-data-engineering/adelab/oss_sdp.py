"""Run a project with the real open-source Spark Declarative Pipelines engine (`spark-pipelines`, Spark 4.1+).

The OSS engine supports the core of the API (`@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`,
`dp.create_streaming_table`, `@dp.append_flow`, `dp.create_sink`, SQL `CREATE MATERIALIZED VIEW ...`).
Expectations, Liquid Clustering via `cluster_by`, and AUTO CDC into Delta are Databricks-only today; the
local `adelab.sdp` engine covers those.

On Windows `spark-pipelines` is a bash script, so this module launches the same Java entry point through
`spark-class2.cmd`, with a generated spark-defaults.conf (Delta, a free UI port, localhost binding).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pyspark
import yaml

from .spark import DATA_ROOT, _load_windows_user_env, file_uri, jars_conf

OSS_UI_PORT = os.environ.get("ADELAB_OSS_UI_PORT", "24041")
_NOISE = ("WARN ", "Ivy Default", "The jars for", "found ", "from central", ":: ", "\tconfs", "---",
          "|", "artifacts copied", "added as a dependency", "loading settings", "resolving dependencies",
          "evicted", "modules in use", "FutureWarning", "require_minimum", "incubator", "log4j",
          "setLogLevel", "Setting default log level", "SUCCESS: The process")


def _write_conf(conf_dir: Path, warehouse: Path) -> None:
    conf_dir.mkdir(parents=True, exist_ok=True)
    (conf_dir / "spark-defaults.conf").write_text("\n".join([
        f"spark.jars                       {jars_conf()}",
        "spark.sql.extensions             io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog  org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.sources.default        delta",
        f"spark.sql.warehouse.dir          {file_uri(warehouse)}",
        f"spark.ui.port                    {OSS_UI_PORT}",
        "spark.driver.host                127.0.0.1",
        "spark.driver.bindAddress         127.0.0.1",
        "spark.databricks.delta.snapshotPartitions 2",
        "spark.sql.shuffle.partitions     4",
        "spark.ui.showConsoleProgress     false",
    ]) + "\n", encoding="utf-8")


def _kill_leftovers(work: Path) -> None:
    """Stop JVM/Python helpers a previous run left behind (they hold ports and break the next run)."""
    if os.name != "nt":
        return
    marker = str(work).replace("'", "''")
    ps = ("Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and "
          f"$_.CommandLine.Contains('{marker}') -and $_.ProcessId -ne {os.getpid()} }} | "
          "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }")
    subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, timeout=60)


def prepare_project(project: str | os.PathLike, name: str, configuration: dict[str, str] | None = None) -> Path:
    """Copy the pipeline project to the lab data dir and generate its spark-pipeline.yml."""
    src = Path(project).resolve()
    work = DATA_ROOT / "oss-sdp" / name
    proj = work / "project"
    shutil.rmtree(proj, ignore_errors=True)
    shutil.copytree(src, proj, ignore=shutil.ignore_patterns("__pycache__", "spark-pipeline.yml"),
                    dirs_exist_ok=True)
    (work / "storage").mkdir(parents=True, exist_ok=True)
    spec = {"name": name, "storage": file_uri(work / "storage"),
            "libraries": [{"glob": {"include": "transformations/**"}}]}
    if configuration:
        spec["configuration"] = {k: str(v) for k, v in configuration.items()}
    (proj / "spark-pipeline.yml").write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    return proj


def run(project: str | os.PathLike, name: str, configuration: dict[str, str] | None = None,
        command: str = "run", full_refresh: bool = False, verbose: bool = False, timeout: int = 900) -> int:
    """`spark-pipelines run` (or `dry-run`) for the project. Returns the process exit code."""
    _load_windows_user_env()
    proj = prepare_project(project, name, configuration)
    work = proj.parent
    if full_refresh:
        shutil.rmtree(work / "warehouse", ignore_errors=True)
        shutil.rmtree(work / "storage", ignore_errors=True)
        (work / "storage").mkdir(parents=True, exist_ok=True)
    _kill_leftovers(work)
    conf_dir = work / "conf"
    _write_conf(conf_dir, work / "warehouse")

    spark_home = Path(pyspark.__file__).parent
    # Start from a clean environment: variables a live notebook SparkSession exported must not leak in.
    inherited = {k: v for k, v in os.environ.items() if not k.upper().startswith(("SPARK_", "PYSPARK_"))}
    env = {**inherited, "SPARK_HOME": str(spark_home), "SPARK_CONF_DIR": str(conf_dir),
           "PYSPARK_PYTHON": sys.executable, "PYSPARK_DRIVER_PYTHON": sys.executable,
           "SPARK_LOCAL_IP": "127.0.0.1", "SPARK_LOCAL_HOSTNAME": "localhost"}
    cli = spark_home / "pipelines" / "cli.py"
    args = [command] + (["--full-refresh-all"] if full_refresh and command == "run" else [])
    if os.name == "nt":
        cmd = [str(spark_home / "bin" / "spark-class2.cmd"), "org.apache.spark.deploy.SparkPipelines",
               str(cli), *args]
    else:
        cmd = [str(spark_home / "bin" / "spark-pipelines"), *args]

    print(f"$ spark-pipelines {' '.join(args)}   (cwd={proj})")
    # Log to a file instead of a pipe: on Windows, helper JVMs inherit the pipe handle and keep it
    # open after the run finishes, so reading a pipe to EOF would hang.
    log = work / f"{command}.log"
    deadline = time.time() + timeout
    with open(log, "w", encoding="utf-8") as out, open(log, encoding="utf-8", errors="replace") as tail:
        proc = subprocess.Popen(cmd, cwd=proj, env=env, stdout=out, stderr=subprocess.STDOUT)
        try:
            while True:
                done = proc.poll() is not None
                for line in tail.readlines():
                    line = line.rstrip()
                    if line and not line.lstrip().startswith("at ") and (
                            verbose or not any(n in line for n in _NOISE)):
                        print(line)
                if done:
                    break
                if time.time() > deadline:
                    raise TimeoutError(f"spark-pipelines did not finish within {timeout}s (log: {log})")
                time.sleep(0.5)
        finally:
            if proc.poll() is None:
                proc.kill()
    _kill_leftovers(work)
    print(f"exit code {proc.returncode}   (full log: {log})")
    return proc.returncode


def table_path(name: str, table: str) -> str:
    """Location of a table produced by the OSS engine (default schema), for spark.read.format('delta')."""
    return file_uri(DATA_ROOT / "oss-sdp" / name / "warehouse" / table)
