"""Environment check:  python -m adelab.doctor

Verifies everything the courses need and prints a fix for each problem found.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

OK, WARN, FAIL = "OK  ", "WARN", "FAIL"
results: list[tuple[str, str, str]] = []


def report(status: str, check: str, detail: str = "") -> None:
    results.append((status, check, detail))
    print(f"[{status}] {check}" + (f" - {detail}" if detail else ""), flush=True)


def check_python() -> None:
    v = sys.version_info
    if (3, 10) <= (v.major, v.minor) <= (3, 12):
        report(OK, "Python", sys.version.split()[0] + f" ({sys.executable})")
    else:
        report(FAIL, "Python", f"{sys.version.split()[0]}: use Python 3.11 (py -3.11 -m venv .venv)")


def check_java() -> None:
    from .spark import _load_windows_user_env

    _load_windows_user_env()
    java = Path(os.environ["JAVA_HOME"]) / "bin" / "java" if os.environ.get("JAVA_HOME") else shutil.which("java")
    if not java:
        report(FAIL, "Java", "not found: install JDK 17 (winget install EclipseAdoptium.Temurin.17.JDK) "
                             "and set JAVA_HOME")
        return
    out = subprocess.run([str(java), "-version"], capture_output=True, text=True).stderr
    m = re.search(r'version "(\d+)', out)
    major = int(m.group(1)) if m else 0
    if major in (17, 21):
        report(OK, "Java", out.splitlines()[0])
    else:
        report(FAIL, "Java", f"{out.splitlines()[0] if out else '?'}: Spark 4.2 needs Java 17 or 21")


def check_hadoop() -> None:
    if os.name != "nt":
        report(OK, "Hadoop native libs", "not needed on macOS/Linux")
        return
    home = os.environ.get("HADOOP_HOME")
    if home and (Path(home) / "bin" / "winutils.exe").exists() and (Path(home) / "bin" / "hadoop.dll").exists():
        report(OK, "HADOOP_HOME", home)
    else:
        report(FAIL, "HADOOP_HOME", "winutils.exe + hadoop.dll (Hadoop 3.3.x) missing: see README > Setup")


def check_env_overrides() -> None:
    other = os.environ.get("PYSPARK_PYTHON")
    if other and Path(other).resolve() != Path(sys.executable).resolve():
        report(WARN, "PYSPARK_PYTHON", f"set to {other} by your environment; adelab overrides it with this "
                                       "interpreter for its sessions (no action needed)")
    if os.environ.get("SPARK_HOME"):
        report(WARN, "SPARK_HOME", f"{os.environ['SPARK_HOME']} is set; unset it if Spark fails to start")


def check_ports() -> None:
    if os.name != "nt":
        return
    from .spark import UI_PORT

    out = subprocess.run(["netsh", "int", "ipv4", "show", "excludedportrange", "protocol=tcp"],
                         capture_output=True, text=True).stdout
    ranges = [(int(a), int(b)) for a, b in re.findall(r"^\s*(\d+)\s+(\d+)", out, re.M)]
    port = int(UI_PORT)
    if any(a <= port <= b for a, b in ranges):
        report(FAIL, "Spark UI port", f"{port} is reserved by Windows; set ADELAB_SPARK_UI_PORT to a free port")
    else:
        reserved_4040 = any(a <= 4040 <= b for a, b in ranges)
        report(OK, "Spark UI port", f"{port} is free" + (" (4040, Spark's default, is reserved by Windows "
                                                          "on this machine)" if reserved_4040 else ""))


def check_spark() -> None:
    t = time.time()
    try:
        from .spark import DATA_ROOT, build_spark

        spark = build_spark("adelab-doctor")
        path = (DATA_ROOT / "doctor" / "delta_check").as_posix()
        spark.range(10).write.format("delta").mode("overwrite").save(path)
        n = spark.read.format("delta").load(path).count()
        spark.sql(f"CREATE TABLE IF NOT EXISTS default.adelab_doctor_lc (id BIGINT) USING DELTA CLUSTER BY (id)")
        spark.sql("DROP TABLE default.adelab_doctor_lc")
        from pyspark.sql.functions import pandas_udf
        import pandas as pd

        @pandas_udf("long")
        def plus_one(s: pd.Series) -> pd.Series:
            return s + 1
        assert spark.range(3).select(plus_one("id")).count() == 3
        report(OK, "Spark + Delta + pandas UDF", f"Spark {spark.version}, Delta write/read {n} rows, "
                                                 f"Liquid Clustering OK, UI {spark.sparkContext.uiWebUrl} "
                                                 f"({time.time() - t:.0f}s)")
    except Exception as e:  # noqa: BLE001
        report(FAIL, "Spark + Delta", f"{type(e).__name__}: {str(e).splitlines()[0][:200]}")


def check_cli() -> None:
    from .bundles import cli_path

    exe = cli_path()
    if not exe:
        report(WARN, "Databricks CLI", "not installed (needed for Course 4): winget install Databricks.DatabricksCLI")
        return
    v = subprocess.run([exe, "--version"], capture_output=True, text=True).stdout.strip()
    report(OK, "Databricks CLI", v)


def main() -> int:
    print("adelab environment check\n" + "=" * 60)
    check_python()
    check_java()
    check_hadoop()
    check_env_overrides()
    check_ports()
    check_cli()
    check_spark()
    failed = [r for r in results if r[0] == FAIL]
    print("=" * 60)
    print("All good: start the lab with setup/start_lab.ps1" if not failed else f"{len(failed)} problem(s) to fix")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
