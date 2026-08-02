"""Shared pytest fixtures. Uses a plain local SparkSession (no Delta extension) since
the functions under test in tests/test_*.py are pure DataFrame transforms with no
Delta-specific I/O -- keeping tests Delta-free means they run fast and don't need any
network access to resolve the Delta Lake Maven package.
"""
import pytest
from pyspark.sql import SparkSession


_JAVA17_ARROW_OPENS = " ".join(
    [
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ]
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", _JAVA17_ARROW_OPENS)
        .config("spark.executor.extraJavaOptions", _JAVA17_ARROW_OPENS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()
