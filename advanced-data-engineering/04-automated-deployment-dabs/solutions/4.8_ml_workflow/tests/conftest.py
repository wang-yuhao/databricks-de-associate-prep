import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Plain local Spark: the transformations under test don't need Delta or a workspace.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    s = (SparkSession.builder.master("local[2]").appName("orders_lib-tests")
         .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    yield s
    s.stop()
