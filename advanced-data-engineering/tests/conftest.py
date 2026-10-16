import os
import shutil
import uuid

import pytest

os.environ.setdefault("ADELAB_SPARK_UI_PORT", "24070")  # don't collide with a running notebook (24040)


@pytest.fixture(scope="session")
def spark():
    from adelab.spark import build_spark

    s = build_spark("adelab-tests", shuffle_partitions=2)
    yield s
    s.stop()


@pytest.fixture
def workdir():
    from adelab.spark import DATA_ROOT

    d = DATA_ROOT / "tests" / uuid.uuid4().hex[:8]
    d.mkdir(parents=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def schema(spark):
    name = f"t_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE SCHEMA {name}")
    yield name
    spark.sql("USE default")
    spark.sql(f"DROP SCHEMA {name} CASCADE")
