import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import DataQualityManager, ExpectationFailed  # noqa: E402
from spark_session import build_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = build_spark("dq-tests")
    yield s
    s.stop()


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [("1", "ACTIVE", 10.0, "2026-10-01 10:00:00"),
         ("2", "UNKNOWN", 5.0, "2026-10-01 11:00:00"),   # bad status -> drop
         ("3", "PENDING", -3.0, "2026-10-02 09:00:00"),  # negative -> warn, kept
         ("4", "ERROR", 7.0, None)],                     # null ts -> drop
        "event_id string, status string, amount double, event_ts string")


def make_dq():
    return (DataQualityManager()
            .expect("non_negative_amount", "amount >= 0")
            .expect_or_drop("valid_timestamp", "event_ts IS NOT NULL")
            .expect_or_drop("valid_status", "status IN ('ACTIVE','INACTIVE','PENDING','ERROR')")
            .expect_or_fail("valid_event_id", "event_id IS NOT NULL"))


def test_drop_rules_split_clean_and_quarantine(df):
    clean, quarantine = make_dq().apply(df)
    assert sorted(r.event_id for r in clean.collect()) == ["1", "3"]
    q = {r.event_id: r.failed_expectations for r in quarantine.collect()}
    assert q == {"2": ["valid_status"], "4": ["valid_timestamp"]}


def test_warn_rule_keeps_rows_but_records_metric(df):
    dq = make_dq()
    clean, _ = dq.apply(df)
    warn = next(m for m in dq.metrics if m["expectation"] == "non_negative_amount")
    assert warn["failed"] == 1 and warn["action"] == "warn"
    assert "3" in [r.event_id for r in clean.collect()]


def test_null_rule_result_counts_as_violation(spark):
    df = spark.createDataFrame([("1", None, 1.0, "2026-10-01 00:00:00")],
                               "event_id string, status string, amount double, event_ts string")
    clean, quarantine = make_dq().apply(df)
    assert clean.count() == 0 and quarantine.count() == 1


def test_fail_rule_raises(spark, df):
    bad = df.unionByName(spark.createDataFrame(
        [(None, "ACTIVE", 1.0, "2026-10-01 00:00:00")], df.schema))
    with pytest.raises(ExpectationFailed, match="valid_event_id"):
        make_dq().apply(bad)
