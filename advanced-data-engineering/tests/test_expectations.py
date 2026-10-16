import pytest

from adelab.sdp.api import Expectation
from adelab.sdp.expectations import ExpectationViolation, evaluate, merge_metrics

RULES = [Expectation("non_negative", "amount >= 0", "warn"),
         Expectation("valid_status", "status IN ('A', 'B')", "drop"),
         Expectation("valid_id", "id IS NOT NULL", "fail")]


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [("1", "A", 10.0), ("2", "X", 5.0), ("3", "B", -1.0), ("4", None, 1.0)],
        "id string, status string, amount double")


def test_drop_warn_and_metrics(df):
    clean, m, release = evaluate(df, RULES, "flow", "ds")
    assert sorted(r.id for r in clean.collect()) == ["1", "3"]    # warn keeps row 3
    by = {e["name"]: e for e in m["expectations"]}
    assert by["non_negative"]["failed_records"] == 1 and by["non_negative"]["action"] == "warn"
    assert by["valid_status"]["failed_records"] == 2               # 'X' and NULL
    assert m["dropped_records"] == 2
    release()


def test_null_result_counts_as_violation(spark):
    df = spark.createDataFrame([("1", None, 1.0)], "id string, status string, amount double")
    clean, m, _ = evaluate(df, RULES, "flow", "ds")
    assert clean.count() == 0 and m["dropped_records"] == 1


def test_fail_rule_raises_with_example(spark, df):
    bad = df.unionByName(spark.createDataFrame([(None, "A", 1.0)], df.schema))
    with pytest.raises(ExpectationViolation, match=r"valid_id.*\"id\":null"):
        evaluate(bad, RULES, "flow", "ds")


def test_merge_metrics_accumulates():
    a = {"dropped_records": 1, "expectations": [{"name": "r", "passed_records": 3, "failed_records": 1}]}
    b = {"dropped_records": 2, "expectations": [{"name": "r", "passed_records": 5, "failed_records": 2}]}
    m = merge_metrics(merge_metrics(None, a), b)
    assert m["dropped_records"] == 3 and m["expectations"][0]["failed_records"] == 3
