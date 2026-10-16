from pathlib import Path

from adelab import bundles
from adelab.notebook import split_sql
from adelab.privacy import k_anonymity, scan_pii, use_identity

BUNDLES = Path(__file__).resolve().parents[1] / "04-automated-deployment-dabs"


def test_split_sql_respects_quotes_and_comments():
    sql = "SELECT ';' AS a; -- comment; here\nSELECT 2 /* x; y */ ;\n\n"
    assert split_sql(sql) == ["SELECT ';' AS a", "SELECT 2"]


def test_bundle_preview_applies_targets_and_dev_mode():
    dev = bundles.preview(BUNDLES / "bundles" / "02_multi_env_dab", "dev", user="ana@x.com", show=False)
    job = dev["resources"]["jobs"]["orders_job"]
    assert job["name"] == "[dev ana] orders_job_dev"
    assert job["schedule"]["pause_status"] == "PAUSED"
    stage = bundles.preview(BUNDLES / "bundles" / "02_multi_env_dab", "stage", show=False)
    assert stage["resources"]["pipelines"]["orders_pipeline"]["schema"] == "orders_stage"
    over = bundles.preview(BUNDLES / "bundles" / "02_multi_env_dab", "dev", variables={"orders_per_run": 5},
                           show=False)
    assert over["resources"]["jobs"]["orders_job"]["tasks"][0]["notebook_task"]["base_parameters"]["rows"] == "5"


def test_all_demo_and_solution_bundles_match_cli_schema():
    if bundles.cli_path() is None:
        import pytest
        pytest.skip("Databricks CLI not installed")
    for d in list((BUNDLES / "bundles").iterdir()) + list((BUNDLES / "solutions").iterdir()):
        if (d / "databricks.yml").exists():
            assert bundles.validate_schema(d) == [], d.name


def test_identity_switch_changes_group_membership(spark):
    use_identity(spark, "eu_analyst")
    assert spark.sql("SELECT is_account_group_member('eu_analysts') AS m").first().m is True
    use_identity(spark, "contractor")
    assert spark.sql("SELECT is_account_group_member('eu_analysts') AS m").first().m is False


def test_scan_pii_and_k_anonymity(spark):
    df = spark.createDataFrame([("a@b.com", "123-45-6789", "10115", "F"), ("c@d.org", "987-65-4321", "10115", "F")],
                               "email string, ssn string, zip string, gender string")
    classes = {r.column: r.pii_class for r in scan_pii(df).collect()}
    assert classes["email"] == "email" and classes["ssn"] == "ssn" and classes["zip"] == "postal_code"
    assert k_anonymity(df, ["zip", "gender"]) == 2
