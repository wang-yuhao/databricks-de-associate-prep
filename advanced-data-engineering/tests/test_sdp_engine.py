"""End-to-end tests of the local Spark Declarative Pipelines engine."""
import json
import textwrap

import pytest

from adelab import datagen
from adelab.sdp import Pipeline, PipelineUpdateFailed


def write(path, code):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(code), encoding="utf-8")


def test_multi_flow_expectations_mv_and_full_refresh(spark, workdir, schema):
    src = workdir / "src"
    write(src / "p.py", """
        from pyspark import pipelines as dp
        from pyspark.sql import functions as F
        dp.create_streaming_table("bronze")

        @dp.append_flow(target="bronze")
        def a():
            return spark.readStream.schema("id INT, v INT").json(spark.conf.get("in_a")).withColumn("src", F.lit("a"))

        @dp.append_flow(target="bronze")
        def b():
            return spark.readStream.schema("id INT, v INT").json(spark.conf.get("in_b")).withColumn("src", F.lit("b"))

        @dp.table(cluster_by=["src"])
        @dp.expect_or_drop("positive", "v > 0")
        def silver():
            return spark.readStream.table("bronze")

        @dp.materialized_view
        def gold():
            return spark.read.table("silver").groupBy("src").count()
    """)
    for name, rows in (("in_a", [(1, 5), (2, -1)]), ("in_b", [(3, 7)])):
        (workdir / name).mkdir()
        (workdir / name / "f0.json").write_text("\n".join(json.dumps({"id": i, "v": v}) for i, v in rows))
    p = Pipeline(spark, name="t", source=src, schema=schema, storage=workdir / "storage",
                 configuration={"in_a": str(workdir / "in_a"), "in_b": str(workdir / "in_b")})
    res = p.run(quiet=True)
    assert res.state == "COMPLETED"
    assert spark.table(f"{schema}.bronze").count() == 3
    assert spark.table(f"{schema}.silver").count() == 2
    assert {r.src: r["count"] for r in spark.table(f"{schema}.gold").collect()} == {"a": 1, "b": 1}
    assert list(spark.sql(f"DESCRIBE DETAIL {schema}.silver").first()["clusteringColumns"]) == ["src"]
    dq = {r.expectation: r.failed_records for r in p.expectation_metrics().collect()}
    assert dq == {"positive": 1}

    # incremental: only the new file is read
    (workdir / "in_a" / "f1.json").write_text(json.dumps({"id": 4, "v": 1}))
    p.run(quiet=True)
    assert spark.table(f"{schema}.bronze").count() == 4

    p.run(full_refresh=True, quiet=True)
    assert spark.table(f"{schema}.bronze").count() == 4


def test_expect_or_fail_fails_update_and_skips_downstream(spark, workdir, schema):
    src = workdir / "src"
    write(src / "p.py", """
        from pyspark import pipelines as dp
        @dp.table
        @dp.expect_or_fail("has_id", "id IS NOT NULL")
        def t1():
            return spark.readStream.schema("id INT").json(spark.conf.get("inp"))
        @dp.materialized_view
        def t2():
            return spark.read.table("t1")
    """)
    (workdir / "inp").mkdir()
    (workdir / "inp" / "f.json").write_text('{"id": 1}\n{"id": null}')
    p = Pipeline(spark, name="f", source=src, schema=schema, storage=workdir / "s",
                 configuration={"inp": str(workdir / "inp")})
    with pytest.raises(PipelineUpdateFailed, match="has_id"):
        p.run(quiet=True)
    statuses = {f.flow: f.status for f in p.last_update.flows}
    assert statuses == {"t1": "FAILED", "t2": "SKIPPED"}
    assert p.event_log().filter("level = 'ERROR'").count() >= 1


def test_auto_cdc_scd1_and_scd2_handle_late_data_and_deletes(spark, workdir, schema):
    src = workdir / "src"
    write(src / "p.py", """
        from pyspark import pipelines as dp
        from pyspark.sql import functions as F
        @dp.temporary_view
        def feed():
            return (spark.readStream.schema("customer_id BIGINT, name STRING, email STRING, city STRING, "
                                            "operation STRING, sequence_num BIGINT").json(spark.conf.get("cdc")))
        for scd in (1, 2):
            dp.create_streaming_table(f"c{scd}")
            dp.create_auto_cdc_flow(target=f"c{scd}", source="feed", keys=["customer_id"],
                                    sequence_by=F.col("sequence_num"), apply_as_deletes="operation = 'DELETE'",
                                    except_column_list=["operation", "sequence_num"], stored_as_scd_type=scd,
                                    track_history_except_column_list=["email"] if scd == 2 else None)
    """)
    p = Pipeline(spark, name="cdc", source=src, schema=schema, storage=workdir / "s",
                 configuration={"cdc": str(workdir / "cdc")})
    for batch in range(3):
        datagen.land_customer_cdc(str(workdir / "cdc"), batch)
        p.run(quiet=True)

    scd1 = {r.customer_id: r.city for r in spark.table(f"{schema}.c1").collect()}
    assert scd1 == {1: "Porto", 2: "Busan", 4: "Brno", 5: "Zurich"}

    scd2 = [(r.customer_id, r.city, r["__START_AT"], r["__END_AT"])
            for r in spark.table(f"{schema}.c2").orderBy("customer_id", "__START_AT").collect()]
    assert scd2 == [
        (1, "Lisbon", 1, 2), (1, "Porto", 2, None),
        (2, "Seoul", 1, 3), (2, "Incheon", 3, 4), (2, "Busan", 4, None),
        (3, "Osaka", 1, 2), (3, "Kyoto", 2, 3),
        (4, "Prague", 1, 2), (4, "Brno", 2, None),
        (5, "Zurich", 1, None)]
    # email is not tracked: the seq-2 email change of customer 2 updated version 1 in place
    v1 = spark.table(f"{schema}.c2").filter("customer_id = 2 AND __START_AT = 1").first()
    assert v1.email == "ben.kim@example.com"


def test_autoloader_rescues_unknown_fields_and_bad_types(spark, workdir):
    from adelab.sdp import autoloader

    autoloader.install()
    inp = workdir / "in"
    inp.mkdir()
    (inp / "a.json").write_text('{"id": "1", "qty": "2"}\n')
    df = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", str(workdir / "schema"))
          .option("cloudFiles.schemaHints", "qty INT")
          .option("cloudFiles.schemaEvolutionMode", "rescue").load(str(inp)))
    assert df.columns == ["id", "qty", "_rescued_data"]
    (inp / "b.json").write_text('{"id": "2", "qty": "two", "coupon": "X"}\n')
    q = (df.writeStream.format("memory").queryName("al_test")
         .option("checkpointLocation", str(workdir / "cp")).trigger(availableNow=True).start())
    q.awaitTermination()
    rows = {r.id: r for r in spark.table("al_test").collect()}
    assert rows["1"].qty == 2 and rows["1"]._rescued_data is None
    rescued = json.loads(rows["2"]._rescued_data)
    assert rows["2"].qty is None and rescued["qty"] == "two" and rescued["coupon"] == "X"
    assert rescued["_file_path"].endswith("b.json")


def test_sink_keeps_data_on_full_refresh_and_reappends(spark, workdir, schema):
    src = workdir / "src"
    write(src / "p.py", """
        from pyspark import pipelines as dp
        @dp.table
        def t():
            return spark.readStream.schema("id INT").json(spark.conf.get("inp"))
        dp.create_sink("out", format="delta", options={"path": spark.conf.get("sink")})
        @dp.append_flow(target="out")
        def export():
            return spark.readStream.table("t")
    """)
    (workdir / "inp").mkdir()
    (workdir / "inp" / "f.json").write_text('{"id": 1}\n{"id": 2}')
    sink = workdir / "sink"
    p = Pipeline(spark, name="s", source=src, schema=schema, storage=workdir / "s",
                 configuration={"inp": str(workdir / "inp"), "sink": str(sink)})
    p.run(quiet=True)
    p.run(quiet=True)
    assert spark.read.format("delta").load(str(sink)).count() == 2      # exactly once across updates
    p.run(full_refresh=True, quiet=True)
    assert spark.table(f"{schema}.t").count() == 2                      # tables are recomputed
    assert spark.read.format("delta").load(str(sink)).count() == 4      # sinks are not cleaned up


def test_expectations_on_append_flow_are_rejected(spark, workdir, schema):
    src = workdir / "src"
    write(src / "p.py", """
        from pyspark import pipelines as dp
        dp.create_streaming_table("t")
        @dp.append_flow(target="t")
        @dp.expect_or_drop("x", "id > 0")
        def a():
            return spark.readStream.schema("id INT").json(spark.conf.get("inp"))
    """)
    p = Pipeline(spark, name="e", source=src, schema=schema, storage=workdir / "s", configuration={"inp": "x"})
    with pytest.raises(ValueError, match="create_streaming_table"):
        p.validate(show=False)
