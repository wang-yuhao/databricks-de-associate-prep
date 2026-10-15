"""
Local emulation of a multi-flow Spark Declarative Pipeline (SDP / DLT).

    source_flow_a ─┐  (append flow A)
                   ├──> bronze_events  (streaming table, many flows -> one target)
    source_flow_b ─┘
                          │  DataQualityManager: expect / expect_or_drop / expect_or_fail
                          ├──> silver_events      (Liquid Clustering: status, ingest_date)
                          └──> silver_quarantine  (rows that failed a drop rule)
                                   │
                                   └──> gold_daily_metrics  (materialized-view style aggregate)

Mapping to Databricks:
    @dp.table + @dp.append_flow  -> MultiFlowPipeline.run_bronze()  (one writeStream per flow)
    @dp.expect*                  -> DataQualityManager
    CLUSTER BY (...)             -> DeltaTable builder .clusterBy(...)
    @dp.materialized_view        -> MultiFlowPipeline.run_gold()   (batch overwrite)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("event_ts", T.StringType()),
])

VALID_STATUSES = ("ACTIVE", "INACTIVE", "PENDING", "ERROR")


# --------------------------------------------------------------------------- #
# Data quality (emulates SDP expectations)
# --------------------------------------------------------------------------- #
class ExpectationFailed(Exception):
    """Raised by an expect_or_fail rule -- equivalent to an SDP update failure."""


@dataclass
class Expectation:
    name: str
    condition: str          # SQL boolean expression, same syntax as @dp.expect
    action: str             # "warn" | "drop" | "fail"


@dataclass
class DataQualityManager:
    expectations: list[Expectation] = field(default_factory=list)
    metrics: list[dict] = field(default_factory=list)   # one entry per rule per micro-batch

    def expect(self, name: str, condition: str) -> "DataQualityManager":
        self.expectations.append(Expectation(name, condition, "warn"))
        return self

    def expect_or_drop(self, name: str, condition: str) -> "DataQualityManager":
        self.expectations.append(Expectation(name, condition, "drop"))
        return self

    def expect_or_fail(self, name: str, condition: str) -> "DataQualityManager":
        self.expectations.append(Expectation(name, condition, "fail"))
        return self

    @staticmethod
    def _passes(condition: str):
        # A rule that evaluates to NULL counts as a violation, like in SDP.
        return F.coalesce(F.expr(condition), F.lit(False))

    def apply(self, df: DataFrame, batch_id: int = 0) -> tuple[DataFrame, DataFrame]:
        """Returns (clean_df, quarantine_df). Raises ExpectationFailed on a fail rule."""
        flagged = df
        for e in self.expectations:
            flagged = flagged.withColumn(f"__dq_{e.name}", self._passes(e.condition))
        flagged = flagged.cache()

        counts = flagged.agg(*[
            F.sum(F.when(~F.col(f"__dq_{e.name}"), 1).otherwise(0)).alias(e.name)
            for e in self.expectations
        ]).first().asDict() if self.expectations else {}
        total = flagged.count()

        for e in self.expectations:
            failed = counts.get(e.name) or 0
            self.metrics.append({"batch_id": batch_id, "expectation": e.name, "action": e.action,
                                 "total": total, "failed": failed})
            if e.action == "fail" and failed > 0:
                flagged.unpersist()
                raise ExpectationFailed(
                    f"expect_or_fail '{e.name}' violated by {failed} row(s): {e.condition}")

        drop_rules = [e for e in self.expectations if e.action == "drop"]
        all_drop_pass = reduce(lambda a, b: a & b,
                               [F.col(f"__dq_{e.name}") for e in drop_rules], F.lit(True))
        failed_rules = F.array_compact(F.array(*[
            F.when(~F.col(f"__dq_{e.name}"), F.lit(e.name)) for e in drop_rules
        ])) if drop_rules else F.array().cast("array<string>")

        dq_cols = [f"__dq_{e.name}" for e in self.expectations]
        clean = flagged.filter(all_drop_pass).drop(*dq_cols)
        quarantine = (flagged.filter(~all_drop_pass)
                      .withColumn("failed_expectations", failed_rules)
                      .drop(*dq_cols))
        return clean, quarantine


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
class MultiFlowPipeline:
    CLUSTER_KEYS = ["status", "ingest_date"]

    def __init__(self, spark: SparkSession, root: str, flows: dict[str, str] | None = None):
        self.spark = spark
        self.root = root
        self.flows = flows or {
            "flow_a": os.path.join(root, "source_flow_a"),
            "flow_b": os.path.join(root, "source_flow_b"),
        }
        # Fully qualified file: URIs keep Delta from seeing "D:\x" and "file:/D:/x" as two
        # different locations on Windows (DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE).
        # Must match Hadoop's qualified form exactly: "file:/D:/..." or "file:/tmp/...".
        lake = lambda name: "file:/" + Path(root, "lake", name).resolve().as_posix().lstrip("/")
        self.bronze_path = lake("bronze_events")
        self.silver_path = lake("silver_events")
        self.quarantine_path = lake("silver_quarantine")
        self.gold_path = lake("gold_daily_metrics")
        self.checkpoints = os.path.join(root, "checkpoints")

        self.dq = (DataQualityManager()
                   .expect("non_negative_amount", "amount >= 0")
                   .expect_or_drop("valid_timestamp", "event_ts IS NOT NULL")
                   .expect_or_drop("valid_status", f"status IN {VALID_STATUSES}")
                   .expect_or_fail("valid_event_id", "event_id IS NOT NULL"))

    # ---- BRONZE: several append flows writing into ONE streaming table ----
    def run_bronze(self) -> None:
        for flow_name, src in self.flows.items():
            stream = (self.spark.readStream
                      .schema(SOURCE_SCHEMA)
                      .option("maxFilesPerTrigger", 1)
                      .json(src)
                      .withColumn("source_flow", F.lit(flow_name))
                      .withColumn("source_file", F.input_file_name())
                      .withColumn("ingested_at", F.current_timestamp()))
            (stream.writeStream
                   .format("delta")
                   .outputMode("append")
                   # Each flow owns its checkpoint -- exactly how SDP isolates append flows.
                   .option("checkpointLocation", os.path.join(self.checkpoints, f"bronze_{flow_name}"))
                   .trigger(availableNow=True)
                   .start(self.bronze_path)
                   .awaitTermination())

    # ---- SILVER: expectations + Liquid Clustered target ----
    def _create_silver_table(self) -> None:
        # Spark 3.5's DataFrameWriter has no clusterBy(); .option("clusterBy", ...) is silently
        # ignored by Delta. The supported OSS route is the DeltaTable builder (Delta 3.2+),
        # equivalent to: CREATE TABLE ... USING DELTA CLUSTER BY (status, ingest_date)
        (DeltaTable.createIfNotExists(self.spark)
            .location(self.silver_path)
            .addColumn("event_id", "STRING")
            .addColumn("device_id", "STRING")
            .addColumn("status", "STRING")
            .addColumn("amount", "DOUBLE")
            .addColumn("event_ts", "TIMESTAMP")
            .addColumn("ingest_date", "DATE")
            .addColumn("source_flow", "STRING")
            .clusterBy(*self.CLUSTER_KEYS)
            .execute())

    def _silver_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        clean, quarantine = self.dq.apply(batch_df, batch_id)
        (clean.withColumn("event_ts", F.to_timestamp("event_ts"))
              .withColumn("ingest_date", F.to_date("event_ts"))
              .select("event_id", "device_id", "status", "amount", "event_ts",
                      "ingest_date", "source_flow")
              .write.format("delta").mode("append").save(self.silver_path))
        quarantine.write.format("delta").mode("append").save(self.quarantine_path)

    def run_silver(self) -> None:
        self._create_silver_table()
        (self.spark.readStream.format("delta").load(self.bronze_path)
            .writeStream
            .foreachBatch(self._silver_batch)
            .option("checkpointLocation", os.path.join(self.checkpoints, "silver"))
            .trigger(availableNow=True)
            .start()
            .awaitTermination())
        # OPTIMIZE on a clustered table performs incremental clustering by the cluster keys.
        DeltaTable.forPath(self.spark, self.silver_path).optimize().executeCompaction()

    # ---- GOLD: materialized-view style full recompute ----
    def run_gold(self) -> DataFrame:
        gold = (self.spark.read.format("delta").load(self.silver_path)
                .groupBy("ingest_date", "status")
                .agg(F.count("*").alias("event_count"),
                     F.countDistinct("device_id").alias("distinct_devices"),
                     F.round(F.sum("amount"), 2).alias("total_amount"),
                     F.round(F.avg("amount"), 2).alias("avg_amount")))
        gold.write.format("delta").mode("overwrite").save(self.gold_path)
        return self.spark.read.format("delta").load(self.gold_path)

    def run(self) -> DataFrame:
        self.run_bronze()
        self.run_silver()
        return self.run_gold()
