# Silver: expectations + Liquid Clustering. A quarantine table keeps the rejected rows.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

VALID_STATUS = "status IN ('ACTIVE', 'INACTIVE', 'PENDING', 'ERROR')"

DROP_RULES = {
    "valid_timestamp": "event_ts IS NOT NULL",
    "valid_status": VALID_STATUS,
}

# Pipeline setting `event_id_policy`: "fail" (default) stops the update on a NULL event_id,
# "drop" removes such rows instead. Changing a setting + re-running is how you recover in the UI.
_event_id_rule = (dp.expect_or_fail if spark.conf.get("event_id_policy", "fail") == "fail"
                  else dp.expect_or_drop)("valid_event_id", "event_id IS NOT NULL")


@dp.table(
    name="silver_events",
    comment="Validated device events, clustered for the most common filters.",
    cluster_by=["status", "ingest_date"],
)
@dp.expect("non_negative_amount", "amount >= 0")                 # warn: keep row, record metric
@dp.expect_all_or_drop(DROP_RULES)                               # drop: remove row, record metric
@_event_id_rule                                                  # fail (or drop): see setting above
def silver_events():
    return (spark.readStream.table("bronze_events")
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .withColumn("ingest_date", F.to_date("event_ts"))
            .select("event_id", "device_id", "status", "amount", "event_ts", "ingest_date", "source_flow"))


# Quarantine = the inverse of the drop rules, so no record is silently lost.
@dp.table(name="silver_quarantine", comment="Rows rejected by silver_events drop rules.")
def silver_quarantine():
    failed = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name)) for name, rule in DROP_RULES.items()]
    return (spark.readStream.table("bronze_events")
            .withColumn("failed_expectations", F.array_compact(F.array(*failed)))
            .filter(F.size("failed_expectations") > 0))
