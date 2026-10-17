# Fan-out: one Silver streaming table per topic, generated in a loop (metaprogramming).
# Locally each table publishes Iceberg metadata (UniForm) so other engines can read it as Iceberg.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# topic -> (payload schema, primary key used to detect unparseable payloads)
TOPICS = {
    "orders": ("order_id STRING, customer_id STRING, items INT, total DOUBLE, order_ts TIMESTAMP", "order_id"),
    "customers": ("customer_id STRING, name STRING, city STRING, region STRING, updated_ts TIMESTAMP",
                  "customer_id"),
    "clickstream": ("session_id STRING, page STRING, device STRING, event_ts TIMESTAMP", "session_id"),
}

UNIFORM_PROPERTIES = {
    "delta.enableIcebergCompatV2": "true",
    "delta.universalFormat.enabledFormats": "iceberg",
    "delta.enableDeletionVectors": "false",   # Iceberg compatibility requires deletion vectors off
}
# Databricks does not allow Iceberg reads on pipeline-managed tables: set enable_uniform=false there
# and publish an Iceberg-readable copy outside the pipeline (see databricks/1.2-multiplex-companion).
if spark.conf.get("enable_uniform", "true") != "true":
    UNIFORM_PROPERTIES = {}


def define_topic_table(topic: str, schema: str, key: str):
    @dp.table(name=f"silver_{topic}", comment=f"Parsed '{topic}' records from the multiplex feed.",
              table_properties=UNIFORM_PROPERTIES)
    @dp.expect_or_drop("valid_payload", f"{key} IS NOT NULL")
    def _topic():
        return (spark.readStream.table("bronze_multiplex")
                .filter(F.col("topic") == topic)
                .select(F.from_json("value", schema).alias("p"), "partition", "offset")
                .select("p.*", "partition", "offset"))


for _topic_name, (_schema, _key) in TOPICS.items():
    define_topic_table(_topic_name, _schema, _key)


# Records whose payload cannot be parsed are kept for auditing instead of being silently lost.
@dp.table(name="bronze_multiplex_quarantine", comment="Multiplex records with unparseable payloads.")
def bronze_multiplex_quarantine():
    parsed_ok = F.coalesce(*[F.when(F.col("topic") == t, F.from_json("value", s)[k].isNotNull())
                             for t, (s, k) in TOPICS.items()], F.lit(False))
    return spark.readStream.table("bronze_multiplex").filter(~parsed_ok)
