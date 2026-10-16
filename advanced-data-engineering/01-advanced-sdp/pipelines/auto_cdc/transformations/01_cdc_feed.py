# CDC feed from an operational database (one JSON file per extract).
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CDC_SCHEMA = ("customer_id BIGINT, name STRING, email STRING, city STRING, "
              "operation STRING, sequence_num BIGINT")


@dp.table(name="customers_cdc_bronze", comment="Raw CDC events, append-only.")
def customers_cdc_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(CDC_SCHEMA)
            .load(spark.conf.get("cdc_source"))
            .withColumn("source_file", F.col("_metadata.file_path")))


@dp.table(name="customers_cdc_clean", comment="Validated CDC events: the source for AUTO CDC.")
@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_operation", "operation IN ('INSERT', 'UPDATE', 'DELETE')")
@dp.expect_or_drop("valid_sequence", "sequence_num IS NOT NULL")
def customers_cdc_clean():
    return spark.readStream.table("customers_cdc_bronze").drop("source_file")
