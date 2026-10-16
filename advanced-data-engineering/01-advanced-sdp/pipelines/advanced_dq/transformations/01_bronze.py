# Bronze with schema drift monitoring: unexpected fields and bad types land in _rescued_data.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="orders_bronze", comment="US app orders as delivered; schema drift goes to _rescued_data.")
@dp.expect("no_rescued_data", "_rescued_data IS NULL")   # warn only: we want to SEE drift, not lose rows
def orders_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaHints", "custId BIGINT, quantity INT, price DOUBLE")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(spark.conf.get("orders_source"))
            .withColumn("source_file", F.col("_metadata.file_path")))


@dp.materialized_view(name="products", comment="Product catalog snapshot.")
def products():
    return (spark.read.format("csv").option("header", True)
            .schema("product_id STRING, name STRING, category STRING, list_price DOUBLE")
            .load(spark.conf.get("products_source")))
