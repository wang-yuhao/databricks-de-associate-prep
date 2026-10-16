# SOLUTION - Dimensions: products (snapshot MV) and customers (SCD2 via AUTO CDC).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="products", comment="Product catalog.")
@dp.expect_or_drop("valid_price", "list_price > 0")
def products():
    return (spark.read.format("csv").option("header", True)
            .schema("product_id STRING, name STRING, category STRING, list_price DOUBLE")
            .load(spark.conf.get("products_source")))


@dp.table(name="customers_cdc", comment="Customer change events from the CRM.")
@dp.expect_or_drop("valid_operation", "operation IN ('INSERT', 'UPDATE', 'DELETE')")
def customers_cdc():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema("customer_id BIGINT, name STRING, email STRING, city STRING, region STRING, "
                    "tier STRING, operation STRING, sequence_num BIGINT")
            .load(spark.conf.get("customers_source")))


dp.create_streaming_table("customers", comment="Customer dimension with history (SCD type 2).")

dp.create_auto_cdc_flow(
    target="customers",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2,
    track_history_column_list=["city", "region", "tier"],
)
