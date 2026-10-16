# LAB Step 2: dimensions.
#   products   : materialized view over the products CSV (spark.conf products_source),
#                schema product_id STRING, name STRING, category STRING, list_price DOUBLE,
#                drop rows where list_price is not > 0.
#   customers  : SCD type 2 dimension built with AUTO CDC from the CRM change feed
#                (spark.conf customers_source, JSON with columns customer_id BIGINT, name, email,
#                city, region, tier, operation ('INSERT'|'UPDATE'|'DELETE'), sequence_num BIGINT).
#                Track history only for city, region and tier.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="products", comment="Product catalog.")
def products():
    # TODO 2.1
    raise NotImplementedError("TODO 2.1: products")


@dp.table(name="customers_cdc", comment="Customer change events from the CRM.")
def customers_cdc():
    # TODO 2.2: streaming read of the CDC files; drop rows with an invalid operation.
    raise NotImplementedError("TODO 2.2: customers_cdc")


# TODO 2.3: declare the "customers" streaming table and an AUTO CDC flow into it (SCD type 2).
