# LAB - Building a Multi-Source E-commerce Pipeline with SDP
# Step 1: ingest two order sources (EU web shop CSV, US mobile app JSON) into ONE Bronze table.
#
# Target schema of orders_bronze (both flows must produce exactly these columns):
#   order_id STRING, customer_id BIGINT, product_id STRING, quantity INT, unit_price DOUBLE,
#   order_ts TIMESTAMP, source_region STRING ('EU' | 'US'), source_file STRING
#
# Source columns:
#   EU CSV  (header): order_id, customer_id, product_id, qty, unit_price, order_ts   (all strings!)
#   US JSON         : orderId, custId, sku, quantity, price, ts
# Hints: Spark 4 runs in ANSI mode, so use try_cast / try_to_timestamp for dirty strings.
#        The source paths are in spark.conf: orders_eu_source, orders_us_source.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# TODO 1.1: declare the target streaming table "orders_bronze", clustered by source_region.


@dp.append_flow(target="orders_bronze", name="orders_eu_flow")
def orders_eu_flow():
    # TODO 1.2: read the EU CSV files with Auto Loader and map them to the target schema.
    raise NotImplementedError("TODO 1.2: orders_eu_flow")


@dp.append_flow(target="orders_bronze", name="orders_us_flow")
def orders_us_flow():
    # TODO 1.3: read the US JSON files with Auto Loader and map them to the target schema.
    raise NotImplementedError("TODO 1.3: orders_us_flow")
