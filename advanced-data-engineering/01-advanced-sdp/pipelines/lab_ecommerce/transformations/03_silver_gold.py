# LAB Step 3: Silver with data quality + quarantine, and Gold business views.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

ORDER_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    # TODO 3.1: add rules: customer_id present, quantity > 0, unit_price > 0, order_ts present.
    #           Write each rule so it is true or false, never NULL (try_cast turned "N/A" into NULL):
    #           e.g. "quantity IS NOT NULL AND quantity > 0".
}


# TODO 3.2: orders_silver - streaming table clustered by (order_date, source_region) that DROPS rows
#           breaking ORDER_RULES and adds order_date = to_date(order_ts), amount = quantity * unit_price.


# TODO 3.3: orders_quarantine - every bronze row that breaks at least one rule, with a
#           failed_rules ARRAY<STRING> column listing the rule names it broke.


# TODO 3.4: revenue_by_category_daily - MV: orders, revenue per (order_date, category).


# TODO 3.5: revenue_by_customer_tier - MV: distinct customers and revenue per CURRENT customer tier
#           (current = the open SCD2 version, __END_AT IS NULL). Unknown tier -> 'unknown'.
