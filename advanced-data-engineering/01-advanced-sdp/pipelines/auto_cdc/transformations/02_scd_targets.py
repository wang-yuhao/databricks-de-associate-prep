# AUTO CDC INTO (formerly APPLY CHANGES INTO): the engine orders events by sequence_num,
# handles late/out-of-order data and deletes, and maintains SCD type 1 or type 2 tables.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---- SCD Type 1: current state only (upsert + delete)
dp.create_streaming_table("customers_scd1", comment="Current customer records (SCD type 1).")

dp.create_auto_cdc_flow(
    target="customers_scd1",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=1,
)

# ---- SCD Type 2: full history with __START_AT / __END_AT.
# Only changes to name/city open a new version; an email-only change updates the current row in place.
dp.create_streaming_table("customers_scd2", comment="Customer history (SCD type 2).")

dp.create_auto_cdc_flow(
    target="customers_scd2",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num"],
    stored_as_scd_type=2,
    track_history_except_column_list=["email"],
)


@dp.materialized_view(name="customers_current", comment="Open SCD2 versions = current customers.")
def customers_current():
    return spark.read.table("customers_scd2").filter("__END_AT IS NULL").drop("__START_AT", "__END_AT")
