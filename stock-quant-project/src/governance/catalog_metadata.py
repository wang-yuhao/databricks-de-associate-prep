"""Data Governance + a taste of Security & Compliance, done the way you'd have to
without Unity Catalog.

On Databricks, Unity Catalog gives you native row filters and column masks you attach
to a table with `ALTER TABLE ... SET ROW FILTER ...` / `ALTER TABLE ... ALTER COLUMN
... SET MASK ...`, enforced automatically for every reader regardless of which query
engine they use. Running on open-source Spark with a local Hive-style catalog, there's
no such enforcement layer -- the closest you can get is a masked VIEW that every
consumer is expected to query instead of the base table. That's a meaningfully weaker
guarantee (nothing stops someone with file-system access from reading the base Delta
files directly), which is exactly the kind of trade-off the exam is testing you on:
know what Unity Catalog buys you that you don't get for free elsewhere.

The masking pattern below (CASE WHEN based on a session-level "role" flag) mirrors the
structure of the exact pattern in Databricks' own public sample exam question for this
section, just implemented as a plain SQL view instead of a native column mask.
"""
import argparse

from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark


def add_table_comment(spark, catalog_table: str, comment: str):
    spark.sql(f"COMMENT ON TABLE {catalog_table} IS '{comment}'")


def add_column_comment(spark, catalog_table: str, column: str, comment: str):
    spark.sql(f"ALTER TABLE {catalog_table} ALTER COLUMN {column} COMMENT '{comment}'")


def create_masked_gold_view(spark, gold_table_path: str, viewer_role_session_var: str = "analyst"):
    """Creates gold.features_masked, a view that only exposes microstructure fields
    (avg_spread, avg_bid_size, etc.) to roles other than 'quant_researcher'.
    This is a stand-in for a Unity Catalog column mask -- the SQL pattern
    (`CASE WHEN is_member('some_group') THEN col ELSE NULL END`) is the same idea
    Databricks' own column-masking exam sample question uses, just with a session
    variable playing the role Unity Catalog's `is_member()` / `current_user()` would.
    """
    spark.sql(f"SET viewer.role = {viewer_role_session_var}")
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW gold.features_masked AS
        SELECT
          symbol,
          timestamp,
          open, high, low, close, volume,
          sma_5, sma_10, sma_20, ema_12, ema_26, macd, macd_signal, rsi_14,
          bb_mid, bb_upper, bb_lower, bb_pct_b,
          volatility_10d, volatility_20d, volume_zscore_20d,
          CASE WHEN current_setting('viewer.role', true) = 'quant_researcher'
               THEN avg_spread ELSE NULL END AS avg_spread,
          CASE WHEN current_setting('viewer.role', true) = 'quant_researcher'
               THEN avg_spread_pct ELSE NULL END AS avg_spread_pct
        FROM delta.`{gold_table_path}`
        """
    )
    print("[governance] created gold.features_masked "
          "(microstructure columns null unless viewer.role = quant_researcher)")


def apply_retention_purge(spark, table_path: str, cutoff_date: str):
    """Deletes rows older than cutoff_date -- the `DELETE` half of a retention policy.
    Pair with VACUUM (src/optimization/optimize_tables.py) to actually reclaim the
    underlying files once the deleted rows fall outside Delta's time-travel window.
    """
    spark.sql(f"DELETE FROM delta.`{table_path}` WHERE date < '{cutoff_date}'")
    print(f"[governance] purged rows with date < {cutoff_date} from {table_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=["comment", "mask-view"], required=True)
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)
    gold_root = resolve_path(cfg, "delta.gold_path")

    if args.action == "comment":
        add_table_comment(
            spark, "gold.features",
            "Daily technical-indicator + microstructure features for AAPL/NVDA/TSLA/AMD, "
            "derived from silver bars/quotes/trades. Owner: quant-practice-project.",
        )
        add_column_comment(spark, "gold.features", "target_up_next",
                            "Label only -- next-day up/down. Never use as a model input.")
    elif args.action == "mask-view":
        create_masked_gold_view(spark, f"{gold_root}/features")

    spark.stop()


if __name__ == "__main__":
    main()
