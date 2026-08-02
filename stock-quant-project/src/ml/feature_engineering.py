"""Technical indicator feature engineering, implemented as a grouped Pandas UDF
(`applyInPandas`) keyed by symbol.

Why a Pandas UDF instead of pure Spark window functions: several of these indicators
(true EMA, Wilder's RSI smoothing) are recursive -- each value depends on the previous
computed value, not just a fixed window of raw inputs. That's awkward to express in
Spark SQL window functions but trivial in pandas. This is also directly relevant exam
practice: Section 1 explicitly calls out "Develop User-Defined Functions (UDFs) using
Pandas/Python UDF" as an objective.

IMPORTANT (lookahead-bias note): every feature column here is computed using only
CURRENT and PAST rows (rolling/ewm/shift with positive lag). The two `target_*` columns
use shift(-1), i.e. FUTURE data -- those are labels for supervised learning, never
features. train_model.py explicitly drops any column starting with `target_` before
building the feature matrix.
"""
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

BASE_COLUMNS = ["symbol", "timestamp", "open", "high", "low", "close", "volume"]

FEATURE_COLUMNS = [
    "return_1d", "log_return_1d",
    "sma_5", "sma_10", "sma_20",
    "ema_12", "ema_26", "macd", "macd_signal",
    "rsi_14",
    "bb_mid", "bb_upper", "bb_lower", "bb_pct_b",
    "volatility_10d", "volatility_20d",
    "volume_zscore_20d",
    "close_lag_1", "close_lag_2", "close_lag_3",
    "return_lag_1", "return_lag_2", "return_lag_3",
]

TARGET_COLUMNS = ["target_return_next", "target_up_next"]

OUTPUT_SCHEMA = StructType(
    [StructField("symbol", StringType()), StructField("timestamp", TimestampType())]
    + [StructField(c, DoubleType()) for c in ["open", "high", "low", "close", "volume"]]
    + [StructField(c, DoubleType()) for c in FEATURE_COLUMNS]
    + [StructField("target_return_next", DoubleType()), StructField("target_up_next", IntegerType())]
)


def _compute_features_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)

    pdf["return_1d"] = pdf["close"].pct_change()
    pdf["log_return_1d"] = np.log(pdf["close"]).diff()

    for w in (5, 10, 20):
        pdf[f"sma_{w}"] = pdf["close"].rolling(w).mean()

    pdf["ema_12"] = pdf["close"].ewm(span=12, adjust=False).mean()
    pdf["ema_26"] = pdf["close"].ewm(span=26, adjust=False).mean()
    pdf["macd"] = pdf["ema_12"] - pdf["ema_26"]
    pdf["macd_signal"] = pdf["macd"].ewm(span=9, adjust=False).mean()

    delta = pdf["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    pdf["rsi_14"] = 100 - (100 / (1 + rs))

    pdf["bb_mid"] = pdf["close"].rolling(20).mean()
    bb_std = pdf["close"].rolling(20).std()
    pdf["bb_upper"] = pdf["bb_mid"] + 2 * bb_std
    pdf["bb_lower"] = pdf["bb_mid"] - 2 * bb_std
    pdf["bb_pct_b"] = (pdf["close"] - pdf["bb_lower"]) / (pdf["bb_upper"] - pdf["bb_lower"])

    pdf["volatility_10d"] = pdf["return_1d"].rolling(10).std()
    pdf["volatility_20d"] = pdf["return_1d"].rolling(20).std()

    vol_mean_20 = pdf["volume"].rolling(20).mean()
    vol_std_20 = pdf["volume"].rolling(20).std()
    pdf["volume_zscore_20d"] = (pdf["volume"] - vol_mean_20) / vol_std_20

    for lag in (1, 2, 3):
        pdf[f"close_lag_{lag}"] = pdf["close"].shift(lag)
        pdf[f"return_lag_{lag}"] = pdf["return_1d"].shift(lag)

    # Labels -- future data, NEVER to be used as a feature.
    pdf["target_return_next"] = pdf["close"].shift(-1) / pdf["close"] - 1
    pdf["target_up_next"] = (pdf["target_return_next"] > 0).astype("Int64")

    keep = ["symbol", "timestamp", "open", "high", "low", "close", "volume"] + FEATURE_COLUMNS + TARGET_COLUMNS
    return pdf[keep]


def add_technical_indicators(spark, silver_bars: DataFrame) -> DataFrame:
    """silver_bars must have at least: symbol, timestamp, open, high, low, close, volume.

    Implementation note: this collects to the driver and computes indicators with
    plain pandas (grouped by symbol) rather than using Spark's `applyInPandas`.
    `applyInPandas`/`toPandas()` under Arrow serialization hit a well-known
    reflection incompatibility on Java 17+/21 (see spark_session.py), so routing
    through plain (non-Arrow) collection keeps this project running the same on
    any JDK. For a few symbols x a few years of bars this is a trivial amount of
    data for a single machine; if you're processing many more symbols or
    minute-level bars across a long history, do this per-symbol in batches, or
    switch to `groupBy("symbol").applyInPandas(...)` once you've confirmed Arrow
    works in your environment (flip spark.sql.execution.arrow.pyspark.enabled to
    "true" in get_spark() first).
    """
    pdf = silver_bars.select(*BASE_COLUMNS).toPandas()

    results = [
        _compute_features_pdf(group)
        for _, group in pdf.groupby("symbol", group_keys=False)
    ]
    out_pdf = pd.concat(results, ignore_index=True) if results else pdf

    # Arrow disabled -> createDataFrame does strict type verification against the
    # schema, so numpy int64 volumes / ints need to be real Python floats first.
    float_cols = ["open", "high", "low", "close", "volume"] + FEATURE_COLUMNS + ["target_return_next"]
    for c in float_cols:
        out_pdf[c] = out_pdf[c].astype(float)

    out_pdf["target_up_next"] = out_pdf["target_up_next"].astype("object").where(out_pdf["target_up_next"].notna(), None)

    return spark.createDataFrame(out_pdf, schema=OUTPUT_SCHEMA)
