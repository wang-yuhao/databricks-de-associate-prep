import numpy as np
import pandas as pd

from src.ml.feature_engineering import FEATURE_COLUMNS, TARGET_COLUMNS, _compute_features_pdf


def _sample_bars(n=60, seed=0):
    rng = np.random.default_rng(seed)
    prices = 100 + np.cumsum(rng.normal(0, 1, n))
    dates = pd.bdate_range("2026-01-01", periods=n)
    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * n,
            "timestamp": dates,
            "open": prices,
            "high": prices + 1,
            "low": prices - 1,
            "close": prices,
            "volume": rng.integers(1_000_000, 5_000_000, n),
        }
    )


def test_output_has_all_expected_columns():
    out = _compute_features_pdf(_sample_bars())
    for col in FEATURE_COLUMNS + TARGET_COLUMNS:
        assert col in out.columns


def test_no_lookahead_in_feature_columns():
    """Feature columns must be computable from data up to and including `t` --
    shuffling everything AFTER row t must not change row t's feature values.
    This is the concrete check for lookahead bias.
    """
    bars = _sample_bars(n=80)
    out_full = _compute_features_pdf(bars.copy())

    cutoff = 50
    truncated = bars.iloc[: cutoff + 1].copy()
    out_truncated = _compute_features_pdf(truncated)

    row_full = out_full.iloc[cutoff]
    row_trunc = out_truncated.iloc[cutoff]

    for col in ["sma_5", "sma_10", "sma_20", "ema_12", "rsi_14", "bb_mid"]:
        a, b = row_full[col], row_trunc[col]
        if pd.isna(a) and pd.isna(b):
            continue
        assert abs(a - b) < 1e-9, f"{col} differs between full/truncated series -- possible lookahead bias"


def test_target_return_is_next_day_not_current_day():
    bars = _sample_bars(n=10)
    out = _compute_features_pdf(bars)
    expected = bars["close"].iloc[1] / bars["close"].iloc[0] - 1
    assert abs(out["target_return_next"].iloc[0] - expected) < 1e-9
    assert pd.isna(out["target_return_next"].iloc[-1])  # last row has no "next day"


def test_rsi_bounded_0_100():
    out = _compute_features_pdf(_sample_bars(n=100))
    valid = out["rsi_14"].dropna()
    assert (valid >= 0).all() and (valid <= 100).all()
