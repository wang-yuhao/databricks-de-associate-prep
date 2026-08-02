"""Vectorized backtester: turns model predictions into a simple long/flat trading
strategy and measures what would have happened.

IMPORTANT -- read this before you trust any number this file prints:
  - This runs on the model's HELD-OUT test predictions only (never on data the
    model was trained on), so at least there's no in-sample overfitting baked into
    the results directly.
  - It still cannot rule out overfitting to the *test* period itself if you tune
    hyperparameters by repeatedly looking at these backtest numbers and adjusting
    -- that just moves the overfitting one level up. Use a separate, never-touched
    holdout period (or walk-forward validation) before you'd trust this for real
    capital.
  - Transaction costs and a fixed confidence threshold are modeled, but real slippage,
    partial fills, market impact, and borrow costs for shorts are NOT modeled here.
  - Past performance in a backtest, especially on a few years of one regime, is not
    a reliable predictor of future returns. Treat every number below as "how this
    specific rule would have performed on this specific historical sample," not as
    a promise about the future.
  - This is a research/education tool. It is not financial advice, and nothing here
    should be read as a recommendation to trade any specific security.
"""
import argparse

import numpy as np
import pandas as pd

from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark

TRADING_DAYS_PER_YEAR = 252


def run_backtest(pdf: pd.DataFrame, cfg: dict) -> dict:
    """pdf must have: timestamp, target_return_next (actual next-day return),
    pred_proba_up (model's probability the next day is up).
    Long-only, single-symbol: go long when confidence exceeds the threshold,
    otherwise stay in cash. This is deliberately simple -- it's meant as a
    reference scaffold to extend, not a production strategy.
    """
    bt_cfg = cfg["backtest"]
    pdf = pdf.sort_values("timestamp").reset_index(drop=True).copy()

    pdf["signal"] = (pdf["pred_proba_up"] > bt_cfg["min_confidence"]).astype(int)
    pdf["position_change"] = pdf["signal"].diff().fillna(pdf["signal"].iloc[0]).abs()

    cost_frac = bt_cfg["transaction_cost_bps"] / 10_000
    pdf["strategy_return"] = (
        pdf["signal"] * pdf["target_return_next"] * bt_cfg["position_size_pct"]
        - pdf["position_change"] * cost_frac * bt_cfg["position_size_pct"]
    )
    pdf["buy_hold_return"] = pdf["target_return_next"]

    pdf["strategy_equity"] = bt_cfg["initial_capital"] * (1 + pdf["strategy_return"]).cumprod()
    pdf["buy_hold_equity"] = bt_cfg["initial_capital"] * (1 + pdf["buy_hold_return"]).cumprod()

    metrics = {
        "strategy": _performance_metrics(pdf["strategy_return"], pdf["strategy_equity"]),
        "buy_and_hold": _performance_metrics(pdf["buy_hold_return"], pdf["buy_hold_equity"]),
        "n_trades": int(pdf["position_change"].sum()),
        "pct_time_in_market": float(pdf["signal"].mean() * 100),
    }
    return {"equity_curve": pdf, "metrics": metrics}


def _performance_metrics(returns: pd.Series, equity: pd.Series) -> dict:
    n = len(returns)
    if n == 0 or equity.iloc[-1] <= 0:
        return {"total_return_pct": 0.0, "cagr_pct": 0.0, "sharpe": 0.0, "max_drawdown_pct": 0.0, "win_rate_pct": 0.0}

    total_return = equity.iloc[-1] / equity.iloc[0] - 1
    years = n / TRADING_DAYS_PER_YEAR
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1 if years > 0 else 0.0

    ret_std = returns.std()
    sharpe = (returns.mean() / ret_std) * np.sqrt(TRADING_DAYS_PER_YEAR) if ret_std > 0 else 0.0

    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    max_drawdown = drawdown.min()

    nonzero = returns[returns != 0]
    win_rate = (nonzero > 0).mean() * 100 if len(nonzero) > 0 else 0.0

    return {
        "total_return_pct": round(total_return * 100, 2),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 2),
        "max_drawdown_pct": round(max_drawdown * 100, 2),
        "win_rate_pct": round(win_rate, 2),
    }


def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)
    gold_root = resolve_path(cfg, "delta.gold_path")
    preds = spark.read.format("delta").load(f"{gold_root}/model_predictions").toPandas()

    all_metrics = {}
    for symbol in cfg["symbols"]:
        sym_preds = preds[preds["symbol"] == symbol]
        if len(sym_preds) < 10:
            print(f"[backtest:{symbol}] not enough test predictions, skipping")
            continue
        result = run_backtest(sym_preds, cfg)
        all_metrics[symbol] = result["metrics"]
        m = result["metrics"]
        print(f"\n=== {symbol} backtest (held-out test period, {len(sym_preds)} days) ===")
        print(f"  Strategy   : total return {m['strategy']['total_return_pct']:>7.2f}%  "
              f"CAGR {m['strategy']['cagr_pct']:>6.2f}%  Sharpe {m['strategy']['sharpe']:>5.2f}  "
              f"MaxDD {m['strategy']['max_drawdown_pct']:>6.2f}%  WinRate {m['strategy']['win_rate_pct']:>5.2f}%")
        print(f"  Buy & Hold : total return {m['buy_and_hold']['total_return_pct']:>7.2f}%  "
              f"CAGR {m['buy_and_hold']['cagr_pct']:>6.2f}%  Sharpe {m['buy_and_hold']['sharpe']:>5.2f}  "
              f"MaxDD {m['buy_and_hold']['max_drawdown_pct']:>6.2f}%")
        print(f"  Trades: {m['n_trades']}  Time in market: {m['pct_time_in_market']:.1f}%")

    print(
        "\nReminder: these numbers are from a short held-out historical window using a "
        "simple long/flat rule. They are not a projection of future returns and are not "
        "financial advice -- paper trade any strategy before ever risking real capital."
    )
    spark.stop()
    return all_metrics


if __name__ == "__main__":
    main()
