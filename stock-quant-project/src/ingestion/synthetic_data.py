"""Generates realistic, Alpaca-shaped synthetic data (bars/quotes/trades) so you can
run and validate the ENTIRE pipeline -- silver, gold, ML, backtest -- before pointing
anything at your real Postgres database. This is purely a local testing aid; it is not
used once you switch ingestion to --mode postgres.

Schemas mirror Alpaca's market data API responses:
  bars:   symbol, timestamp, open, high, low, close, volume, trade_count, vwap
  quotes: symbol, timestamp, bid_price, bid_size, ask_price, ask_size, bid_exchange, ask_exchange
  trades: symbol, timestamp, price, size, exchange, conditions
"""
import numpy as np
import pandas as pd


def _gbm_price_path(n, start_price, mu=0.08, sigma=0.30, seed=0):
    """Geometric Brownian motion daily close path -- good enough to produce
    realistic-looking OHLCV without needing real market data."""
    rng = np.random.default_rng(seed)
    dt = 1 / 252
    shocks = rng.normal((mu - 0.5 * sigma**2) * dt, sigma * np.sqrt(dt), n)
    log_path = np.cumsum(shocks)
    return start_price * np.exp(log_path)


def generate_synthetic_bars(symbols, years=3, seed=42) -> pd.DataFrame:
    n_days = int(years * 252)
    end = pd.Timestamp.today().normalize()
    dates = pd.bdate_range(end=end, periods=n_days)

    start_prices = {"AAPL": 190.0, "NVDA": 120.0, "TSLA": 250.0, "AMD": 140.0}
    rows = []
    for i, sym in enumerate(symbols):
        closes = _gbm_price_path(n_days, start_prices.get(sym, 100.0), seed=seed + i)
        rng = np.random.default_rng(seed + 100 + i)
        for j, (date, close) in enumerate(zip(dates, closes)):
            prev_close = closes[j - 1] if j > 0 else close
            open_px = prev_close * (1 + rng.normal(0, 0.004))
            intraday_range = abs(rng.normal(0, 0.012)) * close
            high = max(open_px, close) + intraday_range
            low = min(open_px, close) - intraday_range
            volume = int(rng.lognormal(mean=16.5, sigma=0.5))
            rows.append(
                {
                    "symbol": sym,
                    "timestamp": date,
                    "open": round(open_px, 4),
                    "high": round(high, 4),
                    "low": round(low, 4),
                    "close": round(close, 4),
                    "volume": volume,
                    "trade_count": int(volume / rng.integers(80, 200)),
                    "vwap": round((open_px + high + low + close) / 4, 4),
                }
            )

    # Deliberately inject a few bad rows so the quality/quarantine step has
    # something real to catch -- mirrors what you'll actually see in raw data.
    rng = np.random.default_rng(seed + 999)
    df = pd.DataFrame(rows)
    bad_idx = rng.choice(df.index, size=max(3, len(df) // 500), replace=False)
    for idx in bad_idx:
        choice = rng.integers(0, 3)
        if choice == 0:
            df.loc[idx, "high"] = df.loc[idx, "low"] - 1  # high < low, physically invalid
        elif choice == 1:
            df.loc[idx, "close"] = -abs(df.loc[idx, "close"])  # negative price
        else:
            df.loc[idx, "volume"] = -1  # negative volume

    return df


def generate_synthetic_quotes(symbols, bars_df: pd.DataFrame, per_day: int = 20, seed=7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for sym in symbols:
        sym_bars = bars_df[bars_df["symbol"] == sym]
        for _, bar in sym_bars.iterrows():
            mid_prices = np.linspace(bar["low"], bar["high"], per_day)
            for k, mid in enumerate(mid_prices):
                spread = max(0.01, mid * rng.uniform(0.0002, 0.0015))
                ts = bar["timestamp"] + pd.Timedelta(minutes=int(390 * k / per_day))
                rows.append(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "bid_price": round(mid - spread / 2, 4),
                        "bid_size": int(rng.integers(1, 50)) * 100,
                        "ask_price": round(mid + spread / 2, 4),
                        "ask_size": int(rng.integers(1, 50)) * 100,
                        "bid_exchange": "Q",
                        "ask_exchange": "Q",
                    }
                )
    return pd.DataFrame(rows)


def generate_synthetic_trades(symbols, bars_df: pd.DataFrame, per_day: int = 30, seed=13) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for sym in symbols:
        sym_bars = bars_df[bars_df["symbol"] == sym]
        for _, bar in sym_bars.iterrows():
                                            lo, hi = min(bar["low"], bar["high"]), max(bar["low"], bar["high"])
                if hi <= lo:
                    hi = lo + 0.01
                prices = rng.uniform(lo, hi, per_day)
            for k, px in enumerate(prices):
                ts = bar["timestamp"] + pd.Timedelta(minutes=int(390 * k / per_day))
                rows.append(
                    {
                        "symbol": sym,
                        "timestamp": ts,
                        "price": round(float(px), 4),
                        "size": int(rng.integers(1, 20)) * 10,
                        "exchange": "Q",
                        "conditions": "@",
                    }
                )
    return pd.DataFrame(rows)


def generate_all(symbols, years=3, seed=42):
    bars = generate_synthetic_bars(symbols, years=years, seed=seed)
    quotes = generate_synthetic_quotes(symbols, bars, seed=seed)
    trades = generate_synthetic_trades(symbols, bars, seed=seed)
    return bars, quotes, trades
