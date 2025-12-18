from __future__ import annotations

from datetime import date

import pandas as pd


def _coerce_trade_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    return parsed.dt.date.astype("string")


def fetch_hist_one(*, symbol: str, start_date: str, end_date: str, **_: object) -> pd.DataFrame:
    """Fetch one symbol history for backfill.

    Returns a DataFrame that must include:
      - `trade_date` (YYYY-MM-DD)
      - `symbol`
    """

    if not symbol:
        raise ValueError("symbol is required")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    import akshare as ak  # imported here for Airflow worker image friendliness

    df = ak.fund_etf_hist_sina(symbol=symbol)
    if df is None or df.empty:
        return pd.DataFrame(columns=["trade_date", "symbol"])

    rename_map = {
        "日期": "trade_date",
        "date": "trade_date",
    }
    df = df.rename(columns=rename_map)
    if "trade_date" not in df.columns:
        raise ValueError("akshare returned data without a trade date column")

    df["trade_date"] = _coerce_trade_date(df["trade_date"])
    df = df.dropna(subset=["trade_date"])

    df["symbol"] = symbol
    df = df[(df["trade_date"] >= start.isoformat()) & (df["trade_date"] <= end.isoformat())]
    df = df.sort_values(by=["trade_date"]).reset_index(drop=True)
    return df
