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
        "代码": "symbol",
        "名称": "name",
        "最新价": "close",
        "开盘价": "open",
        "开盘": "open",
        "最高价": "high",
        "最高": "high",
        "最低价": "low",
        "最低": "low",
        "成交量": "vol",
        "成交额": "amount",
        "昨收": "pre_close",
        "换手率": "turnover_rate",
        "涨跌幅": "pct_chg",
    }
    df = df.rename(columns=rename_map)
    if "trade_date" not in df.columns:
        raise ValueError("akshare returned data without a trade date column")

    # Align columns with daily extractor (dags/extractor/functions/fetch_fund_price_akshare.py)
    target_columns = [
        "trade_date",
        "symbol",
        "name",
        "close",
        "open",
        "high",
        "low",
        "vol",
        "amount",
        "pre_close",
        "pct_chg",
    ]
    # Filter columns that exist in the dataframe
    df = df[[col for col in target_columns if col in df.columns]]

    df["trade_date"] = _coerce_trade_date(df["trade_date"])
    df = df.dropna(subset=["trade_date"])

    df["symbol"] = symbol
    df = df[(df["trade_date"] >= start.isoformat()) & (df["trade_date"] <= end.isoformat())]
    df = df.sort_values(by=["trade_date"]).reset_index(drop=True)
    return df
