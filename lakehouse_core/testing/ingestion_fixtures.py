from __future__ import annotations

import pandas as pd


def sample_fetch(**kwargs) -> pd.DataFrame:
    """Return a small sample dataframe for ingestion tests."""
    rows = [
        {"symbol": "AAA", "trade_date": "20240101", "value": 1},
        {"symbol": "BBB", "trade_date": "20240102", "value": 2},
        {"symbol": "AAA", "trade_date": "20240101", "value": 3},
    ]
    df = pd.DataFrame(rows)

    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")
    if start_date or end_date:
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
        if start_date:
            df = df[df["trade_date"] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df["trade_date"] <= pd.Timestamp(end_date)]
        df["trade_date"] = df["trade_date"].dt.strftime("%Y%m%d")
    return df
