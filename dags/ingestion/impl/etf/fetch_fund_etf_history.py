import pandas as pd


def fetch_fund_etf_history(**kwargs) -> pd.DataFrame | None:
    """
    Fetches ETF history data (Mock implementation for now).
    """
    symbol = kwargs.get("symbol")
    # print(f"Fetching history for symbol: {symbol}")

    # Mock Data
    df = pd.DataFrame(
        {
            "symbol": [symbol],
            "name": [symbol],
            "trade_date": ["2021-01-01"], # Mock date
            "close": [10.0],
            "open": [9.0],
            "high": [11.0],
            "low": [8.0],
        }
    )

    if symbol is not None:
        df = df[df["symbol"] == str(symbol).strip()]

    return df
