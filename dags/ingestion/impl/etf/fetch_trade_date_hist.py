import pandas as pd


def fetch_trade_date_hist(**kwargs) -> pd.DataFrame | None:
    """
    Fetches trading calendar history from Sina Finance via AkShare.

    Returns a DataFrame containing all historical trading dates.
    No parameters required - returns complete calendar from 1990-12-19 onwards.
    """
    import akshare as ak

    try:
        df = ak.tool_trade_date_hist_sina()
    except Exception as e:
        print(f"Error fetching trade date history: {e}")
        raise

    if df is None or df.empty:
        return None

    # API returns 'trade_date' column in YYYY-MM-DD format
    if "trade_date" not in df.columns:
        raise ValueError("API response missing expected 'trade_date' column")

    df = df[["trade_date"]]
    return df
