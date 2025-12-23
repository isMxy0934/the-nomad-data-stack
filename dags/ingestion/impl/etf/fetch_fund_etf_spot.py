import pandas as pd

from lakehouse_core.io.time import get_date_str


def fetch_fund_etf_spot(**kwargs) -> pd.DataFrame | None:
    """
    Fetches real-time ETF spot data from AkShare.
    """
    import akshare as ak

    try:
        df = ak.fund_etf_spot_em()
    except Exception as e:
        print(f"Error fetching ETF spot data: {e}")
        raise
    if df is None or df.empty:
        return None

    rename_map = {
        "代码": "symbol",
        "名称": "name",
        "成交额": "amount",
    }

    df = df.rename(columns=rename_map)
    target_columns = [
        "symbol",
        "name",
        "amount",
    ]

    df = df[[col for col in target_columns if col in df.columns]]

    # Add ingestion date as trade_date for partitioning
    df["trade_date"] = get_date_str()

    return df
