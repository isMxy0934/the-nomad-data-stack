import pandas as pd


def fetch_fund_etf_history(**kwargs) -> pd.DataFrame | None:
    """
    Fetches ETF historical data from AkShare.

    Args:
        symbol: ETF symbol/code
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    """
    import akshare as ak

    symbol = kwargs.get("symbol")
    start_date = kwargs.get("start_date")
    end_date = kwargs.get("end_date")

    if not symbol:
        return None

    try:
        # Convert date format from YYYY-MM-DD to YYYYMMDD for akshare API
        start_date_fmt = start_date.replace("-", "") if start_date else None
        end_date_fmt = end_date.replace("-", "") if end_date else None

        # Call akshare API
        df = ak.fund_etf_hist_em(symbol=symbol, start_date=start_date_fmt, end_date=end_date_fmt)

        if df is None or df.empty:
            return None

        # Rename columns from Chinese to English
        rename_map = {
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "涨跌幅": "change_percent",
            "涨跌额": "change_amount",
            "换手率": "turnover_rate",
        }

        df = df.rename(columns=rename_map)

        # Ensure symbol column exists
        df["symbol"] = symbol

        # Convert trade_date to proper format if needed
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

        # Select and reorder columns
        target_columns = [
            "symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "amplitude",
            "change_percent",
            "change_amount",
            "turnover_rate",
        ]

        # Only keep columns that exist
        existing_columns = [col for col in target_columns if col in df.columns]
        df = df[existing_columns]

        # Convert trade_date to proper format if needed
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime(
                "%Y%m%d"
            )

        return df

    except Exception as e:
        print(f"Error fetching ETF history for {symbol}: {e}")
        raise
