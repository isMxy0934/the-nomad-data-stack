from dags.utils.extractor_utils import CsvPayload
import pandas as pd

def fetch_fund_etf_history_em_akshare(**kwargs: object) -> CsvPayload | None:

    # import akshare as ak

    symbol = kwargs.get("symbol")
    print(f"symbol: {symbol}")

    df = pd.DataFrame({
        "symbol": [symbol],
        "name": [symbol],
        "trade_date": ["2021-01-01"],
        "close": [10.0],
        "open": [9.0],
        "high": [11.0],
        "low": [8.0],
    })

    symbol = kwargs.get("symbol")
    if symbol is not None:
        df = df[df["symbol"] == str(symbol).strip()]

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
