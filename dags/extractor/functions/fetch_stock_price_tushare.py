import os

from dags.utils.extractor_utils import CsvPayload
from dags.utils.time_utils import get_date_str


def fetch_stock_price_tushare() -> CsvPayload | None:
    import tushare as ts

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise ValueError("Missing required env var: TUSHARE_TOKEN")
    ts.set_token(token)
    pro = ts.pro_api()

    df = pro.daily(trade_date=get_date_str())
    if df is None or df.empty:
        return None

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
