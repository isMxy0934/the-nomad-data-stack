from dags.utils.extractor_utils import CsvPayload
from lakehouse_core.time import get_date_str


def fetch_fund_price_akshare() -> CsvPayload | None:
    import akshare as ak

    df = ak.fund_etf_category_sina(symbol="ETF基金")
    if df is None or df.empty:
        return None

    rename_map = {
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
    target_columns = [
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
    df = df[[col for col in target_columns if col in df.columns]]
    df["trade_date"] = get_date_str()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
