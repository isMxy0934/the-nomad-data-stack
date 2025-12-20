from dags.utils.extractor_utils import CsvPayload
from lakehouse_core.io.time import get_date_str


def fetch_stock_price_akshare() -> CsvPayload | None:
    import akshare as ak

    target_date_str = get_date_str()
    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        return None

    rename_map = {
        "代码": "symbol",
        "名称": "name",
        "最新价": "close",
        "开盘": "open",
        "今开": "open",
        "最高": "high",
        "最低": "low",
        "成交量": "vol",
        "成交额": "amount",
        "昨收": "pre_close",
        "换手率": "turnover_rate",
        "涨跌幅": "pct_chg",
        "市盈率-动态": "pe_ttm",
        "市净率": "pb",
    }

    available_cols = [c for c in rename_map.keys() if c in df.columns]
    df = df[available_cols].rename(columns=rename_map)
    df["trade_date"] = target_date_str

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
