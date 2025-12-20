from __future__ import annotations

from dags.utils.extractor_utils import CsvPayload
from lakehouse_core.time import get_date_str


def fetch_fund_universe_akshare() -> CsvPayload | None:
    import akshare as ak

    df = ak.fund_etf_category_sina(symbol="ETF基金")
    if df is None or df.empty:
        return None

    rename_map = {
        "代码": "symbol",
        "名称": "name",
        "代码\xa0": "symbol",
        "名称\xa0": "name",
        "浠ｇ爜": "symbol",
        "鍚嶇О": "name",
    }
    df = df.rename(columns=rename_map)

    if "symbol" not in df.columns:
        raise ValueError("akshare fund_etf_category_sina result missing symbol column")

    keep_cols: list[str] = ["symbol"]
    if "name" in df.columns:
        keep_cols.append("name")
    df = df[keep_cols].copy()
    df["trade_date"] = get_date_str()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
