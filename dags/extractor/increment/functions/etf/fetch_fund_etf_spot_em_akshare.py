from dags.utils.extractor_utils import CsvPayload
from lakehouse_core.io.time import get_date_str


def fetch_fund_etf_spot_em_akshare() -> CsvPayload | None:
    import akshare as ak

    df = ak.fund_etf_spot_em()
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
    df["trade_date"] = get_date_str()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
