from dags.utils.extractor_utils import CsvPayload
from lakehouse_core.time import get_date_str


def fetch_fund_name_from_em_akshare() -> CsvPayload | None:
    import akshare as ak

    df = ak.fund_name_em()
    if df is None or df.empty:
        return None

    rename_map = {
        "基金代码": "symbol",
        "基金简称": "name",
        "基金类型": "fund_type",
        "拼音全称": "pinyin_full",
        "拼音缩写": "pinyin_short",
    }

    df = df.rename(columns=rename_map)
    target_columns = [
        "symbol",
        "name",
        "fund_type",
        "pinyin_full",
        "pinyin_short",
    ]

    df = df[[col for col in target_columns if col in df.columns]]
    df["trade_date"] = get_date_str()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
