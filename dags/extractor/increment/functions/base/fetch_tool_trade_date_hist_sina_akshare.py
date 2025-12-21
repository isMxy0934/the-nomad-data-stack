from dags.utils.extractor_utils import CsvPayload


def fetch_tool_trade_date_hist_sina_akshare() -> CsvPayload | None:
    import akshare as ak

    df = ak.tool_trade_date_hist_sina()
    if df is None or df.empty:
        return None

    target_columns = [
        "trade_date",
    ]

    df = df[[col for col in target_columns if col in df.columns]]

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
