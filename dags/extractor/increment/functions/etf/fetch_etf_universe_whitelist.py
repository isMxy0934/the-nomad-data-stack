import os

import pandas as pd

from dags.utils.extractor_utils import CsvPayload
from lakehouse_core.io.time import get_date_str

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
WHITELIST_FILE = os.path.join(CURRENT_DIR, "etf_universe_whitelist.csv")


def fetch_etf_universe_whitelist() -> CsvPayload | None:
    df = pd.read_csv(WHITELIST_FILE, encoding="utf-8")
    if df is None or df.empty:
        return None

    df["trade_date"] = get_date_str()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return CsvPayload(csv_bytes=csv_bytes, record_count=len(df))
