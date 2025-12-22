import os
import pandas as pd
from lakehouse_core.io.time import get_date_str

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
WHITELIST_FILE = os.path.join(CURRENT_DIR, "etf_universe_whitelist.csv")

def fetch_etf_universe_whitelist(**kwargs) -> pd.DataFrame | None:
    """
    Fetches the ETF whitelist from a local CSV file.
    Returns a DataFrame directly.
    """
    if not os.path.exists(WHITELIST_FILE):
        raise FileNotFoundError(f"Whitelist file not found: {WHITELIST_FILE}")
        
    df = pd.read_csv(WHITELIST_FILE, encoding="utf-8")
    if df is None or df.empty:
        return None

    # Ensure trade_date is present (Data-Driven Partitioning needs it)
    # Since this is a snapshot, we use today's date if not present
    if "trade_date" not in df.columns:
        df["trade_date"] = get_date_str()

    return df
