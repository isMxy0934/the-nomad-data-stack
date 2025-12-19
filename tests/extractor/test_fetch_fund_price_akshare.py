import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.extractor.functions.fetch_fund_price_akshare import fetch_fund_price_akshare
except ImportError as exc:
    pytest.skip(
        f"extractor functions imports unavailable in this environment: {exc}", allow_module_level=True
    )


def test_fetch_fund_price_akshare_success():
    # Mock data returned by akshare
    mock_df = pd.DataFrame({
        "代码": ["510300"],
        "名称": ["300ETF"],
        "最新价": [4.0],
        "开盘价": [3.9],
        "最高价": [4.1],
        "最低价": [3.8],
        "成交量": [1000],
        "成交额": [4000],
    })

    mock_ak = MagicMock()
    mock_ak.fund_etf_category_sina.return_value = mock_df
    
    with patch.dict("sys.modules", {"akshare": mock_ak}), \
         patch("dags.extractor.functions.fetch_fund_price_akshare.get_date_str", return_value="20241219"):
        
        result = fetch_fund_price_akshare()
        
        assert result is not None
        assert result.record_count == 1
        
        # Verify CSV content
        import io
        df_result = pd.read_csv(io.BytesIO(result.csv_bytes))
        assert df_result["symbol"][0] == 510300 # pandas might parse it as int
        assert df_result["trade_date"][0] == 20241219


def test_fetch_fund_price_akshare_empty():
    mock_ak = MagicMock()
    mock_ak.fund_etf_category_sina.return_value = pd.DataFrame()
    with patch.dict("sys.modules", {"akshare": mock_ak}):
        result = fetch_fund_price_akshare()
        assert result is None
