import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from dags.extractor.increment.functions.fetch_fund_name_from_em_akshare import (
    fetch_fund_name_from_em_akshare,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


def test_fetch_fund_name_from_em_akshare_success():
    # Mock data returned by akshare
    mock_df = pd.DataFrame(
        {
            "基金代码": ["510300"],
            "基金简称": ["300ETF"],
            "基金类型": ["ETF"],
            "拼音全称": ["Shenzhen 300 Exchange Traded Fund"],
            "拼音缩写": ["SZ300ETF"],
        }
    )

    mock_ak = MagicMock()
    mock_ak.fund_name_em.return_value = mock_df

    with (
        patch.dict("sys.modules", {"akshare": mock_ak}),
        patch(
            "dags.extractor.increment.functions.fetch_fund_name_from_em_akshare.get_date_str",
            return_value="20241219",
        ),
    ):
        result = fetch_fund_name_from_em_akshare()

        assert result is not None
        assert result.record_count == 1

        # Verify CSV content
        import io

        df_result = pd.read_csv(io.BytesIO(result.csv_bytes))
        assert df_result["symbol"][0] == 510300
        assert df_result["name"][0] == "300ETF"
        assert df_result["fund_type"][0] == "ETF"
        assert df_result["pinyin_full"][0] == "Shenzhen 300 Exchange Traded Fund"
        assert df_result["pinyin_short"][0] == "SZ300ETF"


def test_fetch_fund_name_from_em_akshare_empty():
    mock_ak = MagicMock()
    mock_ak.fund_name_em.return_value = pd.DataFrame()
    with patch.dict("sys.modules", {"akshare": mock_ak}):
        result = fetch_fund_name_from_em_akshare()
        assert result is None
