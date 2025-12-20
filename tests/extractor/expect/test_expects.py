import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.extractor.expect.functions.expects import dedupe_trade_date_symbol, identity
except ImportError as exc:
    pytest.skip(
        f"expect functions imports unavailable in this environment: {exc}", allow_module_level=True
    )


def test_identity():
    df = pd.DataFrame({"a": [1, 2]})
    result = identity(df, trade_date="2024-01-01", spec=None)
    pd.testing.assert_frame_equal(df, result)


def test_dedupe_trade_date_symbol():
    df = pd.DataFrame(
        {
            "trade_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "symbol": ["A", "A", "B"],
            "value": [1, 2, 3],
        }
    )
    result = dedupe_trade_date_symbol(df, trade_date="2024-01-01", spec=None)

    assert len(result) == 2
    # Should keep last (value 2 for A)
    assert result[(result["symbol"] == "A")]["value"].values[0] == 2
    assert result[(result["symbol"] == "B")]["value"].values[0] == 3


def test_dedupe_trade_date_symbol_empty():
    df = pd.DataFrame()
    result = dedupe_trade_date_symbol(df, trade_date="2024-01-01", spec=None)
    assert result.empty


def test_dedupe_trade_date_symbol_missing_cols():
    df = pd.DataFrame({"a": [1]})
    result = dedupe_trade_date_symbol(df, trade_date="2024-01-01", spec=None)
    pd.testing.assert_frame_equal(df, result)
