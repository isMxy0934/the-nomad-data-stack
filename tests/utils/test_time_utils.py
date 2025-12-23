import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.utils.freezegun_utils import freeze_time
    from lakehouse_core.io.time import (
        get_date_str,
        get_partition_date_str,
        normalize_to_partition_format,
    )
except ImportError as exc:
    pytest.skip(
        f"time utils imports unavailable in this environment: {exc}", allow_module_level=True
    )


class TestGetPartitionDateStr:
    """Test get_partition_date_str function."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_default_timezone_returns_previous_day(self):
        """Test with default timezone (Asia/Shanghai) returns T-1."""
        result = get_partition_date_str()
        assert result == "2024-12-16"
        assert isinstance(result, str)
        assert len(result) == 10  # yyyy-MM-dd


class TestGetDateStr:
    """Test get_date_str function."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_default_timezone_returns_previous_day_compact_format(self):
        """Test with default timezone returns T-1 in yyyyMMdd format."""
        result = get_date_str()
        assert result == "20241216"
        assert isinstance(result, str)
        assert len(result) == 8  # yyyyMMdd


class TestNormalizeToPartitionFormat:
    """Test normalize_to_partition_format function."""

    def test_accepts_date_object(self):
        """Test with date object."""
        from datetime import date

        result = normalize_to_partition_format(date(2024, 12, 17))
        assert result == "2024-12-17"

    def test_accepts_datetime_object(self):
        """Test with datetime object."""
        from datetime import datetime

        result = normalize_to_partition_format(datetime(2024, 12, 17, 10, 30, 0))
        assert result == "2024-12-17"

    def test_accepts_yyyy_mm_dd_string(self):
        """Test with YYYY-MM-DD string."""
        result = normalize_to_partition_format("2024-12-17")
        assert result == "2024-12-17"

    def test_converts_yyyymmdd_string(self):
        """Test conversion from YYYYMMDD to YYYY-MM-DD."""
        result = normalize_to_partition_format("20241217")
        assert result == "2024-12-17"

    def test_raises_error_on_invalid_string(self):
        """Test that invalid date string raises ValueError."""
        with pytest.raises(ValueError):
            normalize_to_partition_format("invalid-date")

    def test_raises_error_on_unsupported_type(self):
        """Test that unsupported type raises TypeError."""
        with pytest.raises(TypeError):
            normalize_to_partition_format(12345)


class TestTimeUtilsIntegration:
    """Integration tests for time utilities."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_functions_consistency(self):
        """Test that functions return consistent date formats."""
        partition_result = get_partition_date_str()
        compact_result = get_date_str()

        assert partition_result == "2024-12-16"
        assert compact_result == "20241216"
        assert len(compact_result) == 8  # yyyyMMdd
        assert len(partition_result) == 10  # yyyy-MM-dd

    @freeze_time("2024-01-01 00:00:00+08:00")
    def test_year_boundary_previous_day(self):
        """Test previous day calculation across year boundary."""
        partition_result = get_partition_date_str()
        compact_result = get_date_str()

        assert partition_result == "2023-12-31"
        assert compact_result == "20231231"

    @freeze_time("2024-03-01 00:00:00+08:00")
    def test_month_boundary_previous_day(self):
        """Test previous day calculation across month boundary."""
        partition_result = get_partition_date_str()
        compact_result = get_date_str()

        assert partition_result == "2024-02-29"
        assert compact_result == "20240229"
