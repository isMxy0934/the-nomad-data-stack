import sys
from pathlib import Path

import pytest
from freezegun import freeze_time

from dags.utils.time_utils import (
    get_current_date_str,
    get_current_partition_date_str,
    get_previous_date_str,
    get_previous_partition_date_str,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


class TestGetCurrentPartitionDateStr:
    """Test get_current_partition_date_str function."""

    @freeze_time("2024-12-17 10:30:00", tz_offset=8)
    def test_default_timezone_returns_correct_format(self):
        """Test with default timezone (Asia/Shanghai)."""
        result = get_current_partition_date_str()
        assert result == "2024-12-17"
        assert isinstance(result, str)
        assert len(result) == 10  # yyyy-MM-dd

    @freeze_time("2024-12-17 02:30:00", tz_offset=0)
    def test_custom_timezone_utc(self):
        """Test with UTC timezone."""
        result = get_current_partition_date_str("UTC")
        assert result == "2024-12-17"

    @freeze_time("2024-12-17 05:30:00", tz_offset=-5)
    def test_custom_timezone_tokyo(self):
        """Test with Tokyo timezone."""
        result = get_current_partition_date_str("Asia/Tokyo")
        assert result == "2024-12-17"


class TestGetCurrentDateStr:
    """Test get_current_date_str function."""

    @freeze_time("2024-12-17 10:30:00", tz_offset=8)
    def test_default_timezone_returns_compact_format(self):
        """Test with default timezone returns yyyyMMdd format."""
        result = get_current_date_str()
        assert result == "20241217"
        assert isinstance(result, str)
        assert len(result) == 8

    @freeze_time("2024-12-17 02:30:00", tz_offset=0)
    def test_custom_timezone_returns_compact_format(self):
        """Test with custom timezone returns yyyyMMdd format."""
        result = get_current_date_str("UTC")
        assert result == "20241217"


class TestGetPreviousDateStr:
    """Test get_previous_date_str function."""

    @freeze_time("2024-12-17 10:30:00", tz_offset=8)
    def test_returns_previous_day_compact_format(self):
        """Test returns previous day (16号) in yyyyMMdd format."""
        result = get_previous_date_str()
        assert result == "20241216"
        assert isinstance(result, str)
        assert len(result) == 8

    @freeze_time("2024-12-17 02:30:00", tz_offset=0)
    def test_custom_timezone_previous_day(self):
        """Test with UTC timezone returns previous day."""
        result = get_previous_date_str("UTC")
        assert result == "20241216"


class TestGetPreviousPartitionDateStr:
    """Test get_previous_partition_date_str function."""

    @freeze_time("2024-12-17 10:30:00", tz_offset=8)
    def test_returns_previous_day_hyphen_format(self):
        """Test returns previous day in yyyy-MM-dd format."""
        result = get_previous_partition_date_str()
        assert result == "2024-12-16"
        assert isinstance(result, str)
        assert len(result) == 10

    @freeze_time("2024-12-17 02:30:00", tz_offset=0)
    def test_custom_timezone_previous_day_hyphen_format(self):
        """Test with custom timezone returns previous day in hyphen format."""
        result = get_previous_partition_date_str("UTC")
        assert result == "2024-12-16"


class TestTimeUtilsIntegration:
    """Integration tests for time utilities."""

    @pytest.mark.parametrize(
        "tz,frozen_time,expected_partition,expected_compact",
        [
            ("Asia/Shanghai", "2024-12-17 10:30:00+08:00", "2024-12-17", "20241217"),
            ("UTC", "2024-12-17 02:30:00+00:00", "2024-12-17", "20241217"),
            ("America/New_York", "2024-12-17 05:30:00-05:00", "2024-12-17", "20241217"),
        ],
    )
    def test_timezone_consistency(self, tz, frozen_time, expected_partition, expected_compact):
        """Test that different timezone functions work consistently."""
        with freeze_time(frozen_time):
            partition_result = get_current_partition_date_str(tz)
            compact_result = get_current_date_str(tz)

            assert partition_result == expected_partition
            assert compact_result == expected_compact

    @freeze_time("2024-12-17 10:30:00", tz_offset=8)
    def test_previous_functions_consistency(self):
        """Test that previous day functions return consistent date formats."""
        compact_result = get_previous_date_str()
        partition_result = get_previous_partition_date_str()

        assert compact_result == "20241216"
        assert partition_result == "2024-12-16"
        assert len(compact_result) == 8  # yyyyMMdd
        assert len(partition_result) == 10  # yyyy-MM-dd
        # 验证日期逻辑正确（16号是17号的前一天）
        assert compact_result.endswith("1216")
        assert partition_result.endswith("-12-16")

    @freeze_time("2024-01-01 00:00:00", tz_offset=8)  # 元旦
    def test_year_boundary_previous_day(self):
        """Test previous day calculation across year boundary."""
        compact_result = get_previous_date_str()
        partition_result = get_previous_partition_date_str()

        assert compact_result == "20231231"  # 2023年12月31日
        assert partition_result == "2023-12-31"

    @freeze_time("2024-03-01 00:00:00", tz_offset=8)  # 3月1日
    def test_month_boundary_previous_day(self):
        """Test previous day calculation across month boundary."""
        compact_result = get_previous_date_str()
        partition_result = get_previous_partition_date_str()

        assert compact_result == "20240229"  # 2024年2月29日（闰年）
        assert partition_result == "2024-02-29"
