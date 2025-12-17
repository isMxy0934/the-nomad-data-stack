import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from freezegun import freeze_time

from dags.utils.time_utils import (
    get_current_date_str,
    get_current_partition_date_str,
    get_previous_date_str,
    get_previous_partition_date_str,
)


class TestGetCurrentPartitionDateStr:
    """Test get_current_partition_date_str function."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_default_timezone_returns_correct_format(self):
        """Test with default timezone (Asia/Shanghai)."""
        result = get_current_partition_date_str()
        assert result == "2024-12-17"
        assert isinstance(result, str)
        assert len(result) == 10  # yyyy-MM-dd


class TestGetCurrentDateStr:
    """Test get_current_date_str function."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_default_timezone_returns_compact_format(self):
        """Test with default timezone returns yyyyMMdd format."""
        result = get_current_date_str()
        assert result == "20241217"
        assert isinstance(result, str)
        assert len(result) == 8  # yyyyMMdd


class TestGetPreviousDateStr:
    """Test get_previous_date_str function."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_returns_previous_day_compact_format(self):
        """Test returns previous day in yyyyMMdd format."""
        result = get_previous_date_str()
        assert result == "20241216"
        assert isinstance(result, str)
        assert len(result) == 8


class TestGetPreviousPartitionDateStr:
    """Test get_previous_partition_date_str function."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_returns_previous_day_hyphen_format(self):
        """Test returns previous day in yyyy-MM-dd format."""
        result = get_previous_partition_date_str()
        assert result == "2024-12-16"
        assert isinstance(result, str)
        assert len(result) == 10


class TestTimeUtilsIntegration:
    """Integration tests for time utilities."""

    @freeze_time("2024-12-17 10:30:00+08:00")
    def test_previous_functions_consistency(self):
        """Test that previous day functions return consistent date formats."""
        compact_result = get_previous_date_str()
        partition_result = get_previous_partition_date_str()

        assert compact_result == "20241216"
        assert partition_result == "2024-12-16"
        assert len(compact_result) == 8  # yyyyMMdd
        assert len(partition_result) == 10  # yyyy-MM-dd
        assert compact_result.endswith("1216")
        assert partition_result.endswith("-12-16")

    @freeze_time("2024-01-01 00:00:00+08:00")
    def test_year_boundary_previous_day(self):
        """Test previous day calculation across year boundary."""
        compact_result = get_previous_date_str()
        partition_result = get_previous_partition_date_str()

        assert compact_result == "20231231"
        assert partition_result == "2023-12-31"

    @freeze_time("2024-03-01 00:00:00+08:00")
    def test_month_boundary_previous_day(self):
        """Test previous day calculation across month boundary."""
        compact_result = get_previous_date_str()
        partition_result = get_previous_partition_date_str()

        assert compact_result == "20240229"
        assert partition_result == "2024-02-29"
