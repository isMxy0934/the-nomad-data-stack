"""Unit tests for backfill DAGs."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest

from dags.dw_dags_backfill import (
    _generate_dates,
    _target_matches,
    backfill_table_range,
)


class TestGenerateDates:
    """Tests for _generate_dates helper function."""

    def test_single_date(self):
        """Test generating a single date."""
        dates = _generate_dates("2024-12-01", "2024-12-01")
        assert dates == ["2024-12-01"]

    def test_multiple_dates(self):
        """Test generating multiple consecutive dates."""
        dates = _generate_dates("2024-12-01", "2024-12-05")
        assert dates == [
            "2024-12-01",
            "2024-12-02",
            "2024-12-03",
            "2024-12-04",
            "2024-12-05",
        ]
        assert len(dates) == 5

    def test_date_range_across_month(self):
        """Test date range crossing month boundary."""
        dates = _generate_dates("2024-01-30", "2024-02-02")
        assert dates == [
            "2024-01-30",
            "2024-01-31",
            "2024-02-01",
            "2024-02-02",
        ]

    def test_leap_year(self):
        """Test date generation in leap year."""
        dates = _generate_dates("2024-02-28", "2024-03-01")
        assert dates == [
            "2024-02-28",
            "2024-02-29",  # Leap year
            "2024-03-01",
        ]


class TestTargetMatches:
    """Tests for _target_matches helper function."""

    def test_match_layer_only(self):
        """Test matching by layer name only."""
        run_spec = {"layer": "ods", "table": "fund_etf_spot"}
        assert _target_matches(run_spec, "ods") is True
        assert _target_matches(run_spec, "dwd") is False

    def test_match_full_name(self):
        """Test matching by full table name (layer.table)."""
        run_spec = {"layer": "ods", "table": "fund_etf_spot"}
        assert _target_matches(run_spec, "ods.fund_etf_spot") is True
        assert _target_matches(run_spec, "ods.other_table") is False
        assert _target_matches(run_spec, "dwd.fund_etf_spot") is False

    def test_empty_target(self):
        """Test empty or invalid target strings."""
        run_spec = {"layer": "ods", "table": "fund_etf_spot"}
        assert _target_matches(run_spec, "") is False
        assert _target_matches(run_spec, None) is False
        assert _target_matches(run_spec, "   ") is False


class TestBackfillTableRange:
    """Tests for backfill_table_range function."""

    @pytest.fixture
    def mock_context(self):
        """Mock Airflow context."""
        return {
            "run_id": "manual__2024-12-01T00:00:00",
            "dag_run": Mock(conf={}),
        }

    @pytest.fixture
    def run_spec(self):
        """Sample run spec for testing."""
        return {
            "layer": "ods",
            "table": "fund_etf_spot",
            "name": "ods.fund_etf_spot",
            "base_prefix": "lake/ods/fund_etf_spot",
            "is_partitioned": True,
        }

    def test_skip_table_not_in_targets(self, mock_context, run_spec):
        """Test that tables not in targets are skipped."""
        mock_context["dag_run"].conf = {"targets": ["ods.other_table"]}

        result = backfill_table_range(
            run_spec=run_spec,
            start_date="2024-12-01",
            end_date="2024-12-03",
            **mock_context,
        )

        assert result["skipped"] is True
        assert result["total_dates"] == 3
        assert result["skipped_count"] == 3
        assert result["successful_count"] == 0
        assert result["failed_count"] == 0

    @patch("dags.dw_dags_backfill.S3Hook")
    @patch("dags.dw_dags_backfill.build_s3_connection_config")
    @patch("dags.dw_dags_backfill.temporary_connection")
    @patch("dags.dw_dags_backfill.configure_s3_access")
    @patch("dags.dw_dags_backfill._attach_catalog_if_available")
    @patch("dags.dw_dags_backfill.prepare_dataset")
    @patch("dags.dw_dags_backfill.pipeline_load")
    @patch("dags.dw_dags_backfill.validate_dataset")
    @patch("dags.dw_dags_backfill.commit_dataset")
    @patch("dags.dw_dags_backfill.cleanup_dataset")
    def test_successful_backfill_single_date(
        self,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_load,
        mock_prepare,
        mock_attach_catalog,
        mock_configure_s3,
        mock_temp_conn,
        mock_s3_config,
        mock_s3_hook,
        mock_context,
        run_spec,
    ):
        """Test successful backfill of a single date."""
        # Setup mocks
        mock_prepare.return_value = {
            "tmp_prefix": "lake/ods/fund_etf_spot/_tmp/run_123",
            "tmp_partition_prefix": "lake/ods/fund_etf_spot/_tmp/run_123/dt=2024-12-01",
            "canonical_prefix": "lake/ods/fund_etf_spot",
            "manifest_path": "lake/ods/fund_etf_spot/dt=2024-12-01/manifest.json",
            "success_flag_path": "lake/ods/fund_etf_spot/dt=2024-12-01/_SUCCESS",
            "partition_date": "2024-12-01",
            "partitioned": True,
        }
        mock_load.return_value = {"row_count": 100, "file_count": 1}

        mock_conn = MagicMock()
        mock_temp_conn.return_value.__enter__.return_value = mock_conn

        result = backfill_table_range(
            run_spec=run_spec,
            start_date="2024-12-01",
            end_date="2024-12-01",
            **mock_context,
        )

        # Verify results
        assert result["total_dates"] == 1
        assert result["successful_count"] == 1
        assert result["failed_count"] == 0
        assert result["first_success"] == "2024-12-01"
        assert result["last_success"] == "2024-12-01"
        assert result["failed_dates"] == []

        # Verify functions were called
        mock_prepare.assert_called_once()
        mock_load.assert_called_once()
        mock_validate.assert_called_once()
        mock_commit.assert_called_once()
        mock_cleanup.assert_called_once()

    @patch("dags.dw_dags_backfill.S3Hook")
    @patch("dags.dw_dags_backfill.build_s3_connection_config")
    @patch("dags.dw_dags_backfill.temporary_connection")
    @patch("dags.dw_dags_backfill.configure_s3_access")
    @patch("dags.dw_dags_backfill._attach_catalog_if_available")
    @patch("dags.dw_dags_backfill.prepare_dataset")
    @patch("dags.dw_dags_backfill.pipeline_load")
    @patch("dags.dw_dags_backfill.validate_dataset")
    @patch("dags.dw_dags_backfill.commit_dataset")
    @patch("dags.dw_dags_backfill.cleanup_dataset")
    def test_successful_backfill_multiple_dates(
        self,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_load,
        mock_prepare,
        mock_attach_catalog,
        mock_configure_s3,
        mock_temp_conn,
        mock_s3_config,
        mock_s3_hook,
        mock_context,
        run_spec,
    ):
        """Test successful backfill of multiple dates with connection reuse."""
        # Setup mocks
        mock_prepare.side_effect = lambda **kwargs: {
            "tmp_prefix": "lake/ods/fund_etf_spot/_tmp/run_123",
            "tmp_partition_prefix": f"lake/ods/fund_etf_spot/_tmp/run_123/dt={kwargs['partition_date']}",
            "canonical_prefix": "lake/ods/fund_etf_spot",
            "manifest_path": f"lake/ods/fund_etf_spot/dt={kwargs['partition_date']}/manifest.json",
            "success_flag_path": f"lake/ods/fund_etf_spot/dt={kwargs['partition_date']}/_SUCCESS",
            "partition_date": kwargs["partition_date"],
            "partitioned": True,
        }
        mock_load.return_value = {"row_count": 100, "file_count": 1}

        mock_conn = MagicMock()
        mock_temp_conn.return_value.__enter__.return_value = mock_conn

        result = backfill_table_range(
            run_spec=run_spec,
            start_date="2024-12-01",
            end_date="2024-12-03",
            **mock_context,
        )

        # Verify results
        assert result["total_dates"] == 3
        assert result["successful_count"] == 3
        assert result["failed_count"] == 0
        assert result["first_success"] == "2024-12-01"
        assert result["last_success"] == "2024-12-03"
        assert result["failed_dates"] == []

        # Verify connection was created only once (reuse optimization)
        mock_temp_conn.assert_called_once()

        # Verify each date was processed
        assert mock_prepare.call_count == 3
        assert mock_load.call_count == 3
        assert mock_cleanup.call_count == 3

    @patch("dags.dw_dags_backfill.S3Hook")
    @patch("dags.dw_dags_backfill.build_s3_connection_config")
    @patch("dags.dw_dags_backfill.temporary_connection")
    @patch("dags.dw_dags_backfill.configure_s3_access")
    @patch("dags.dw_dags_backfill._attach_catalog_if_available")
    @patch("dags.dw_dags_backfill.prepare_dataset")
    @patch("dags.dw_dags_backfill.pipeline_load")
    @patch("dags.dw_dags_backfill.validate_dataset")
    @patch("dags.dw_dags_backfill.commit_dataset")
    @patch("dags.dw_dags_backfill.cleanup_dataset")
    def test_failure_with_continue_on_error_false(
        self,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_load,
        mock_prepare,
        mock_attach_catalog,
        mock_configure_s3,
        mock_temp_conn,
        mock_s3_config,
        mock_s3_hook,
        mock_context,
        run_spec,
    ):
        """Test that backfill stops immediately on error when continue_on_error=False."""
        # Setup mocks: first date succeeds, second fails
        mock_prepare.side_effect = [
            {
                "tmp_prefix": "lake/ods/fund_etf_spot/_tmp/run_123",
                "tmp_partition_prefix": "lake/ods/fund_etf_spot/_tmp/run_123/dt=2024-12-01",
                "canonical_prefix": "lake/ods/fund_etf_spot",
                "manifest_path": "lake/ods/fund_etf_spot/dt=2024-12-01/manifest.json",
                "success_flag_path": "lake/ods/fund_etf_spot/dt=2024-12-01/_SUCCESS",
                "partition_date": "2024-12-01",
                "partitioned": True,
            },
            {
                "tmp_prefix": "lake/ods/fund_etf_spot/_tmp/run_123",
                "tmp_partition_prefix": "lake/ods/fund_etf_spot/_tmp/run_123/dt=2024-12-02",
                "canonical_prefix": "lake/ods/fund_etf_spot",
                "manifest_path": "lake/ods/fund_etf_spot/dt=2024-12-02/manifest.json",
                "success_flag_path": "lake/ods/fund_etf_spot/dt=2024-12-02/_SUCCESS",
                "partition_date": "2024-12-02",
                "partitioned": True,
            },
        ]
        mock_load.side_effect = [
            {"row_count": 100, "file_count": 1},
            Exception("Load failed"),
        ]

        mock_conn = MagicMock()
        mock_temp_conn.return_value.__enter__.return_value = mock_conn

        with pytest.raises(RuntimeError, match="Backfill terminated"):
            backfill_table_range(
                run_spec=run_spec,
                start_date="2024-12-01",
                end_date="2024-12-03",
                continue_on_error=False,  # Should stop on first error
                **mock_context,
            )

        # Verify only 2 dates were attempted (stopped after second failed)
        assert mock_prepare.call_count == 2
        assert mock_load.call_count == 2

    @patch("dags.dw_dags_backfill.S3Hook")
    @patch("dags.dw_dags_backfill.build_s3_connection_config")
    @patch("dags.dw_dags_backfill.temporary_connection")
    @patch("dags.dw_dags_backfill.configure_s3_access")
    @patch("dags.dw_dags_backfill._attach_catalog_if_available")
    @patch("dags.dw_dags_backfill.prepare_dataset")
    @patch("dags.dw_dags_backfill.pipeline_load")
    @patch("dags.dw_dags_backfill.validate_dataset")
    @patch("dags.dw_dags_backfill.commit_dataset")
    @patch("dags.dw_dags_backfill.cleanup_dataset")
    def test_failure_with_continue_on_error_true(
        self,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_load,
        mock_prepare,
        mock_attach_catalog,
        mock_configure_s3,
        mock_temp_conn,
        mock_s3_config,
        mock_s3_hook,
        mock_context,
        run_spec,
    ):
        """Test that backfill continues on error when continue_on_error=True."""
        # Setup mocks: first succeeds, second fails, third succeeds
        mock_prepare.side_effect = lambda **kwargs: {
            "tmp_prefix": "lake/ods/fund_etf_spot/_tmp/run_123",
            "tmp_partition_prefix": f"lake/ods/fund_etf_spot/_tmp/run_123/dt={kwargs['partition_date']}",
            "canonical_prefix": "lake/ods/fund_etf_spot",
            "manifest_path": f"lake/ods/fund_etf_spot/dt={kwargs['partition_date']}/manifest.json",
            "success_flag_path": f"lake/ods/fund_etf_spot/dt={kwargs['partition_date']}/_SUCCESS",
            "partition_date": kwargs["partition_date"],
            "partitioned": True,
        }
        mock_load.side_effect = [
            {"row_count": 100, "file_count": 1},
            Exception("Load failed for 2024-12-02"),
            {"row_count": 100, "file_count": 1},
        ]

        mock_conn = MagicMock()
        mock_temp_conn.return_value.__enter__.return_value = mock_conn

        result = backfill_table_range(
            run_spec=run_spec,
            start_date="2024-12-01",
            end_date="2024-12-03",
            continue_on_error=True,  # Should continue after error
            **mock_context,
        )

        # Verify all 3 dates were attempted
        assert result["total_dates"] == 3
        assert result["successful_count"] == 2
        assert result["failed_count"] == 1
        assert result["first_success"] == "2024-12-01"
        assert result["last_success"] == "2024-12-03"
        assert len(result["failed_dates"]) == 1
        assert result["failed_dates"][0]["date"] == "2024-12-02"
        assert "Load failed" in result["failed_dates"][0]["error"]

        # Verify all dates were processed
        assert mock_prepare.call_count == 3
        assert mock_load.call_count == 3

    @patch("dags.dw_dags_backfill.S3Hook")
    @patch("dags.dw_dags_backfill.build_s3_connection_config")
    @patch("dags.dw_dags_backfill.temporary_connection")
    @patch("dags.dw_dags_backfill.configure_s3_access")
    @patch("dags.dw_dags_backfill._attach_catalog_if_available")
    @patch("dags.dw_dags_backfill.prepare_dataset")
    @patch("dags.dw_dags_backfill.pipeline_load")
    @patch("dags.dw_dags_backfill.cleanup_dataset")
    def test_cleanup_always_runs_on_error(
        self,
        mock_cleanup,
        mock_load,
        mock_prepare,
        mock_attach_catalog,
        mock_configure_s3,
        mock_temp_conn,
        mock_s3_config,
        mock_s3_hook,
        mock_context,
        run_spec,
    ):
        """Test that cleanup runs even when load fails."""
        # Setup mocks
        mock_prepare.return_value = {
            "tmp_prefix": "lake/ods/fund_etf_spot/_tmp/run_123",
            "tmp_partition_prefix": "lake/ods/fund_etf_spot/_tmp/run_123/dt=2024-12-01",
            "canonical_prefix": "lake/ods/fund_etf_spot",
            "manifest_path": "lake/ods/fund_etf_spot/dt=2024-12-01/manifest.json",
            "success_flag_path": "lake/ods/fund_etf_spot/dt=2024-12-01/_SUCCESS",
            "partition_date": "2024-12-01",
            "partitioned": True,
        }
        mock_load.side_effect = Exception("Load failed")

        mock_conn = MagicMock()
        mock_temp_conn.return_value.__enter__.return_value = mock_conn

        with pytest.raises(RuntimeError):
            backfill_table_range(
                run_spec=run_spec,
                start_date="2024-12-01",
                end_date="2024-12-01",
                continue_on_error=False,
                **mock_context,
            )

        # Verify cleanup was called despite the error
        mock_cleanup.assert_called_once()
