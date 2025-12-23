"""Tests for StandardS3Compactor."""

import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parents[3]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.ingestion.standard.compactors import StandardS3Compactor
    from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
except ImportError as exc:
    pytest.skip(
        f"StandardS3Compactor imports unavailable in this environment: {exc}",
        allow_module_level=True,
    )


@pytest.fixture
def mock_s3_hook():
    """Create a mock S3Hook."""
    hook = MagicMock()
    return hook


@pytest.fixture
def mock_store():
    """Create a mock AirflowS3Store."""
    store = MagicMock()
    store.read_bytes = MagicMock(return_value=b"fake_parquet_data")
    store.write_bytes = MagicMock()
    store.delete_prefix = MagicMock()
    return store


@pytest.fixture
def sample_partition_paths_dict():
    """Create sample partitioned paths dict."""
    return {
        "partitioned": True,
        "partition_date": "2024-01-01",
        "canonical_prefix": "s3://bucket/lake/raw/daily/target/dt=2024-01-01",
        "tmp_prefix": "s3://bucket/lake/raw/daily/target/_tmp/run_123",
        "tmp_partition_prefix": "s3://bucket/lake/raw/daily/target/_tmp/run_123/dt=2024-01-01",
        "manifest_path": "s3://bucket/lake/raw/daily/target/dt=2024-01-01/manifest.json",
        "success_flag_path": "s3://bucket/lake/raw/daily/target/dt=2024-01-01/_SUCCESS",
    }


@pytest.fixture
def sample_non_partition_paths_dict():
    """Create sample non-partitioned paths dict."""
    return {
        "partitioned": False,
        "canonical_prefix": "s3://bucket/lake/raw/daily/target",
        "tmp_prefix": "s3://bucket/lake/raw/daily/target/_tmp/run_123",
        "manifest_path": "s3://bucket/lake/raw/daily/target/manifest.json",
        "success_flag_path": "s3://bucket/lake/raw/daily/target/_SUCCESS",
    }


class TestStandardS3Compactor:
    """Test StandardS3Compactor functionality."""

    def test_init(self):
        """Test StandardS3Compactor initialization."""
        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
            file_format="csv",
            dedup_cols=["id", "date"],
        )
        assert compactor.bucket == "test-bucket"
        assert compactor.prefix_template == "lake/raw/daily/{target}"
        assert compactor.file_format == "csv"
        assert compactor.dedup_cols == ["id", "date"]
        assert compactor.partition_column is None

    def test_init_with_partition_column(self):
        """Test initialization with partition column."""
        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
            file_format="parquet",
            partition_column="trade_date",
        )
        assert compactor.partition_column == "trade_date"

    def test_get_paths_obj_partitioned(self, sample_partition_paths_dict):
        """Test _get_paths_obj for partitioned paths."""
        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
        )
        paths = compactor._get_paths_obj(sample_partition_paths_dict)
        assert isinstance(paths, PartitionPaths)
        assert paths.partition_date == "2024-01-01"
        assert paths.canonical_prefix == sample_partition_paths_dict["canonical_prefix"]

    def test_get_paths_obj_non_partitioned(self, sample_non_partition_paths_dict):
        """Test _get_paths_obj for non-partitioned paths."""
        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
        )
        paths = compactor._get_paths_obj(sample_non_partition_paths_dict)
        assert isinstance(paths, NonPartitionPaths)
        assert paths.canonical_prefix == sample_non_partition_paths_dict["canonical_prefix"]

    @patch("dags.ingestion.standard.compactors.AirflowS3Store")
    @patch("dags.ingestion.standard.compactors.validate_dataset")
    @patch("dags.ingestion.standard.compactors.commit")
    @patch("dags.ingestion.standard.compactors.cleanup")
    @patch("pandas.read_parquet")
    def test_compact_with_data(
        self,
        mock_read_parquet,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_store_class,
        sample_partition_paths_dict,
    ):
        """Test compact method with valid data."""
        # Setup mocks
        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        # Create sample DataFrames
        df1 = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        df2 = pd.DataFrame({"id": [2, 3], "value": [25, 30]})

        # Mock read_parquet to return DataFrames
        mock_read_parquet.side_effect = [df1, df2]

        # Mock store.read_bytes to return parquet bytes
        parquet_bytes = BytesIO()
        df1.to_parquet(parquet_bytes)
        mock_store.read_bytes.return_value = parquet_bytes.getvalue()

        # Mock validate to return the same metrics
        mock_validate.return_value = {"row_count": 3, "file_count": 1, "has_data": 1}

        # Mock commit return value
        mock_commit.return_value = ({"manifest_path": "s3://.../manifest.json"}, {})

        # Create compactor
        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
            file_format="parquet",
            dedup_cols=["id"],
        )

        # Prepare results
        results = [
            {"uri": "s3://tmp/result1.parquet", "row_count": 2},
            {"uri": "s3://tmp/result2.parquet", "row_count": 2},
        ]

        # Execute compact
        result = compactor.compact(
            results=results,
            target="test_target",
            partition_date="2024-01-01",
            paths_dict=sample_partition_paths_dict,
            run_id="test_run_123",
        )

        # Verify validate was called with load metrics
        mock_validate.assert_called_once()
        call_args = mock_validate.call_args
        assert call_args[1]["metrics"]["row_count"] == 3  # After dedup
        assert call_args[1]["metrics"]["file_count"] == 1
        # Verify file_format was passed to validate
        assert call_args[1]["file_format"] == "parquet"

        # Verify commit was called with validated metrics
        mock_commit.assert_called_once()

        # Verify cleanup was called
        mock_cleanup.assert_called_once()

        # Verify - compact returns publish_result from commit()
        assert "manifest_path" in result or "published" in result

    @patch("dags.ingestion.standard.compactors.AirflowS3Store")
    def test_compact_with_no_data(self, mock_store_class, sample_partition_paths_dict):
        """Test compact with no data returns early."""
        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
        )

        result = compactor.compact(
            results=[],
            target="test_target",
            partition_date="2024-01-01",
            paths_dict=sample_partition_paths_dict,
            run_id="test_run_123",
        )

        assert result["row_count"] == 0
        assert result["status"] == "skipped"

    def test_compact_requires_paths_dict(self):
        """Test that compact raises ValueError without paths_dict."""
        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
        )

        with pytest.raises(ValueError, match="paths_dict is required"):
            compactor.compact(
                results=[],
                target="test_target",
                partition_date="2024-01-01",
                run_id="test_run_123",
            )

    @patch("dags.ingestion.standard.compactors.AirflowS3Store")
    @patch("dags.ingestion.standard.compactors.validate_dataset")
    @patch("dags.ingestion.standard.compactors.commit")
    @patch("dags.ingestion.standard.compactors.cleanup")
    @patch("pandas.read_parquet")
    def test_compact_csv_format(
        self,
        mock_read_parquet,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_store_class,
        sample_partition_paths_dict,
    ):
        """Test compact with CSV format."""
        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        mock_read_parquet.return_value = df

        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes)
        mock_store.read_bytes.return_value = parquet_bytes.getvalue()

        mock_validate.return_value = {"row_count": 2, "file_count": 1, "has_data": 1}
        mock_commit.return_value = ({"manifest_path": "s3://..."}, {})

        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
            file_format="csv",
        )

        results = [{"uri": "s3://tmp/result1.parquet", "row_count": 2}]

        result = compactor.compact(
            results=results,
            target="test_target",
            partition_date="2024-01-01",
            paths_dict=sample_partition_paths_dict,
            run_id="test_run_123",
        )

        # Verify compact returns publish_result
        assert "manifest_path" in result or "published" in result

        # Verify validate, commit, cleanup were called
        mock_validate.assert_called_once()
        mock_commit.assert_called_once()
        mock_cleanup.assert_called_once()

        # Verify file_format="csv" was passed to validate
        call_args = mock_validate.call_args
        assert call_args[1]["file_format"] == "csv"

        # Verify write_bytes was called (for CSV format)
        assert mock_store.write_bytes.called
        # Check that the written content is CSV
        call_args = mock_store.write_bytes.call_args
        written_content = call_args[0][1]  # Second arg is content
        assert b"id,value" in written_content or b"value,id" in written_content

    @patch("dags.ingestion.standard.compactors.AirflowS3Store")
    @patch("dags.ingestion.standard.compactors.validate_dataset")
    @patch("dags.ingestion.standard.compactors.commit")
    @patch("dags.ingestion.standard.compactors.cleanup")
    def test_compact_with_partition_column(
        self,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_store_class,
        sample_partition_paths_dict,
    ):
        """Test compact with partition_column groups data correctly."""
        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        # Create DataFrames with different trade_date values AND different symbols
        # to avoid deduplication removing rows
        df1 = pd.DataFrame(
            {
                "symbol": ["ETF1", "ETF2"],
                "trade_date": ["2024-01-01", "2024-01-01"],
                "close": [10.0, 11.0],
            }
        )
        df2 = pd.DataFrame(
            {
                "symbol": ["ETF3", "ETF4"],
                "trade_date": ["2024-01-02", "2024-01-02"],
                "close": [11.5, 12.0],
            }
        )

        # Create parquet bytes for each DataFrame
        parquet_bytes1 = BytesIO()
        df1.to_parquet(parquet_bytes1)
        parquet_bytes2 = BytesIO()
        df2.to_parquet(parquet_bytes2)

        # Track read_bytes calls to debug
        read_bytes_calls = []

        def track_read_bytes(uri):
            read_bytes_calls.append(uri)
            if len(read_bytes_calls) == 1:
                return parquet_bytes1.getvalue()
            elif len(read_bytes_calls) == 2:
                return parquet_bytes2.getvalue()
            raise AssertionError(f"Unexpected read_bytes call: {uri}")

        mock_store.read_bytes.side_effect = track_read_bytes

        # Mock validate to return actual metrics based on input
        def mock_validate_side_effect(paths_dict, metrics, s3_hook, file_format):
            # Return the actual metrics passed in
            return metrics

        mock_validate.side_effect = mock_validate_side_effect

        # Mock commit to return success for each partition
        mock_commit.return_value = ({"manifest_path": "s3://.../manifest.json"}, {})

        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
            file_format="csv",
            dedup_cols=["trade_date", "symbol"],
            partition_column="trade_date",
        )

        results = [
            {"uri": "s3://tmp/result1.parquet", "row_count": 2},
            {"uri": "s3://tmp/result2.parquet", "row_count": 2},
        ]

        result = compactor.compact(
            results=results,
            target="test_target",
            partition_date="2024-01-01",
            paths_dict=sample_partition_paths_dict,
            run_id="test_run_123",
        )

        # Verify the result has partitioned status
        assert result["status"] == "partitioned"
        assert result["partition_count"] == 2
        assert result["row_count"] == 4

        # Verify commit was called twice (once for each partition)
        assert mock_commit.call_count == 2
        assert mock_cleanup.call_count == 2
        assert mock_validate.call_count == 2

        # Verify each partition was committed with correct partition_date
        commit_calls = mock_commit.call_args_list
        partition_dates = [call[1]["partition_date"] for call in commit_calls]
        assert "2024-01-01" in partition_dates
        assert "2024-01-02" in partition_dates

    @patch("dags.ingestion.standard.compactors.AirflowS3Store")
    @patch("dags.ingestion.standard.compactors.validate_dataset")
    @patch("dags.ingestion.standard.compactors.commit")
    @patch("dags.ingestion.standard.compactors.cleanup")
    def test_compact_partition_column_non_partitioned_paths(
        self,
        mock_cleanup,
        mock_commit,
        mock_validate,
        mock_store_class,
        sample_non_partition_paths_dict,
    ):
        """Test that partition_column is ignored for non-partitioned paths."""
        mock_store = MagicMock()
        mock_store_class.return_value = mock_store

        df = pd.DataFrame(
            {
                "symbol": ["ETF1"],
                "trade_date": ["2024-01-01"],
                "close": [10.0],
            }
        )

        parquet_bytes = BytesIO()
        df.to_parquet(parquet_bytes)
        mock_store.read_bytes.return_value = parquet_bytes.getvalue()

        mock_validate.return_value = {"row_count": 1, "file_count": 1, "has_data": 1}
        mock_commit.return_value = ({"manifest_path": "s3://..."}, {})

        compactor = StandardS3Compactor(
            bucket="test-bucket",
            prefix_template="lake/raw/daily/{target}",
            partition_column="trade_date",
        )

        results = [{"uri": "s3://tmp/result1.parquet", "row_count": 1}]

        compactor.compact(
            results=results,
            target="test_target",
            partition_date="2024-01-01",
            paths_dict=sample_non_partition_paths_dict,
            run_id="test_run_123",
        )

        # Should use single partition mode (no grouping)
        mock_commit.assert_called_once()
        mock_validate.assert_called_once()
