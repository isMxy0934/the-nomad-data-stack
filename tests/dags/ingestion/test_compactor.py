"""Tests for StandardS3Compactor."""

import sys
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

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
        f"StandardS3Compactor imports unavailable in this environment: {exc}", allow_module_level=True
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
    @patch("dags.ingestion.standard.compactors.commit")
    @patch("dags.ingestion.standard.compactors.cleanup")
    @patch("pandas.read_parquet")
    def test_compact_with_data(
        self,
        mock_read_parquet,
        mock_cleanup,
        mock_commit,
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

        # Verify - compact returns publish_result from commit()
        assert "manifest_path" in result or "published" in result
        # The result is what commit() returns: dict with manifest_path, success_flag_path
        assert isinstance(result, dict)

        # Verify commit and cleanup were called
        mock_commit.assert_called_once()
        mock_cleanup.assert_called_once()

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
    @patch("dags.ingestion.standard.compactors.commit")
    @patch("dags.ingestion.standard.compactors.cleanup")
    @patch("pandas.read_parquet")
    def test_compact_csv_format(
        self,
        mock_read_parquet,
        mock_cleanup,
        mock_commit,
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

        # Verify write_bytes was called (for CSV format)
        assert mock_store.write_bytes.called
        # Check that the written content is CSV
        call_args = mock_store.write_bytes.call_args
        written_content = call_args[0][1]  # Second arg is content
        assert b"id,value" in written_content or b"value,id" in written_content
