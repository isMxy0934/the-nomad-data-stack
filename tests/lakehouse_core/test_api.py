from lakehouse_core.api import prepare_paths


def test_prepare_paths_partitioned_uses_store_namespace_bucket():
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )
    assert paths.canonical_prefix == "s3://bucket/lake/ods/t/dt=2024-01-01"


def test_prepare_paths_non_partitioned_uses_store_namespace_uri():
    paths = prepare_paths(
        base_prefix="lake/dim/t",
        run_id="r1",
        partition_date=None,
        is_partitioned=False,
        store_namespace="s3://bucket",
    )
    assert paths.canonical_prefix == "s3://bucket/lake/dim/t"


def test_cleanup_tmp_deletes_tmp_prefix(tmp_path):
    from lakehouse_core.api import cleanup_tmp
    from lakehouse_core.testing.local_store import LocalS3StyleStore

    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )
    store.write_bytes(f"{paths.tmp_partition_prefix}/a.parquet", b"x")
    store.write_bytes(f"{paths.tmp_partition_prefix}/b.parquet", b"y")

    cleanup_tmp(store=store, paths=paths)
    assert store.list_keys(paths.tmp_prefix) == []


def test_validate_output_checks_file_count(tmp_path):
    from lakehouse_core.api import validate_output
    from lakehouse_core.testing.local_store import LocalS3StyleStore

    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )

    store.write_bytes(f"{paths.tmp_partition_prefix}/a.parquet", b"x")
    store.write_bytes(f"{paths.tmp_partition_prefix}/b.parquet", b"y")

    metrics = {"has_data": 1, "file_count": 2, "row_count": 10}
    assert validate_output(store=store, paths=paths, metrics=metrics) == metrics


"""CSV validation tests to be appended to test_api.py"""
import pytest

from lakehouse_core.api import validate_output
from lakehouse_core.testing.local_store import LocalS3StyleStore


def test_validate_output_with_csv_format(tmp_path):
    """Test validate_output with CSV format."""
    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )

    # Write CSV file to tmp
    store.write_bytes(f"{paths.tmp_partition_prefix}/data.csv", b"id,value\n1,10")

    metrics = {"row_count": 1, "file_count": 1, "has_data": 1}

    # Validate CSV format
    result = validate_output(
        store=store,
        paths=paths,
        metrics=metrics,
        file_format="csv",
    )

    assert result["row_count"] == 1
    assert result["file_count"] == 1


def test_validate_output_with_csv_format_missing_files(tmp_path):
    """Test validate_output with CSV format when files are missing."""
    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )

    # Write Parquet file (not CSV)
    store.write_bytes(f"{paths.tmp_partition_prefix}/data.parquet", b"x")

    metrics = {"row_count": 1, "file_count": 1, "has_data": 1}

    # Should raise error for CSV validation when only parquet files exist
    with pytest.raises(ValueError, match="No csv files were written"):
        validate_output(
            store=store,
            paths=paths,
            metrics=metrics,
            file_format="csv",
        )
