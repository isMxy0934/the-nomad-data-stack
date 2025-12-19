import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.utils.etl_utils import (
        build_s3_connection_config,
        commit_dataset,
        delete_tmp_prefix,
        list_parquet_keys,
        non_partition_paths_from_xcom,
        partition_paths_from_xcom,
        prepare_dataset,
        validate_dataset,
    )
    from dags.utils.partition_utils import NonPartitionPaths, PartitionPaths
except ImportError as exc:
    pytest.skip(
        f"etl utils imports unavailable in this environment: {exc}", allow_module_level=True
    )


def test_partition_paths_from_xcom():
    paths_dict = {
        "partition_date": "2024-01-01",
        "canonical_prefix": "s3://bucket/table/dt=2024-01-01",
        "tmp_prefix": "s3://bucket/table/_tmp/run_1",
        "tmp_partition_prefix": "s3://bucket/table/_tmp/run_1/dt=2024-01-01",
        "manifest_path": "s3://bucket/table/dt=2024-01-01/manifest.json",
        "success_flag_path": "s3://bucket/table/dt=2024-01-01/_SUCCESS",
    }
    paths = partition_paths_from_xcom(paths_dict)
    assert isinstance(paths, PartitionPaths)
    assert paths.partition_date == "2024-01-01"
    assert paths.canonical_prefix == paths_dict["canonical_prefix"]


def test_non_partition_paths_from_xcom():
    paths_dict = {
        "canonical_prefix": "s3://bucket/table",
        "tmp_prefix": "s3://bucket/table/_tmp/run_1",
        "manifest_path": "s3://bucket/table/manifest.json",
        "success_flag_path": "s3://bucket/table/_SUCCESS",
    }
    paths = non_partition_paths_from_xcom(paths_dict)
    assert isinstance(paths, NonPartitionPaths)
    assert paths.canonical_prefix == paths_dict["canonical_prefix"]


def test_build_s3_connection_config():
    mock_s3_hook = MagicMock()
    mock_connection = MagicMock()
    mock_connection.login = "access"
    mock_connection.password = "secret"
    mock_connection.extra_dejson = {
        "endpoint_url": "http://minio:9000",
        "region_name": "us-east-1",
    }
    mock_s3_hook.get_connection.return_value = mock_connection
    mock_s3_hook.aws_conn_id = "MINIO_S3"

    config = build_s3_connection_config(mock_s3_hook)
    assert config.endpoint_url == "http://minio:9000"
    assert config.access_key == "access"
    assert config.secret_key == "secret"
    assert config.region == "us-east-1"


def test_build_s3_connection_config_missing_endpoint():
    mock_s3_hook = MagicMock()
    mock_connection = MagicMock()
    mock_connection.extra_dejson = {}
    mock_s3_hook.get_connection.return_value = mock_connection

    with pytest.raises(ValueError, match="S3 connection must define endpoint_url"):
        build_s3_connection_config(mock_s3_hook)


def test_list_parquet_keys():
    mock_s3_hook = MagicMock()
    mock_s3_hook.list_keys.return_value = ["table/file1.parquet", "table/file2.txt", "table/sub/file3.parquet"]
    
    keys = list_parquet_keys(mock_s3_hook, "s3://bucket/table/")
    assert keys == ["table/file1.parquet", "table/sub/file3.parquet"]
    mock_s3_hook.list_keys.assert_called_once_with(bucket_name="bucket", prefix="table/")


def test_delete_tmp_prefix():
    mock_s3_hook = MagicMock()
    mock_s3_hook.list_keys.return_value = ["tmp/f1.pq", "tmp/f2.pq"]
    
    delete_tmp_prefix(mock_s3_hook, "s3://bucket/tmp/")
    mock_s3_hook.delete_objects.assert_called_once_with(bucket="bucket", keys=["tmp/f1.pq", "tmp/f2.pq"])


def test_prepare_dataset_partitioned():
    result = prepare_dataset(
        base_prefix="lake/ods/table",
        run_id="run123",
        is_partitioned=True,
        partition_date="2024-01-01",
        bucket_name="my-bucket"
    )
    assert result["partitioned"] is True
    assert result["partition_date"] == "2024-01-01"
    assert "lake/ods/table/dt=2024-01-01" in result["canonical_prefix"]
    assert "run123" in result["tmp_prefix"]


def test_prepare_dataset_non_partitioned():
    result = prepare_dataset(
        base_prefix="lake/ods/table",
        run_id="run123",
        is_partitioned=False,
        bucket_name="my-bucket"
    )
    assert result["partitioned"] is False
    assert result["canonical_prefix"] == "s3://my-bucket/lake/ods/table"


def test_validate_dataset_success():
    mock_s3_hook = MagicMock()
    # Mock list_parquet_keys to return 2 files
    with patch("dags.utils.etl_utils.list_parquet_keys", return_value=["f1.pq", "f2.pq"]):
        paths_dict = {"partitioned": False, "tmp_prefix": "s3://b/tmp"}
        metrics = {"has_data": 1, "file_count": 2, "row_count": 100}
        
        result = validate_dataset(paths_dict, metrics, mock_s3_hook)
        assert result == metrics


def test_validate_dataset_no_data_expected():
    mock_s3_hook = MagicMock()
    with patch("dags.utils.etl_utils.list_parquet_keys", return_value=[]):
        paths_dict = {"partitioned": False, "tmp_prefix": "s3://b/tmp"}
        metrics = {"has_data": 0, "file_count": 0, "row_count": 0}
        
        result = validate_dataset(paths_dict, metrics, mock_s3_hook)
        assert result == metrics


def test_validate_dataset_file_count_mismatch():
    mock_s3_hook = MagicMock()
    with patch("dags.utils.etl_utils.list_parquet_keys", return_value=["f1.pq"]):
        paths_dict = {"partitioned": False, "tmp_prefix": "s3://b/tmp"}
        metrics = {"has_data": 1, "file_count": 2, "row_count": 100}
        
        with pytest.raises(ValueError, match="File count mismatch"):
            validate_dataset(paths_dict, metrics, mock_s3_hook)


@patch("dags.utils.etl_utils.publish_partition")
@patch("dags.utils.etl_utils.build_manifest")
def test_commit_dataset_partitioned(mock_build_manifest, mock_publish):
    mock_s3_hook = MagicMock()
    paths_dict = {
        "partitioned": True,
        "partition_date": "2024-01-01",
        "canonical_prefix": "s3://b/t/dt=2024-01-01",
        "tmp_prefix": "s3://b/t/_tmp/r1",
        "tmp_partition_prefix": "s3://b/t/_tmp/r1/dt=2024-01-01",
        "manifest_path": "s3://b/t/dt=2024-01-01/m.json",
        "success_flag_path": "s3://b/t/dt=2024-01-01/_S",
    }
    metrics = {"has_data": 1, "file_count": 1, "row_count": 10}
    mock_publish.return_value = {"published": "1"}
    mock_build_manifest.return_value = {"manifest": "data"}

    res, manifest = commit_dataset("table", "r1", paths_dict, metrics, mock_s3_hook)
    
    assert res == {"published": "1"}
    assert manifest == {"manifest": "data"}
    mock_publish.assert_called_once()


def test_commit_dataset_no_data():
    mock_s3_hook = MagicMock()
    paths_dict = {
        "partitioned": False,
        "partition_date": "2024-01-01",
        "canonical_prefix": "s3://b/t",
        "tmp_prefix": "s3://b/t/_tmp/r1",
        "manifest_path": "s3://b/t/m.json",
        "success_flag_path": "s3://b/t/_S",
    }
    metrics = {"has_data": 0}
    
    with patch("dags.utils.etl_utils.delete_prefix") as mock_delete:
        res, manifest = commit_dataset("table", "r1", paths_dict, metrics, mock_s3_hook)
        assert res["action"] == "cleared"
        assert manifest == {}
        mock_delete.assert_called_once_with(mock_s3_hook, "s3://b/t")
