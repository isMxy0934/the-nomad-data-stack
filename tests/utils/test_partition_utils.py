import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.utils.partition_utils import (  # pylint: disable=wrong-import-position
        NonPartitionPaths,
        PartitionPaths,
        build_manifest,
        build_non_partition_paths,
        build_partition_paths,
        parse_s3_uri,
        publish_non_partition,
        publish_partition,
    )
except ImportError as exc:
    pytest.skip(
        f"partition_utils import unavailable in this environment: {exc}", allow_module_level=True
    )


def test_build_partition_paths_generates_expected_prefixes():
    paths = build_partition_paths(
        base_prefix="lake/ods/sample_table",
        partition_date="2024-03-01",
        run_id="abc123",
        bucket_name="stock-data",
    )

    assert paths == PartitionPaths(
        partition_date="2024-03-01",
        canonical_prefix="s3://stock-data/lake/ods/sample_table/dt=2024-03-01",
        tmp_prefix="s3://stock-data/lake/ods/sample_table/_tmp/run_abc123",
        tmp_partition_prefix="s3://stock-data/lake/ods/sample_table/_tmp/run_abc123/dt=2024-03-01",
        manifest_path="s3://stock-data/lake/ods/sample_table/dt=2024-03-01/manifest.json",
        success_flag_path="s3://stock-data/lake/ods/sample_table/dt=2024-03-01/_SUCCESS",
    )


def test_build_non_partition_paths_generates_expected_prefixes():
    paths = build_non_partition_paths(
        base_prefix="lake/dim/dim_customer",
        run_id="abc123",
        bucket_name="stock-data",
    )

    assert paths == NonPartitionPaths(
        canonical_prefix="s3://stock-data/lake/dim/dim_customer",
        tmp_prefix="s3://stock-data/lake/dim/dim_customer/_tmp/run_abc123",
        manifest_path="s3://stock-data/lake/dim/dim_customer/manifest.json",
        success_flag_path="s3://stock-data/lake/dim/dim_customer/_SUCCESS",
    )


def test_build_manifest_contains_required_fields():
    manifest = build_manifest(
        dest="ods_sample_table",
        partition_date="2024-03-01",
        run_id="abc123",
        file_count=5,
        row_count=1000,
        source_prefix="s3://bucket/tmp",
        target_prefix="s3://bucket/canonical",
    )

    assert manifest["dest"] == "ods_sample_table"
    assert manifest["partition_date"] == "2024-03-01"
    assert manifest["run_id"] == "abc123"
    assert manifest["file_count"] == 5
    assert manifest["row_count"] == 1000
    assert manifest["status"] == "success"
    assert manifest["source_prefix"] == "s3://bucket/tmp"
    assert manifest["target_prefix"] == "s3://bucket/canonical"
    assert "generated_at" in manifest


@pytest.mark.parametrize(
    "uri,bucket,key",
    [
        ("s3://bucket/path/to/file", "bucket", "path/to/file"),
        (
            "s3://stock-data/lake/ods/table/dt=2024-01-01",
            "stock-data",
            "lake/ods/table/dt=2024-01-01",
        ),
    ],
)
def test_parse_s3_uri_parses_bucket_and_key(uri: str, bucket: str, key: str):
    parsed_bucket, parsed_key = parse_s3_uri(uri)

    assert parsed_bucket == bucket
    assert parsed_key == key


def test_publish_partition_cleans_and_promotes_prefixes(tmp_path):
    s3_hook = MagicMock()
    s3_hook.list_keys.side_effect = [
        [
            "lake/ods/table/dt=2024-03-01/file_1.parquet",
            "lake/ods/table/dt=2024-03-01/file_2.parquet",
        ],
        [
            "lake/ods/table/_tmp/run_abc123/dt=2024-03-01/file_1.parquet",
            "lake/ods/table/_tmp/run_abc123/dt=2024-03-01/file_2.parquet",
        ],
    ]

    paths = PartitionPaths(
        partition_date="2024-03-01",
        canonical_prefix="s3://bucket/lake/ods/table/dt=2024-03-01",
        tmp_prefix="s3://bucket/lake/ods/table/_tmp/run_abc123",
        tmp_partition_prefix="s3://bucket/lake/ods/table/_tmp/run_abc123/dt=2024-03-01",
        manifest_path="s3://bucket/lake/ods/table/dt=2024-03-01/manifest.json",
        success_flag_path="s3://bucket/lake/ods/table/dt=2024-03-01/_SUCCESS",
    )
    manifest = {
        "dest": "ods_table",
        "partition_date": "2024-03-01",
        "run_id": "abc123",
        "file_count": 2,
        "row_count": 20,
        "status": "success",
        "source_prefix": paths.tmp_partition_prefix,
        "target_prefix": paths.canonical_prefix,
        "generated_at": "2024-03-02T00:00:00Z",
    }

    publish_partition(s3_hook=s3_hook, paths=paths, manifest=manifest)

    s3_hook.delete_objects.assert_called_once_with(
        bucket="bucket",
        keys=[
            "lake/ods/table/dt=2024-03-01/file_1.parquet",
            "lake/ods/table/dt=2024-03-01/file_2.parquet",
        ],
    )

    copy_calls = s3_hook.copy_object.call_args_list
    assert len(copy_calls) == 2
    assert (
        copy_calls[0]
        .kwargs["source_bucket_key"]
        .startswith("lake/ods/table/_tmp/run_abc123/dt=2024-03-01/")
    )
    assert copy_calls[0].kwargs["dest_bucket_key"].startswith("lake/ods/table/dt=2024-03-01/")

    manifest_args = s3_hook.load_string.call_args_list[0].kwargs
    assert manifest_args["bucket_name"] == "bucket"
    assert manifest_args["key"] == "lake/ods/table/dt=2024-03-01/manifest.json"
    json.loads(manifest_args["string_data"])

    success_args = s3_hook.load_string.call_args_list[1].kwargs
    assert success_args["key"].endswith("_SUCCESS")
    assert success_args["bucket_name"] == "bucket"


def test_publish_non_partition_promotes_tmp_prefix():
    s3_hook = MagicMock()
    s3_hook.list_keys.side_effect = [
        [
            "lake/dim/table/file_1.parquet",
            "lake/dim/table/file_2.parquet",
        ],
        [
            "lake/dim/table/_tmp/run_abc123/file_1.parquet",
            "lake/dim/table/_tmp/run_abc123/file_2.parquet",
        ],
    ]

    paths = NonPartitionPaths(
        canonical_prefix="s3://bucket/lake/dim/table",
        tmp_prefix="s3://bucket/lake/dim/table/_tmp/run_abc123",
        manifest_path="s3://bucket/lake/dim/table/manifest.json",
        success_flag_path="s3://bucket/lake/dim/table/_SUCCESS",
    )
    manifest = {
        "dest": "dim_table",
        "partition_date": "2024-03-01",
        "run_id": "abc123",
        "file_count": 2,
        "row_count": 20,
        "status": "success",
        "source_prefix": paths.tmp_prefix,
        "target_prefix": paths.canonical_prefix,
        "generated_at": "2024-03-02T00:00:00Z",
    }

    publish_non_partition(s3_hook=s3_hook, paths=paths, manifest=manifest)

    s3_hook.delete_objects.assert_called_once()
    copy_calls = s3_hook.copy_object.call_args_list
    assert len(copy_calls) == 2
    assert copy_calls[0].kwargs["dest_bucket_key"].startswith("lake/dim/table/")

    manifest_args = s3_hook.load_string.call_args_list[0].kwargs
    assert manifest_args["key"] == "lake/dim/table/manifest.json"


@pytest.mark.parametrize(
    "file_count,row_count",
    [(-1, 0), (0, -5)],
)
def test_build_manifest_raises_for_negative_counts(file_count: int, row_count: int):
    with pytest.raises(ValueError):
        build_manifest(
            dest="ods_table",
            partition_date="2024-03-01",
            run_id="abc123",
            file_count=file_count,
            row_count=row_count,
            source_prefix="s3://bucket/tmp",
            target_prefix="s3://bucket/canonical",
        )
