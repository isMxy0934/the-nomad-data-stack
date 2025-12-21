from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dags.adapters.airflow_s3_store import AirflowS3Store
from lakehouse_core.testing.local_store import LocalS3StyleStore


def test_local_s3_style_store_list_keys_treats_trailing_slash_same(tmp_path) -> None:
    store = LocalS3StyleStore(tmp_path)
    store.write_bytes("s3://bucket/lake/a/file1.parquet", b"x")
    store.write_bytes("s3://bucket/lake/a2/file2.parquet", b"y")

    keys_no_slash = store.list_keys("s3://bucket/lake/a")
    keys_with_slash = store.list_keys("s3://bucket/lake/a/")
    assert keys_no_slash == keys_with_slash


def test_local_s3_style_store_list_keys_is_string_prefix_match(tmp_path) -> None:
    store = LocalS3StyleStore(tmp_path)
    store.write_bytes("s3://bucket/lake/a/file1.parquet", b"x")
    store.write_bytes("s3://bucket/lake/ab/file2.parquet", b"y")

    keys = store.list_keys("s3://bucket/lake/a")
    assert "s3://bucket/lake/a/file1.parquet" in keys
    assert "s3://bucket/lake/ab/file2.parquet" in keys


def test_airflow_s3_store_list_keys_normalizes_prefix_without_directory_inference() -> None:
    s3_hook = MagicMock()
    s3_hook.list_keys.return_value = ["lake/a/file1.parquet"]
    store = AirflowS3Store(s3_hook)

    keys = store.list_keys("s3://bucket/lake/a/")
    assert keys == ["s3://bucket/lake/a/file1.parquet"]
    s3_hook.list_keys.assert_called_once_with(bucket_name="bucket", prefix="lake/a")


def test_airflow_s3_store_delete_prefix_uses_same_prefix_normalization() -> None:
    s3_hook = MagicMock()
    s3_hook.list_keys.return_value = ["lake/a/file1.parquet", "lake/a/file2.parquet"]
    store = AirflowS3Store(s3_hook)

    store.delete_prefix("s3://bucket/lake/a/")

    s3_hook.list_keys.assert_called_once_with(bucket_name="bucket", prefix="lake/a")
    s3_hook.delete_objects.assert_called_once_with(
        bucket="bucket",
        keys=["lake/a/file1.parquet", "lake/a/file2.parquet"],
    )


def test_airflow_s3_store_copy_prefix_uses_string_prefix_suffix() -> None:
    s3_hook = MagicMock()
    s3_hook.list_keys.return_value = ["lake/a/file1.parquet"]
    store = AirflowS3Store(s3_hook)

    store.copy_prefix("s3://bucket/lake/a/", "s3://bucket/lake/b/")

    s3_hook.list_keys.assert_called_once_with(bucket_name="bucket", prefix="lake/a")
    s3_hook.copy_object.assert_called_once()
    kwargs = s3_hook.copy_object.call_args.kwargs
    assert kwargs["source_bucket_key"] == "lake/a/file1.parquet"
    assert kwargs["dest_bucket_key"] == "lake/b/file1.parquet"


@pytest.mark.parametrize(
    "prefix_uri",
    [
        "s3://bucket/lake/a/file1.parquet",
        "s3://bucket/lake/a/file1.parquet/",
    ],
)
def test_list_keys_exact_key_prefix_includes_object(tmp_path, prefix_uri: str) -> None:
    store = LocalS3StyleStore(tmp_path)
    store.write_bytes("s3://bucket/lake/a/file1.parquet", b"x")

    keys = store.list_keys(prefix_uri)
    assert keys == ["s3://bucket/lake/a/file1.parquet"]
