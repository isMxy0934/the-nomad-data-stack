"""Integration test configuration and shared fixtures."""

import csv
import io
import json
import os
import sys
from collections.abc import Callable, Generator
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError

# 设置测试环境路径
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture(scope="session")
def test_bucket_name() -> str:
    """Test bucket name for MinIO/S3 operations."""
    return os.getenv("S3_BUCKET_NAME", "stock-data")


@pytest.fixture(scope="session")
def integration_prefix() -> str:
    """Root prefix for integration test data inside the bucket."""

    return "lake/_integration"


@pytest.fixture(scope="session")
def minio_client(test_bucket_name: str) -> Generator[boto3.client, None, None]:
    """Real MinIO client for integration tests."""
    endpoint_url = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    region = os.getenv("S3_REGION", "us-east-1")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    # 确保 bucket 存在
    try:
        client.head_bucket(Bucket=test_bucket_name)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchBucket"}:
            client.create_bucket(Bucket=test_bucket_name)
        else:
            raise

    yield client


@pytest.fixture(scope="session")
def test_s3_config():
    """S3 connection configuration for DuckDB."""
    from dags.utils.duckdb_utils import S3ConnectionConfig

    endpoint_url = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    region = os.getenv("S3_REGION", "us-east-1")

    return S3ConnectionConfig(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        use_ssl=False,
        url_style="path",
    )


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Directory containing test data files."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def load_test_csv(test_data_dir: Path) -> Callable[[str, str], str]:
    """Load a CSV fixture file and filter rows for a target date (first column equals date)."""

    def loader(filename: str, partition_date: str) -> str:
        path = test_data_dir / filename
        raw = path.read_text(encoding="utf-8")
        input_buf = io.StringIO(raw)
        reader = csv.reader(input_buf)
        rows = list(reader)
        if not rows:
            raise ValueError(f"CSV fixture is empty: {path}")

        header = rows[0]
        filtered = [row for row in rows[1:] if row and row[0] == partition_date]
        output_buf = io.StringIO()
        writer = csv.writer(output_buf, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(filtered)
        return output_buf.getvalue()

    return loader


@pytest.fixture(scope="function")
def temp_output_dir(tmp_path: Path) -> Path:
    """Temporary directory for test outputs."""
    output_dir = tmp_path / "output"
    output_dir.mkdir(exist_ok=True)
    return output_dir


@pytest.fixture(scope="session")
def test_date() -> str:
    """Default test date for integration tests."""
    return os.getenv("TEST_DATE", "2024-01-15")


@pytest.fixture(scope="session")
def s3_delete_prefix(minio_client) -> Callable[[str, str], None]:
    """Delete all objects under a prefix."""

    def delete_prefix(bucket: str, prefix: str) -> None:
        paginator = minio_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            if not contents:
                continue
            minio_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
            )

    return delete_prefix


@pytest.fixture(scope="session")
def s3_publish_partition(minio_client, s3_delete_prefix):
    """Publish a partition from tmp prefix into canonical prefix and write markers."""

    from dags.utils.partition_utils import parse_s3_uri

    def publish_partition(paths, manifest: dict) -> None:
        canonical_bucket, canonical_prefix_key = parse_s3_uri(paths.canonical_prefix)
        tmp_bucket, tmp_prefix_key = parse_s3_uri(paths.tmp_prefix)
        if canonical_bucket != tmp_bucket:
            raise AssertionError("tmp/canonical buckets must match in integration tests")

        canonical_prefix_key = canonical_prefix_key.rstrip("/") + "/"
        tmp_prefix_key = tmp_prefix_key.rstrip("/") + "/"

        s3_delete_prefix(canonical_bucket, canonical_prefix_key)

        paginator = minio_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=tmp_bucket, Prefix=tmp_prefix_key):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                rel = src_key[len(tmp_prefix_key) :]
                dest_key = canonical_prefix_key + rel
                minio_client.copy_object(
                    Bucket=canonical_bucket,
                    Key=dest_key,
                    CopySource={"Bucket": tmp_bucket, "Key": src_key},
                )

        manifest_bucket, manifest_key = parse_s3_uri(paths.manifest_path)
        minio_client.put_object(Bucket=manifest_bucket, Key=manifest_key, Body=json.dumps(manifest))

        success_bucket, success_key = parse_s3_uri(paths.success_flag_path)
        minio_client.put_object(Bucket=success_bucket, Key=success_key, Body=b"")

    return publish_partition


@pytest.fixture(autouse=True)
def setup_minio_bucket(minio_client, test_bucket_name, integration_prefix, s3_delete_prefix):
    """Auto-setup for MinIO bucket before each test."""
    s3_delete_prefix(test_bucket_name, f"{integration_prefix}/")
