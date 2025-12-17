"""Integration test configuration and shared fixtures."""

import os
import sys
from collections.abc import Generator
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


@pytest.fixture(autouse=True)
def setup_minio_bucket(minio_client, test_bucket_name):
    """Auto-setup for MinIO bucket before each test."""
    # 这里可以添加通用的 bucket 清理或设置逻辑
    # 目前保持简单，只确保 bucket 存在
    pass
