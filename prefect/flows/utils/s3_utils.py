"""S3 utilities for Prefect flows."""

from __future__ import annotations

from typing import Any

from flows.utils.config import build_s3_connection_config_from_env, get_s3_bucket_name


def get_boto3_client():  # noqa: ANN001
    try:
        import boto3
        from botocore.config import Config
    except ModuleNotFoundError as exc:  # noqa: PERF203
        raise ModuleNotFoundError(
            "boto3 is required for Prefect S3 access. Install boto3 to use flows."
        ) from exc

    config = build_s3_connection_config_from_env()
    boto_config = Config(s3={"addressing_style": config.url_style})
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
        use_ssl=config.use_ssl,
        config=boto_config,
    )


class PrefectS3Uploader:
    """S3 File upload utility class for Prefect flows."""

    def __init__(self, client: Any | None = None, bucket_name: str | None = None) -> None:
        self.bucket_name = bucket_name or get_s3_bucket_name()
        self.client = client or get_boto3_client()

    def upload_file(self, local_file_path: str, key: str) -> str:
        if not key:
            raise ValueError("s3 key is required")
        self.client.upload_file(local_file_path, self.bucket_name, key)
        return f"s3://{self.bucket_name}/{key}"

    def upload_bytes(self, data: bytes, key: str) -> str:
        if not key:
            raise ValueError("s3 key is required")
        self.client.put_object(Bucket=self.bucket_name, Key=key, Body=data)
        return f"s3://{self.bucket_name}/{key}"
