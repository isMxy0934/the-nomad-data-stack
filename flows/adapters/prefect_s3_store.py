from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from lakehouse_core.io.uri import normalize_s3_key_prefix, parse_s3_uri
from lakehouse_core.store import ObjectStore

from flows.utils.config import build_s3_connection_config_from_env


def _get_boto3_client():  # noqa: ANN001
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


def _batched(values: Iterable[str], *, size: int = 1000) -> Iterable[list[str]]:
    batch: list[str] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class PrefectS3Store(ObjectStore):
    """ObjectStore backed by boto3 (MinIO/S3)."""

    def __init__(self, client: Any | None = None) -> None:
        self._client = client or _get_boto3_client()

    def list_keys(self, prefix_uri: str) -> list[str]:
        bucket, key_prefix = parse_s3_uri(prefix_uri)
        normalized_prefix = normalize_s3_key_prefix(key_prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
            for obj in page.get("Contents", []) or []:
                keys.append(f"s3://{bucket}/{obj['Key']}")
        return keys

    def exists(self, uri: str) -> bool:
        bucket, key = parse_s3_uri(uri)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except Exception:
            return False
        return True

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = parse_s3_uri(uri)
        obj = self._client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    def write_text(self, uri: str, text: str) -> None:
        bucket, key = parse_s3_uri(uri)
        self._client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = parse_s3_uri(uri)
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def delete_prefix(self, prefix_uri: str) -> None:
        bucket, key_prefix = parse_s3_uri(prefix_uri)
        normalized_prefix = normalize_s3_key_prefix(key_prefix)
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
            for obj in page.get("Contents", []) or []:
                keys.append(obj["Key"])
        if not keys:
            return
        for batch in _batched(keys):
            self._client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in batch]},
            )

    def delete_objects(self, uris: list[str]) -> None:
        if not uris:
            return
        bucket, _ = parse_s3_uri(uris[0])
        keys: list[str] = []
        for uri in uris:
            b, key = parse_s3_uri(uri)
            if b != bucket:
                raise ValueError("delete_objects expects all URIs to be in the same bucket")
            keys.append(key)
        for batch in _batched(keys):
            self._client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in batch]},
            )

    def copy_prefix(self, source_prefix_uri: str, dest_prefix_uri: str) -> None:
        src_bucket, src_prefix = parse_s3_uri(source_prefix_uri)
        dest_bucket, dest_prefix = parse_s3_uri(dest_prefix_uri)

        normalized_src = normalize_s3_key_prefix(src_prefix)
        normalized_dest = normalize_s3_key_prefix(dest_prefix)

        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=src_bucket, Prefix=normalized_src):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if not key.startswith(normalized_src):
                    continue
                suffix = key[len(normalized_src) :]
                dest_key = normalized_dest + suffix
                self._client.copy_object(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    CopySource={"Bucket": src_bucket, "Key": key},
                )
