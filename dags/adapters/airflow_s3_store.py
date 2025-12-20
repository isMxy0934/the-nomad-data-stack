from __future__ import annotations

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lakehouse_core.io.uri import normalize_s3_key_prefix, parse_s3_uri
from lakehouse_core.store import ObjectStore


class AirflowS3Store(ObjectStore):
    """ObjectStore backed by Airflow's S3Hook (MinIO/S3)."""

    def __init__(self, s3_hook: S3Hook):
        self._hook = s3_hook

    def list_keys(self, prefix_uri: str) -> list[str]:
        bucket, key_prefix = parse_s3_uri(prefix_uri)
        normalized_prefix = normalize_s3_key_prefix(key_prefix)
        keys = self._hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
        return [f"s3://{bucket}/{key}" for key in keys]

    def exists(self, uri: str) -> bool:
        bucket, key = parse_s3_uri(uri)
        check = getattr(self._hook, "check_for_key", None)
        if callable(check):
            return bool(check(key=key, bucket_name=bucket))
        return self._hook.get_key(key=key, bucket_name=bucket) is not None

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = parse_s3_uri(uri)
        obj = self._hook.get_key(key=key, bucket_name=bucket)
        if obj is None:
            raise FileNotFoundError(f"Object not found: {uri}")
        return obj.get()["Body"].read()

    def write_text(self, uri: str, text: str) -> None:
        bucket, key = parse_s3_uri(uri)
        self._hook.load_string(string_data=text, key=key, bucket_name=bucket, replace=True)

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = parse_s3_uri(uri)
        self._hook.load_bytes(bytes_data=data, key=key, bucket_name=bucket, replace=True)

    def delete_prefix(self, prefix_uri: str) -> None:
        bucket, key_prefix = parse_s3_uri(prefix_uri)
        normalized_prefix = normalize_s3_key_prefix(key_prefix)
        keys = self._hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
        if keys:
            self._hook.delete_objects(bucket=bucket, keys=keys)

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
        self._hook.delete_objects(bucket=bucket, keys=keys)

    def copy_prefix(self, source_prefix_uri: str, dest_prefix_uri: str) -> None:
        src_bucket, src_prefix = parse_s3_uri(source_prefix_uri)
        dest_bucket, dest_prefix = parse_s3_uri(dest_prefix_uri)

        normalized_src = normalize_s3_key_prefix(src_prefix)
        normalized_dest = normalize_s3_key_prefix(dest_prefix)

        keys = self._hook.list_keys(bucket_name=src_bucket, prefix=normalized_src) or []
        for key in keys:
            if not key.startswith(normalized_src):
                continue
            suffix = key[len(normalized_src) :]
            dest_key = normalized_dest + suffix
            self._hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=dest_key,
                source_bucket_name=src_bucket,
                dest_bucket_name=dest_bucket,
            )
