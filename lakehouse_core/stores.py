from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

from lakehouse_core.storage import ObjectStore
from lakehouse_core.uri import normalize_s3_key_prefix, parse_s3_uri


def _normalize_prefix_uri(prefix_uri: str) -> str:
    value = (prefix_uri or "").strip()
    if not value:
        raise ValueError("prefix_uri is required")
    return unquote(value.rstrip("/")) + "/"


def _file_uri_to_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"Invalid file URI: {uri}")
    raw_path = unquote(parsed.path or "")
    local_path = url2pathname(raw_path)
    if local_path.startswith("/") and len(local_path) >= 3 and local_path[2] == ":":
        local_path = local_path[1:]
    path = Path(local_path)
    if not path.is_absolute():
        raise ValueError(f"file URI must be absolute: {uri}")
    return path


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


@dataclass
class LocalFileStore(ObjectStore):
    """A simple local filesystem store using file:// URIs."""

    root_dir: Path

    def __post_init__(self) -> None:
        self.root_dir = self.root_dir.resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _assert_under_root(self, path: Path) -> None:
        resolved = path.resolve()
        try:
            resolved.relative_to(self.root_dir)
        except ValueError as exc:
            raise ValueError(f"Path escapes root_dir: {path}") from exc

    def _uri_to_path(self, uri: str) -> Path:
        path = _file_uri_to_path(uri)
        self._assert_under_root(path)
        return path

    def list_keys(self, prefix_uri: str) -> list[str]:
        normalized_prefix = _normalize_prefix_uri(prefix_uri)
        prefix_path = self._uri_to_path(prefix_uri)
        if not prefix_path.exists():
            return []

        if prefix_path.is_file():
            uri = unquote(prefix_path.resolve().as_uri())
            return [uri] if uri.startswith(normalized_prefix) else []

        keys: list[str] = []
        for file_path in prefix_path.rglob("*"):
            if file_path.is_file():
                uri = unquote(file_path.resolve().as_uri())
                if uri.startswith(normalized_prefix):
                    keys.append(uri)
        return sorted(keys)

    def exists(self, uri: str) -> bool:
        return self._uri_to_path(uri).exists()

    def read_bytes(self, uri: str) -> bytes:
        path = self._uri_to_path(uri)
        return path.read_bytes()

    def write_text(self, uri: str, text: str) -> None:
        path = self._uri_to_path(uri)
        _ensure_parent(path)
        path.write_text(text, encoding="utf-8")

    def write_bytes(self, uri: str, data: bytes) -> None:
        path = self._uri_to_path(uri)
        _ensure_parent(path)
        path.write_bytes(data)

    def delete_objects(self, uris: list[str]) -> None:
        for uri in uris:
            path = self._uri_to_path(uri)
            try:
                path.unlink(missing_ok=True)
            except IsADirectoryError:
                shutil.rmtree(path, ignore_errors=True)

    def delete_prefix(self, prefix_uri: str) -> None:
        prefix_path = self._uri_to_path(prefix_uri)
        if not prefix_path.exists():
            return
        if prefix_path.is_file():
            prefix_path.unlink(missing_ok=True)
            return
        shutil.rmtree(prefix_path, ignore_errors=True)

    def copy_prefix(self, source_prefix_uri: str, dest_prefix_uri: str) -> None:
        source_base = _normalize_prefix_uri(source_prefix_uri)
        dest_base = _normalize_prefix_uri(dest_prefix_uri)
        for src_uri in self.list_keys(source_prefix_uri):
            if not src_uri.startswith(source_base):
                continue
            suffix = src_uri[len(source_base) :]
            dst_uri = dest_base + suffix
            src_path = self._uri_to_path(src_uri)
            dst_path = self._uri_to_path(dst_uri)
            _ensure_parent(dst_path)
            shutil.copy2(src_path, dst_path)


class Boto3S3Store(ObjectStore):
    """S3/MinIO adapter using boto3 (for scripts/Prefect, not Airflow-specific)."""

    def __init__(
        self,
        *,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        region: str = "us-east-1",
        use_ssl: bool | None = None,
        url_style: str = "path",
        session_token: str | None = None,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        try:
            import boto3
            from botocore.config import Config
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("boto3 is required for Boto3S3Store") from exc

        if use_ssl is None:
            use_ssl = bool(endpoint_url and endpoint_url.startswith("https://"))

        config = Config(s3={"addressing_style": url_style})
        kwargs: dict[str, Any] = dict(client_kwargs or {})
        kwargs.update(
            dict(
                service_name="s3",
                endpoint_url=endpoint_url,
                region_name=region,
                use_ssl=use_ssl,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                config=config,
            )
        )
        self._client = boto3.client(**kwargs)

    def list_keys(self, prefix_uri: str) -> list[str]:
        bucket, key_prefix = parse_s3_uri(prefix_uri)
        normalized_prefix = normalize_s3_key_prefix(key_prefix)

        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=normalized_prefix):
            for obj in page.get("Contents", []) or []:
                key = obj.get("Key")
                if key:
                    keys.append(f"s3://{bucket}/{key}")
        return sorted(keys)

    def exists(self, uri: str) -> bool:
        bucket, key = parse_s3_uri(uri)
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as exc:  # noqa: BLE001
            response = getattr(exc, "response", None) or {}
            error = response.get("Error") or {}
            code = str(error.get("Code") or "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def read_bytes(self, uri: str) -> bytes:
        bucket, key = parse_s3_uri(uri)
        response = self._client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        return body

    def write_text(self, uri: str, text: str) -> None:
        self.write_bytes(uri, text.encode("utf-8"))

    def write_bytes(self, uri: str, data: bytes) -> None:
        bucket, key = parse_s3_uri(uri)
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    def delete_objects(self, uris: list[str]) -> None:
        by_bucket: dict[str, list[str]] = {}
        for uri in uris:
            bucket, key = parse_s3_uri(uri)
            by_bucket.setdefault(bucket, []).append(key)

        for bucket, keys in by_bucket.items():
            if not keys:
                continue
            chunks = [keys[i : i + 1000] for i in range(0, len(keys), 1000)]
            for chunk in chunks:
                self._client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
                )

    def delete_prefix(self, prefix_uri: str) -> None:
        uris = self.list_keys(prefix_uri)
        if uris:
            self.delete_objects(uris)

    def copy_prefix(self, source_prefix_uri: str, dest_prefix_uri: str) -> None:
        source_bucket, source_prefix = parse_s3_uri(source_prefix_uri)
        dest_bucket, dest_prefix = parse_s3_uri(dest_prefix_uri)

        normalized_src = normalize_s3_key_prefix(source_prefix)
        normalized_dest = normalize_s3_key_prefix(dest_prefix)
        source_list_uri = f"s3://{source_bucket}/{normalized_src}"

        for src_uri in self.list_keys(source_list_uri):
            _, src_key = parse_s3_uri(src_uri)
            if not src_key.startswith(normalized_src):
                continue
            suffix = src_key[len(normalized_src) :]
            dst_key = normalized_dest + suffix
            self._client.copy_object(
                Bucket=dest_bucket,
                Key=dst_key,
                CopySource={"Bucket": source_bucket, "Key": src_key},
            )


def env_default(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()
