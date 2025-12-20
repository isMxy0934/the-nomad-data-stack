from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from lakehouse_core.storage import ObjectStore
from lakehouse_core.uri import normalize_s3_key_prefix, parse_s3_uri


@dataclass
class StoreOp:
    name: str
    args: tuple[object, ...]


class LocalS3StyleStore(ObjectStore):
    """Local filesystem store that simulates S3 semantics using ``s3://bucket/key`` URIs.

    Maps each URI to a file under: ``root_dir / bucket / key``.
    """

    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)
        self.ops: list[StoreOp] = []

    def _to_path(self, uri: str) -> Path:
        bucket, key = parse_s3_uri(uri)
        return self.root_dir / bucket / key

    def list_keys(self, prefix_uri: str) -> list[str]:
        self.ops.append(StoreOp("list_keys", (prefix_uri,)))
        bucket, key_prefix = parse_s3_uri(prefix_uri)
        bucket_root = self.root_dir / bucket
        if not bucket_root.exists():
            return []

        normalized = normalize_s3_key_prefix(key_prefix)
        uris: list[str] = []
        for file_path in bucket_root.rglob("*"):
            if not file_path.is_file():
                continue
            rel = file_path.relative_to(bucket_root).as_posix()
            if rel.startswith(normalized):
                uris.append(f"s3://{bucket}/{rel}")
        return sorted(uris)

    def exists(self, uri: str) -> bool:
        return self._to_path(uri).exists()

    def read_bytes(self, uri: str) -> bytes:
        return self._to_path(uri).read_bytes()

    def write_text(self, uri: str, text: str) -> None:
        self.ops.append(StoreOp("write_text", (uri,)))
        path = self._to_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def write_bytes(self, uri: str, data: bytes) -> None:
        self.ops.append(StoreOp("write_bytes", (uri,)))
        path = self._to_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def delete_prefix(self, prefix_uri: str) -> None:
        self.ops.append(StoreOp("delete_prefix", (prefix_uri,)))
        self.delete_objects(self.list_keys(prefix_uri))

    def delete_objects(self, uris: list[str]) -> None:
        self.ops.append(StoreOp("delete_objects", (list(uris),)))
        for uri in uris:
            try:
                self._to_path(uri).unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                pass

    def copy_prefix(self, source_prefix_uri: str, dest_prefix_uri: str) -> None:
        self.ops.append(StoreOp("copy_prefix", (source_prefix_uri, dest_prefix_uri)))
        src_bucket, src_prefix = parse_s3_uri(source_prefix_uri)
        dest_bucket, dest_prefix = parse_s3_uri(dest_prefix_uri)

        normalized_src = normalize_s3_key_prefix(src_prefix)
        normalized_dest = normalize_s3_key_prefix(dest_prefix)

        for src_uri in self.list_keys(source_prefix_uri):
            _, src_key = parse_s3_uri(src_uri)
            if not src_key.startswith(normalized_src):
                continue
            suffix = src_key[len(normalized_src) :]
            dest_uri = f"s3://{dest_bucket}/{normalized_dest}{suffix}"
            src_path = self._to_path(src_uri)
            dest_path = self._to_path(dest_uri)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(src_path.read_bytes())
