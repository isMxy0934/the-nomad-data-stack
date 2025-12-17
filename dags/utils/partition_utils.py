"""Utilities for partition publish workflow using S3-compatible storage."""

from __future__ import annotations

import json
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dataclass(frozen=True)
class PartitionPaths:
    """Resolved locations for a single partition commit."""

    canonical_prefix: str
    tmp_prefix: str
    manifest_path: str
    success_flag_path: str


def _strip_slashes(path: str) -> str:
    return path.strip("/")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://`` URI into bucket and key components."""

    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")

    remainder = uri[len("s3://") :]
    if "/" not in remainder:
        raise ValueError(f"S3 URI missing key: {uri}")

    bucket, key = remainder.split("/", 1)
    if not bucket or not key:
        raise ValueError(f"S3 URI missing bucket or key: {uri}")

    return bucket, key


def build_partition_paths(
    base_prefix: str, partition_date: str, run_id: str, bucket_name: str
) -> PartitionPaths:
    """Construct canonical and temporary prefixes for a partition run.

    Args:
        base_prefix: Base key prefix without bucket (for example ``dw/ods/table_name``).
        partition_date: Partition value in ``YYYY-MM-DD`` format.
        run_id: Unique identifier for this execution (used in tmp prefix).
        bucket_name: Target bucket name.

    Returns:
        ``PartitionPaths`` with canonical/tmp prefixes and marker file paths.
    """

    if not partition_date:
        raise ValueError("partition_date is required")
    if not run_id:
        raise ValueError("run_id is required")
    if not bucket_name:
        raise ValueError("bucket_name is required")

    normalized_base = _strip_slashes(base_prefix)
    canonical_prefix = f"s3://{bucket_name}/{normalized_base}/dt={partition_date}"
    tmp_prefix = f"s3://{bucket_name}/{normalized_base}/_tmp/run_{run_id}/dt={partition_date}"
    manifest_path = f"{canonical_prefix}/manifest.json"
    success_flag_path = f"{canonical_prefix}/_SUCCESS"

    return PartitionPaths(
        canonical_prefix=canonical_prefix,
        tmp_prefix=tmp_prefix,
        manifest_path=manifest_path,
        success_flag_path=success_flag_path,
    )


def build_manifest(
    *,
    dest: str,
    partition_date: str,
    run_id: str,
    file_count: int,
    row_count: int,
    source_prefix: str,
    target_prefix: str,
    status: str = "success",
    generated_at: str | None = None,
) -> MutableMapping[str, object]:
    """Compose manifest metadata for a committed partition."""

    if file_count < 0:
        raise ValueError("file_count cannot be negative")
    if row_count < 0:
        raise ValueError("row_count cannot be negative")

    timestamp = generated_at or datetime.now(UTC).isoformat()

    return {
        "dest": dest,
        "partition_date": partition_date,
        "run_id": run_id,
        "file_count": file_count,
        "row_count": row_count,
        "status": status,
        "source_prefix": source_prefix,
        "target_prefix": target_prefix,
        "generated_at": timestamp,
    }


def _delete_prefix(s3_hook: S3Hook, prefix_uri: str) -> None:
    bucket, key_prefix = parse_s3_uri(prefix_uri)
    normalized_prefix = _strip_slashes(key_prefix) + "/"
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
    if keys:
        s3_hook.delete_objects(bucket=bucket, keys=keys)


def _copy_prefix(s3_hook: S3Hook, source_uri: str, dest_uri: str) -> None:
    src_bucket, src_prefix = parse_s3_uri(source_uri)
    dest_bucket, dest_prefix = parse_s3_uri(dest_uri)

    normalized_src = _strip_slashes(src_prefix) + "/"
    normalized_dest = _strip_slashes(dest_prefix) + "/"

    keys = s3_hook.list_keys(bucket_name=src_bucket, prefix=normalized_src) or []
    for key in keys:
        if not key.startswith(normalized_src):
            continue
        relative_key = key[len(normalized_src) :]
        dest_key = normalized_dest + relative_key
        s3_hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=dest_key,
            source_bucket_name=src_bucket,
            dest_bucket_name=dest_bucket,
        )


def _write_json(s3_hook: S3Hook, content: Mapping[str, object], target_uri: str) -> str:
    bucket, key = parse_s3_uri(target_uri)
    payload = json.dumps(content, sort_keys=True)
    s3_hook.load_string(string_data=payload, key=key, bucket_name=bucket, replace=True)
    return target_uri


def _write_success_flag(s3_hook: S3Hook, target_uri: str) -> str:
    bucket, key = parse_s3_uri(target_uri)
    s3_hook.load_string(string_data="", key=key, bucket_name=bucket, replace=True)
    return target_uri


def publish_partition(
    *,
    s3_hook: S3Hook,
    paths: PartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    """Publish a partition by promoting tmp outputs and writing markers."""

    _delete_prefix(s3_hook, paths.canonical_prefix)
    _copy_prefix(s3_hook, paths.tmp_prefix, paths.canonical_prefix)

    manifest_path = _write_json(s3_hook, manifest, paths.manifest_path)
    success_path = None
    if write_success_flag:
        success_path = _write_success_flag(s3_hook, paths.success_flag_path)

    result = {"manifest_path": manifest_path}
    if success_path:
        result["success_flag_path"] = success_path
    return result
