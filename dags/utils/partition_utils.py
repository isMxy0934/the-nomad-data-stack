"""Utilities for partition publish workflow using S3-compatible storage.

This module is an Airflow-facing adapter layer. Core commit protocol logic lives
in `lakehouse_core` and is orchestrator-agnostic.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters.airflow_s3_store import AirflowS3Store
from lakehouse_core import api as core_api
from lakehouse_core import manifest as core_manifest
from lakehouse_core import paths as core_paths
from lakehouse_core import uri as core_uri

PartitionPaths = core_paths.PartitionPaths
NonPartitionPaths = core_paths.NonPartitionPaths


def _strip_slashes(path: str) -> str:
    return path.strip("/")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://`` URI into bucket and key components."""

    return core_uri.parse_s3_uri(uri)


def build_partition_paths(
    base_prefix: str, partition_date: str, run_id: str, bucket_name: str
) -> PartitionPaths:
    """Construct canonical and temporary prefixes for a partition run.

    Args:
        base_prefix: Base key prefix without bucket (for example ``lake/ods/table_name``).
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

    return core_api.prepare_paths(
        base_prefix=_strip_slashes(base_prefix),
        run_id=run_id,
        partition_date=partition_date,
        is_partitioned=True,
        store_namespace=bucket_name,
    )


def build_non_partition_paths(
    *, base_prefix: str, run_id: str, bucket_name: str
) -> NonPartitionPaths:
    """Construct canonical and temporary prefixes for non-partitioned tables."""

    if not run_id:
        raise ValueError("run_id is required")
    if not bucket_name:
        raise ValueError("bucket_name is required")

    return core_api.prepare_paths(
        base_prefix=_strip_slashes(base_prefix),
        run_id=run_id,
        partition_date=None,
        is_partitioned=False,
        store_namespace=bucket_name,
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

    return core_manifest.build_manifest(
        dest=dest,
        partition_date=partition_date,
        run_id=run_id,
        file_count=file_count,
        row_count=row_count,
        source_prefix=source_prefix,
        target_prefix=target_prefix,
        status=status,
        generated_at=generated_at,
    )


def delete_prefix(s3_hook: S3Hook, prefix_uri: str) -> None:
    store = AirflowS3Store(s3_hook)
    store.delete_prefix(prefix_uri)


def publish_partition(
    *,
    s3_hook: S3Hook,
    paths: PartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    """Publish a partition by promoting tmp outputs and writing markers."""

    store = AirflowS3Store(s3_hook)
    return core_api.publish_output(
        store=store,
        paths=paths,
        manifest=manifest,
        write_success_flag=write_success_flag,
    )


def publish_non_partition(
    *,
    s3_hook: S3Hook,
    paths: NonPartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    """Publish non-partitioned outputs by promoting tmp contents."""

    store = AirflowS3Store(s3_hook)
    return core_api.publish_output(
        store=store,
        paths=paths,
        manifest=manifest,
        write_success_flag=write_success_flag,
    )
