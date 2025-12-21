"""Common utilities and task functions for Prefect flows."""

from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping
from typing import Any

from flows.adapters.prefect_s3_store import PrefectS3Store
from flows.utils.config import build_s3_connection_config_from_env, get_s3_bucket_name
from lakehouse_core.api import prepare_paths
from lakehouse_core.compute import S3ConnectionConfig
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.io.time import get_partition_date_str
from lakehouse_core.pipeline import cleanup as pipeline_cleanup
from lakehouse_core.pipeline import commit as pipeline_commit
from lakehouse_core.pipeline import validate as pipeline_validate

logger = logging.getLogger(__name__)


def build_s3_connection_config() -> S3ConnectionConfig:
    return build_s3_connection_config_from_env()


def get_default_bucket_name() -> str:
    return get_s3_bucket_name()


def _strip_slashes(path: str) -> str:
    return path.strip("/")


def partition_paths_from_payload(paths_dict: Mapping[str, Any]) -> PartitionPaths:
    """Reconstruct ``PartitionPaths`` from task payloads produced by ``prepare_dataset``."""

    return PartitionPaths(
        partition_date=str(paths_dict["partition_date"]),
        canonical_prefix=str(paths_dict["canonical_prefix"]),
        tmp_prefix=str(paths_dict["tmp_prefix"]),
        tmp_partition_prefix=str(paths_dict["tmp_partition_prefix"]),
        manifest_path=str(paths_dict["manifest_path"]),
        success_flag_path=str(paths_dict["success_flag_path"]),
    )


def non_partition_paths_from_payload(paths_dict: Mapping[str, Any]) -> NonPartitionPaths:
    """Reconstruct ``NonPartitionPaths`` from task payloads produced by ``prepare_dataset``."""

    return NonPartitionPaths(
        canonical_prefix=str(paths_dict["canonical_prefix"]),
        tmp_prefix=str(paths_dict["tmp_prefix"]),
        manifest_path=str(paths_dict["manifest_path"]),
        success_flag_path=str(paths_dict["success_flag_path"]),
    )


def prepare_dataset(
    base_prefix: str,
    run_id: str,
    is_partitioned: bool = True,
    bucket_name: str | None = None,
    partition_date: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    """Prepare generic paths for a dataset write operation."""

    if partition_date is None or str(partition_date).strip() in {"", "None", "null"}:
        partition_date = get_partition_date_str()
    else:
        partition_date = str(partition_date).strip()

    bucket = bucket_name or get_default_bucket_name()

    if is_partitioned:
        paths = prepare_paths(
            base_prefix=_strip_slashes(base_prefix),
            run_id=run_id,
            partition_date=partition_date,
            is_partitioned=True,
            store_namespace=bucket,
        )
        return {
            "partition_date": partition_date,
            "partitioned": True,
            "canonical_prefix": paths.canonical_prefix,
            "tmp_prefix": paths.tmp_prefix,
            "tmp_partition_prefix": paths.tmp_partition_prefix,
            "manifest_path": paths.manifest_path,
            "success_flag_path": paths.success_flag_path,
        }

    paths = prepare_paths(
        base_prefix=_strip_slashes(base_prefix),
        run_id=run_id,
        partition_date=None,
        is_partitioned=False,
        store_namespace=bucket,
    )
    return {
        "partition_date": partition_date,
        "partitioned": False,
        "canonical_prefix": paths.canonical_prefix,
        "tmp_prefix": paths.tmp_prefix,
        "manifest_path": paths.manifest_path,
        "success_flag_path": paths.success_flag_path,
    }


def validate_dataset(
    paths_dict: Mapping[str, Any],
    metrics: Mapping[str, int],
) -> dict[str, int]:
    """Core logic to validate dataset output (delegates to lakehouse_core API)."""

    store = PrefectS3Store()
    partitioned = bool(paths_dict.get("partitioned"))

    if partitioned:
        try:
            paths = partition_paths_from_payload(paths_dict)
        except KeyError:
            tmp_prefix = str(paths_dict["tmp_prefix"])
            partition_date = str(paths_dict.get("partition_date") or "").strip()
            tmp_partition_prefix = str(paths_dict.get("tmp_partition_prefix") or "").strip()
            if not tmp_partition_prefix and partition_date:
                tmp_partition_prefix = f"{tmp_prefix}/dt={partition_date}"
            paths = PartitionPaths(
                partition_date=partition_date,
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=tmp_prefix,
                tmp_partition_prefix=tmp_partition_prefix,
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )
    else:
        try:
            paths = non_partition_paths_from_payload(paths_dict)
        except KeyError:
            paths = NonPartitionPaths(
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=str(paths_dict["tmp_prefix"]),
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )
    return pipeline_validate(store=store, paths=paths, metrics=metrics)


def commit_dataset(
    dest_name: str,
    run_id: str,
    paths_dict: Mapping[str, Any],
    metrics: Mapping[str, int],
) -> tuple[dict[str, str], MutableMapping[str, object]]:
    """Commit tmp outputs into canonical and write markers (pipeline-backed)."""

    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = str(paths_dict.get("partition_date"))

    store = PrefectS3Store()
    if partitioned:
        paths = partition_paths_from_payload(paths_dict)
    else:
        paths = non_partition_paths_from_payload(paths_dict)

    publish_result, manifest = pipeline_commit(
        store=store,
        paths=paths,
        dest=dest_name,
        run_id=run_id,
        partition_date=partition_date,
        metrics=metrics,
        write_success_flag=True,
    )
    if publish_result.get("action") == "cleared":
        logger.info(
            "No data for %s (dt=%s); clearing partition and skipping publish.",
            dest_name,
            partition_date,
        )
    return publish_result, manifest


def cleanup_dataset(paths_dict: Mapping[str, Any]) -> None:
    """Clean up temporary files (delegates to lakehouse_core API)."""

    store = PrefectS3Store()
    tmp_prefix = str(paths_dict["tmp_prefix"])
    partitioned = bool(paths_dict.get("partitioned"))
    if partitioned:
        try:
            paths = partition_paths_from_payload(paths_dict)
        except KeyError:
            paths = PartitionPaths(
                partition_date=str(paths_dict.get("partition_date") or ""),
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=tmp_prefix,
                tmp_partition_prefix=str(paths_dict.get("tmp_partition_prefix") or ""),
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )
    else:
        try:
            paths = non_partition_paths_from_payload(paths_dict)
        except KeyError:
            paths = NonPartitionPaths(
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=tmp_prefix,
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )

    pipeline_cleanup(store=store, paths=paths)
