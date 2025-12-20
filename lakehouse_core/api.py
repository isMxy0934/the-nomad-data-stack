from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core import commit as core_commit
from lakehouse_core import paths as core_paths
from lakehouse_core.storage import ObjectStore


def prepare_paths(
    *,
    base_prefix: str,
    run_id: str,
    partition_date: str | None,
    is_partitioned: bool,
    store_namespace: str,
) -> core_paths.PartitionPaths | core_paths.NonPartitionPaths:
    """Build canonical/tmp/marker paths for a dataset run.

    Phase 1 convention: ``store_namespace`` is either an ``s3://bucket`` URI or a bucket name.
    """

    if store_namespace.startswith("s3://"):
        base_uri = store_namespace.rstrip("/")
    else:
        base_uri = f"s3://{store_namespace.strip()}"

    if is_partitioned:
        if not partition_date or not str(partition_date).strip():
            raise ValueError("partition_date is required for partitioned datasets")
        return core_paths.build_partition_paths(
            base_uri=base_uri,
            base_prefix=base_prefix,
            partition_date=str(partition_date).strip(),
            run_id=run_id,
        )

    return core_paths.build_non_partition_paths(
        base_uri=base_uri,
        base_prefix=base_prefix,
        run_id=run_id,
    )


def validate_output(
    *,
    store: ObjectStore,
    paths: core_paths.PartitionPaths | core_paths.NonPartitionPaths,
    metrics: Mapping[str, int],
) -> dict[str, int]:
    """Validate tmp output based on expected metrics.

    This mirrors the Phase 1 behavior in `dags/utils/etl_utils.py`: validate tmp only.
    """

    def _parquet_count(prefix_uri: str) -> int:
        return len([uri for uri in store.list_keys(prefix_uri) if uri.endswith(".parquet")])

    has_data = int(metrics.get("has_data", 1))
    if not has_data:
        count = _parquet_count(paths.tmp_prefix)
        if count != 0:
            raise ValueError("Expected no parquet files in tmp prefix for empty source result")
        return dict(metrics)

    expected_files = int(metrics["file_count"])
    actual_files = (
        _parquet_count(paths.tmp_partition_prefix)
        if isinstance(paths, core_paths.PartitionPaths)
        else _parquet_count(paths.tmp_prefix)
    )

    if actual_files == 0:
        raise ValueError("No parquet files were written to the tmp prefix")
    if actual_files != expected_files:
        raise ValueError("File count mismatch between load metrics and store contents")
    if int(metrics["row_count"]) < 0:
        raise ValueError("Row count cannot be negative")

    return dict(metrics)


def publish_output(
    *,
    store: ObjectStore,
    paths: core_paths.PartitionPaths | core_paths.NonPartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    if isinstance(paths, core_paths.PartitionPaths):
        return core_commit.publish_partition(
            store=store,
            paths=paths,
            manifest=manifest,
            write_success_flag=write_success_flag,
        )
    return core_commit.publish_non_partition(
        store=store,
        paths=paths,
        manifest=manifest,
        write_success_flag=write_success_flag,
    )


def cleanup_tmp(
    *, store: ObjectStore, paths: core_paths.PartitionPaths | core_paths.NonPartitionPaths
) -> None:
    store.delete_prefix(paths.tmp_prefix)
