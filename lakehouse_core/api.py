from __future__ import annotations

import logging
from collections.abc import Mapping

from lakehouse_core.compute.execution import (
    execute_sql,
    normalize_duckdb_path,
    run_query_to_parquet,
    run_query_to_partitioned_parquet,
)
from lakehouse_core.domain import commit_protocol as core_commit
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io import paths as core_paths
from lakehouse_core.store.object_store import ObjectStore

logger = logging.getLogger(__name__)


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

    if "://" in store_namespace:
        base_uri = store_namespace.rstrip("/")
    else:
        base_uri = f"s3://{store_namespace.strip()}"

    if is_partitioned:
        if not partition_date or not str(partition_date).strip():
            raise ValueError("partition_date is required for partitioned datasets")
        paths = core_paths.build_partition_paths(
            base_uri=base_uri,
            base_prefix=base_prefix,
            partition_date=str(partition_date).strip(),
            run_id=run_id,
        )
        log_event(
            logger,
            "core.prepare_paths",
            base_prefix=base_prefix,
            dt=paths.partition_date,
            run_id=run_id,
            is_partitioned=True,
            canonical_prefix=paths.canonical_prefix,
            tmp_prefix=paths.tmp_prefix,
        )
        return paths

    paths = core_paths.build_non_partition_paths(
        base_uri=base_uri,
        base_prefix=base_prefix,
        run_id=run_id,
    )
    log_event(
        logger,
        "core.prepare_paths",
        base_prefix=base_prefix,
        run_id=run_id,
        is_partitioned=False,
        canonical_prefix=paths.canonical_prefix,
        tmp_prefix=paths.tmp_prefix,
    )
    return paths


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
        log_event(
            logger,
            "core.validate_output",
            status="no_data",
            file_count=0,
            row_count=0,
            tmp_prefix=paths.tmp_prefix,
        )
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

    log_event(
        logger,
        "core.validate_output",
        status="ok",
        file_count=actual_files,
        row_count=int(metrics["row_count"]),
        tmp_prefix=paths.tmp_prefix,
    )
    return dict(metrics)


def publish_output(
    *,
    store: ObjectStore,
    paths: core_paths.PartitionPaths | core_paths.NonPartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    log_event(
        logger,
        "core.publish_output",
        status="start",
        manifest_path=getattr(paths, "manifest_path", ""),
        success_flag_path=getattr(paths, "success_flag_path", "") if write_success_flag else "",
    )
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
    log_event(logger, "core.cleanup_tmp", tmp_prefix=paths.tmp_prefix)
    store.delete_prefix(paths.tmp_prefix)


def materialize_query_to_tmp_and_measure(
    *,
    connection,
    store: ObjectStore,
    query: str,
    destination_prefix: str,
    partitioned: bool,
    tmp_partition_prefix: str | None = None,
    partition_column: str = "dt",
    filename_pattern: str = "file_{uuid}",
    use_tmp_file: bool = False,
) -> dict[str, int]:
    """Execute a query and write Parquet into tmp, then compute (file_count, row_count).

    This is orchestrator-agnostic and intended to be called by Airflow/Prefect/scripts.
    """

    if partitioned:
        run_query_to_partitioned_parquet(
            connection,
            query=query,
            destination=destination_prefix,
            partition_column=partition_column,
            filename_pattern=filename_pattern,
            use_tmp_file=use_tmp_file,
        )
        count_prefix = tmp_partition_prefix or destination_prefix
        parquet_glob = f"{count_prefix}/*.parquet"
    else:
        run_query_to_parquet(
            connection,
            query=query,
            destination=destination_prefix,
            filename_pattern=filename_pattern,
            use_tmp_file=use_tmp_file,
        )
        count_prefix = destination_prefix
        parquet_glob = f"{destination_prefix}/*.parquet"

    file_count = len([uri for uri in store.list_keys(count_prefix) if uri.endswith(".parquet")])
    if file_count == 0:
        log_event(
            logger,
            "core.materialize_query",
            status="no_data",
            partitioned=partitioned,
            destination_prefix=destination_prefix,
        )
        return {"row_count": 0, "file_count": 0, "has_data": 0}

    parquet_glob_duckdb = normalize_duckdb_path(parquet_glob)
    relation = execute_sql(
        connection, f"SELECT COUNT(*) AS row_count FROM read_parquet('{parquet_glob_duckdb}')"
    )
    row_count = int(relation.fetchone()[0])
    log_event(
        logger,
        "core.materialize_query",
        status="ok",
        partitioned=partitioned,
        destination_prefix=destination_prefix,
        file_count=file_count,
        row_count=row_count,
    )
    return {"row_count": row_count, "file_count": file_count, "has_data": 1}
