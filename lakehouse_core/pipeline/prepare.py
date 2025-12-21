from __future__ import annotations

from typing import Any

from lakehouse_core.api import prepare_paths
from lakehouse_core.domain.models import RunSpec
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths


def prepare(
    *,
    spec: RunSpec,
    run_id: str,
    store_namespace: str,
    partition_date: str | None,
) -> dict[str, Any]:
    """Prepare canonical/tmp paths payload for orchestrators.

    Returns a JSON-serializable mapping suitable for Airflow XCom.
    """

    effective_dt = partition_date if spec.is_partitioned else None
    paths = prepare_paths(
        base_prefix=spec.base_prefix,
        run_id=run_id,
        partition_date=effective_dt,
        is_partitioned=spec.is_partitioned,
        store_namespace=store_namespace,
    )
    if isinstance(paths, PartitionPaths):
        return {
            "partition_date": paths.partition_date,
            "partitioned": True,
            "canonical_prefix": paths.canonical_prefix,
            "tmp_prefix": paths.tmp_prefix,
            "tmp_partition_prefix": paths.tmp_partition_prefix,
            "manifest_path": paths.manifest_path,
            "success_flag_path": paths.success_flag_path,
        }
    if isinstance(paths, NonPartitionPaths):
        return {
            "partition_date": str(partition_date or ""),
            "partitioned": False,
            "canonical_prefix": paths.canonical_prefix,
            "tmp_prefix": paths.tmp_prefix,
            "manifest_path": paths.manifest_path,
            "success_flag_path": paths.success_flag_path,
        }
    raise TypeError(f"Unexpected paths type: {type(paths)}")
