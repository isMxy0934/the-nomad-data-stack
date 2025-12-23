from __future__ import annotations

from dataclasses import dataclass

from lakehouse_core.io.uri import join_uri, strip_slashes


@dataclass(frozen=True)
class PartitionPaths:
    """Resolved locations for a single partition commit."""

    partition_date: str
    canonical_prefix: str
    tmp_prefix: str
    tmp_partition_prefix: str
    manifest_path: str
    success_flag_path: str


@dataclass(frozen=True)
class NonPartitionPaths:
    """Resolved locations for non-partitioned tables."""

    canonical_prefix: str
    tmp_prefix: str
    manifest_path: str
    success_flag_path: str


def build_partition_paths(
    *, base_uri: str, base_prefix: str, partition_date: str, run_id: str
) -> PartitionPaths:
    if not partition_date:
        raise ValueError("partition_date is required")
    if not run_id:
        raise ValueError("run_id is required")
    if not base_uri:
        raise ValueError("base_uri is required")

    normalized_base = strip_slashes(base_prefix)
    canonical_prefix = join_uri(base_uri, f"{normalized_base}/dt={partition_date}")
    tmp_prefix = join_uri(base_uri, f"{normalized_base}/_tmp/run_{run_id}")
    tmp_partition_prefix = f"{tmp_prefix}/dt={partition_date}"
    manifest_path = f"{canonical_prefix}/manifest.json"
    success_flag_path = f"{canonical_prefix}/_SUCCESS"

    return PartitionPaths(
        partition_date=partition_date,
        canonical_prefix=canonical_prefix,
        tmp_prefix=tmp_prefix,
        tmp_partition_prefix=tmp_partition_prefix,
        manifest_path=manifest_path,
        success_flag_path=success_flag_path,
    )


def build_non_partition_paths(*, base_uri: str, base_prefix: str, run_id: str) -> NonPartitionPaths:
    if not run_id:
        raise ValueError("run_id is required")
    if not base_uri:
        raise ValueError("base_uri is required")

    normalized_base = strip_slashes(base_prefix)
    canonical_prefix = join_uri(base_uri, normalized_base)
    tmp_prefix = join_uri(base_uri, f"{normalized_base}/_tmp/run_{run_id}")
    manifest_path = f"{canonical_prefix}/manifest.json"
    success_flag_path = f"{canonical_prefix}/_SUCCESS"

    return NonPartitionPaths(
        canonical_prefix=canonical_prefix,
        tmp_prefix=tmp_prefix,
        manifest_path=manifest_path,
        success_flag_path=success_flag_path,
    )


def paths_to_dict(
    paths: PartitionPaths | NonPartitionPaths,
    *,
    partition_date: str | None = None,
) -> dict[str, object]:
    """Convert paths object to XCom-serializable dict.

    This is the canonical way to serialize paths for Airflow XCom or similar.

    Args:
        paths: PartitionPaths or NonPartitionPaths object
        partition_date: Override partition_date for non-partitioned paths (optional)

    Returns:
        JSON-serializable dict suitable for XCom
    """
    if isinstance(paths, PartitionPaths):
        return {
            "partitioned": True,
            "partition_date": paths.partition_date,
            "canonical_prefix": paths.canonical_prefix,
            "tmp_prefix": paths.tmp_prefix,
            "tmp_partition_prefix": paths.tmp_partition_prefix,
            "manifest_path": paths.manifest_path,
            "success_flag_path": paths.success_flag_path,
        }
    if isinstance(paths, NonPartitionPaths):
        return {
            "partitioned": False,
            "partition_date": str(partition_date or ""),
            "canonical_prefix": paths.canonical_prefix,
            "tmp_prefix": paths.tmp_prefix,
            "manifest_path": paths.manifest_path,
            "success_flag_path": paths.success_flag_path,
        }
    raise TypeError(f"Unexpected paths type: {type(paths)}")
