from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore


def _count_files(store: ObjectStore, prefix_uri: str, file_format: str) -> int:
    """Count files with given extension in prefix."""
    extension = f".{file_format}"
    return len([uri for uri in store.list_keys(prefix_uri) if uri.endswith(extension)])


def validate_output_core(
    *,
    store: ObjectStore,
    paths: PartitionPaths | NonPartitionPaths,
    metrics: Mapping[str, int],
    file_format: str = "parquet",
) -> dict[str, int]:
    """Core validation logic for tmp output based on expected metrics.

    Args:
        store: Object store to list files
        paths: Temporary paths for validation
        metrics: Expected metrics (row_count, file_count, has_data)
        file_format: File format to validate ("parquet" or "csv")

    Returns:
        Validated metrics dict
    """
    has_data = int(metrics.get("has_data", 1))
    if not has_data:
        count = _count_files(store, paths.tmp_prefix, file_format)
        if count != 0:
            raise ValueError(
                f"Expected no {file_format} files in tmp prefix for empty source result"
            )
        return dict(metrics)

    expected_files = int(metrics["file_count"])
    actual_files = (
        _count_files(store, paths.tmp_partition_prefix, file_format)
        if isinstance(paths, PartitionPaths)
        else _count_files(store, paths.tmp_prefix, file_format)
    )

    if actual_files == 0:
        raise ValueError(f"No {file_format} files were written to the tmp prefix")
    if actual_files != expected_files:
        raise ValueError(
            f"File count mismatch between load metrics and store contents: "
            f"expected {expected_files}, found {actual_files}"
        )
    if int(metrics["row_count"]) < 0:
        raise ValueError("Row count cannot be negative")

    return dict(metrics)


# Legacy function for backwards compatibility
def validate_dataset(
    paths_dict: Mapping[str, object],
    metrics: Mapping[str, int],
    store: ObjectStore,
) -> dict[str, int]:
    """Validate dataset output based on expected metrics (legacy API)."""
    partitioned = bool(paths_dict.get("partitioned"))

    if not int(metrics.get("has_data", 1)):
        file_count = _count_files(store, str(paths_dict["tmp_prefix"]), "parquet")
        if file_count != 0:
            raise ValueError("Expected no parquet files in tmp prefix for empty source result")
        return dict(metrics)

    target_prefix = (
        str(paths_dict["tmp_partition_prefix"]) if partitioned else str(paths_dict["tmp_prefix"])
    )
    file_count = _count_files(store, target_prefix, "parquet")

    if file_count == 0:
        raise ValueError("No parquet files were written to the tmp prefix")
    if file_count != int(metrics["file_count"]):
        raise ValueError("File count mismatch between load metrics and store contents")
    if int(metrics["row_count"]) < 0:
        raise ValueError("Row count cannot be negative")

    return dict(metrics)
