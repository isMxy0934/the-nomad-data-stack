from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.store.object_store import ObjectStore


def _list_parquet_uris(store: ObjectStore, prefix_uri: str) -> list[str]:
    uris = store.list_keys(prefix_uri)
    return [uri for uri in uris if uri.endswith(".parquet")]


def validate_dataset(
    paths_dict: Mapping[str, object],
    metrics: Mapping[str, int],
    store: ObjectStore,
) -> dict[str, int]:
    """Validate dataset output based on expected metrics."""

    partitioned = bool(paths_dict.get("partitioned"))

    if not int(metrics.get("has_data", 1)):
        file_count = len(_list_parquet_uris(store, str(paths_dict["tmp_prefix"])))
        if file_count != 0:
            raise ValueError("Expected no parquet files in tmp prefix for empty source result")
        return dict(metrics)

    target_prefix = (
        str(paths_dict["tmp_partition_prefix"]) if partitioned else str(paths_dict["tmp_prefix"])
    )
    file_count = len(_list_parquet_uris(store, target_prefix))

    if file_count == 0:
        raise ValueError("No parquet files were written to the tmp prefix")
    if file_count != int(metrics["file_count"]):
        raise ValueError("File count mismatch between load metrics and store contents")
    if int(metrics["row_count"]) < 0:
        raise ValueError("Row count cannot be negative")

    return dict(metrics)
