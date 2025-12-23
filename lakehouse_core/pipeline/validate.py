from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.api import validate_output
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore


def validate(
    *,
    store: ObjectStore,
    paths: PartitionPaths | NonPartitionPaths,
    metrics: Mapping[str, int],
    file_format: str = "parquet",
) -> dict[str, int]:
    return validate_output(store=store, paths=paths, metrics=metrics, file_format=file_format)
