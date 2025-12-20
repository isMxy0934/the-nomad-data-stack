from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.api import validate_output
from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.storage import ObjectStore


def validate(
    *,
    store: ObjectStore,
    paths: PartitionPaths | NonPartitionPaths,
    metrics: Mapping[str, int],
) -> dict[str, int]:
    return validate_output(store=store, paths=paths, metrics=metrics)

