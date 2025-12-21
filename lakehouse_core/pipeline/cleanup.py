from __future__ import annotations

from lakehouse_core.api import cleanup_tmp
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore


def cleanup(*, store: ObjectStore, paths: PartitionPaths | NonPartitionPaths) -> None:
    cleanup_tmp(store=store, paths=paths)
