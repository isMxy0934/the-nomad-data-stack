from __future__ import annotations

import logging

from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore

logger = logging.getLogger(__name__)


def cleanup(*, store: ObjectStore, paths: PartitionPaths | NonPartitionPaths) -> None:
    """Clean up temporary files after commit.

    Directly calls store.delete_prefix instead of going through api.
    """
    log_event(logger, "pipeline.cleanup", tmp_prefix=paths.tmp_prefix)
    store.delete_prefix(paths.tmp_prefix)
