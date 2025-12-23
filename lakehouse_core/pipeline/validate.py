from __future__ import annotations

import logging
from collections.abc import Mapping

from lakehouse_core.domain.observability import log_event
from lakehouse_core.domain.validate import validate_output_core
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore

logger = logging.getLogger(__name__)


def validate(
    *,
    store: ObjectStore,
    paths: PartitionPaths | NonPartitionPaths,
    metrics: Mapping[str, int],
    file_format: str = "parquet",
) -> dict[str, int]:
    """Validate tmp output and log the result.

    Calls domain.validate.validate_output_core for core logic.
    """
    result = validate_output_core(
        store=store,
        paths=paths,
        metrics=metrics,
        file_format=file_format,
    )

    has_data = int(metrics.get("has_data", 1))
    log_event(
        logger,
        "pipeline.validate",
        status="no_data" if not has_data else "ok",
        file_count=result.get("file_count", 0),
        row_count=result.get("row_count", 0),
        tmp_prefix=paths.tmp_prefix,
        file_format=file_format,
    )
    return result
