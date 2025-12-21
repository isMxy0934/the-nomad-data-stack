from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping

from lakehouse_core.api import publish_output
from lakehouse_core.domain.manifest import build_manifest
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore

logger = logging.getLogger(__name__)


def commit(
    *,
    store: ObjectStore,
    paths: PartitionPaths | NonPartitionPaths,
    dest: str,
    run_id: str,
    partition_date: str,
    metrics: Mapping[str, int],
    write_success_flag: bool = True,
) -> tuple[dict[str, str], MutableMapping[str, object]]:
    """Commit tmp outputs to canonical.

    For has_data=0, this clears canonical prefix and does NOT write manifest/_SUCCESS.
    """

    if not int(metrics.get("has_data", 1)):
        log_event(
            logger,
            "pipeline.commit",
            dest=dest,
            dt=partition_date,
            run_id=run_id,
            status="no_data",
            action="cleared",
        )
        store.delete_prefix(paths.canonical_prefix)
        return {"published": "0", "action": "cleared"}, {}

    manifest = build_manifest(
        dest=dest,
        partition_date=partition_date,
        run_id=run_id,
        file_count=int(metrics["file_count"]),
        row_count=int(metrics["row_count"]),
        source_prefix=getattr(paths, "tmp_partition_prefix", paths.tmp_prefix),
        target_prefix=paths.canonical_prefix,
    )
    publish_result = publish_output(
        store=store, paths=paths, manifest=manifest, write_success_flag=write_success_flag
    )
    log_event(
        logger,
        "pipeline.commit",
        dest=dest,
        dt=partition_date,
        run_id=run_id,
        status="success",
        published=publish_result.get("manifest_path", ""),
    )
    return dict(publish_result), manifest
