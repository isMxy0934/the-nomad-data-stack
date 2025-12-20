from __future__ import annotations

import logging
import json
from collections.abc import Mapping

from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.observability import log_event, manifest_log_fields
from lakehouse_core.storage import ObjectStore

logger = logging.getLogger(__name__)


def delete_prefix(store: ObjectStore, prefix_uri: str) -> None:
    store.delete_prefix(prefix_uri)


def publish_partition(
    *,
    store: ObjectStore,
    paths: PartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    log_event(
        logger,
        "core.publish_partition",
        **manifest_log_fields(manifest),
        canonical_prefix=paths.canonical_prefix,
        tmp_prefix=paths.tmp_partition_prefix,
        stage="delete_prefix",
    )
    store.delete_prefix(paths.canonical_prefix)
    log_event(
        logger,
        "core.publish_partition",
        **manifest_log_fields(manifest),
        canonical_prefix=paths.canonical_prefix,
        tmp_prefix=paths.tmp_partition_prefix,
        stage="copy_prefix",
    )
    store.copy_prefix(paths.tmp_partition_prefix, paths.canonical_prefix)

    payload = json.dumps(dict(manifest), sort_keys=True)
    store.write_text(paths.manifest_path, payload)
    result: dict[str, str] = {"manifest_path": paths.manifest_path}

    if write_success_flag:
        store.write_text(paths.success_flag_path, "")
        result["success_flag_path"] = paths.success_flag_path
    log_event(
        logger,
        "core.publish_partition",
        **manifest_log_fields(manifest),
        canonical_prefix=paths.canonical_prefix,
        manifest_path=paths.manifest_path,
        success_flag_path=paths.success_flag_path if write_success_flag else "",
        stage="done",
    )
    return result


def publish_non_partition(
    *,
    store: ObjectStore,
    paths: NonPartitionPaths,
    manifest: Mapping[str, object],
    write_success_flag: bool = True,
) -> Mapping[str, str]:
    log_event(
        logger,
        "core.publish_non_partition",
        **manifest_log_fields(manifest),
        canonical_prefix=paths.canonical_prefix,
        tmp_prefix=paths.tmp_prefix,
        stage="delete_objects",
    )
    # Non-partitioned tables keep tmp outputs under the canonical prefix. A naive
    # delete_prefix would delete tmp before we can copy. Delete everything under
    # canonical except the `_tmp/` subtree.
    excluded_prefix = f"{paths.canonical_prefix}/_tmp/"
    candidates = store.list_keys(paths.canonical_prefix)
    to_delete = [uri for uri in candidates if not uri.startswith(excluded_prefix)]
    if to_delete:
        store.delete_objects(to_delete)
    log_event(
        logger,
        "core.publish_non_partition",
        **manifest_log_fields(manifest),
        canonical_prefix=paths.canonical_prefix,
        tmp_prefix=paths.tmp_prefix,
        stage="copy_prefix",
    )
    store.copy_prefix(paths.tmp_prefix, paths.canonical_prefix)

    payload = json.dumps(dict(manifest), sort_keys=True)
    store.write_text(paths.manifest_path, payload)
    result: dict[str, str] = {"manifest_path": paths.manifest_path}

    if write_success_flag:
        store.write_text(paths.success_flag_path, "")
        result["success_flag_path"] = paths.success_flag_path
    log_event(
        logger,
        "core.publish_non_partition",
        **manifest_log_fields(manifest),
        canonical_prefix=paths.canonical_prefix,
        manifest_path=paths.manifest_path,
        success_flag_path=paths.success_flag_path if write_success_flag else "",
        stage="done",
    )
    return result
