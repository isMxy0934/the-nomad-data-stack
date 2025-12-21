from __future__ import annotations

import logging

from lakehouse_core.api import prepare_paths
from lakehouse_core.domain import commit_protocol as core_commit
from lakehouse_core.domain.manifest import build_manifest
from lakehouse_core.testing.local_store import LocalS3StyleStore


def test_prepare_paths_emits_structured_log(caplog) -> None:
    caplog.set_level(logging.INFO)
    _ = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )
    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "core.prepare_paths" in m and "dt=2024-01-01" in m and "run_id=r1" in m for m in messages
    )


def test_publish_partition_emits_structured_log(caplog, tmp_path) -> None:
    caplog.set_level(logging.INFO)
    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )

    store.write_bytes(f"{paths.tmp_partition_prefix}/file_1.parquet", b"x")
    manifest = build_manifest(
        dest="ods.t",
        partition_date="2024-01-01",
        run_id="r1",
        file_count=1,
        row_count=1,
        source_prefix=paths.tmp_partition_prefix,
        target_prefix=paths.canonical_prefix,
    )
    core_commit.publish_partition(store=store, paths=paths, manifest=manifest)

    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "core.publish_partition" in m and "stage=done" in m and "dest=ods.t" in m for m in messages
    )
