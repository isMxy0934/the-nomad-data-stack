import json

from lakehouse_core import commit as core_commit
from lakehouse_core import manifest as core_manifest
from lakehouse_core import paths as core_paths
from lakehouse_core.testing.local_store import LocalS3StyleStore


def test_publish_partition_local_store_writes_markers_and_copies_files(tmp_path):
    store = LocalS3StyleStore(tmp_path)

    paths = core_paths.build_partition_paths(
        base_uri="s3://bucket",
        base_prefix="lake/ods/table",
        partition_date="2024-03-01",
        run_id="abc123",
    )

    store.write_text(f"{paths.tmp_partition_prefix}/file_1.parquet", "x")
    store.write_text(f"{paths.tmp_partition_prefix}/file_2.parquet", "y")

    manifest = core_manifest.build_manifest(
        dest="ods_table",
        partition_date=paths.partition_date,
        run_id="abc123",
        file_count=2,
        row_count=20,
        source_prefix=paths.tmp_partition_prefix,
        target_prefix=paths.canonical_prefix,
    )

    result = core_commit.publish_partition(store=store, paths=paths, manifest=manifest)

    assert result["manifest_path"] == paths.manifest_path
    assert result["success_flag_path"] == paths.success_flag_path

    assert store.exists(f"{paths.canonical_prefix}/file_1.parquet")
    assert store.exists(f"{paths.canonical_prefix}/file_2.parquet")
    assert store.exists(paths.manifest_path)
    assert store.exists(paths.success_flag_path)

    manifest_payload = json.loads(store.read_bytes(paths.manifest_path).decode("utf-8"))
    assert manifest_payload["dest"] == "ods_table"
    assert manifest_payload["partition_date"] == "2024-03-01"
    assert manifest_payload["file_count"] == 2

    op_names = [op.name for op in store.ops]
    assert "delete_prefix" in op_names
    assert "copy_prefix" in op_names
    assert op_names.index("delete_prefix") < op_names.index("copy_prefix")


def test_publish_non_partition_local_store_writes_markers_and_copies_files(tmp_path):
    store = LocalS3StyleStore(tmp_path)

    paths = core_paths.build_non_partition_paths(
        base_uri="s3://bucket",
        base_prefix="lake/dim/table",
        run_id="abc123",
    )

    store.write_text(f"{paths.tmp_prefix}/file_1.parquet", "x")
    store.write_text(f"{paths.tmp_prefix}/file_2.parquet", "y")

    manifest = core_manifest.build_manifest(
        dest="dim_table",
        partition_date="2024-03-01",
        run_id="abc123",
        file_count=2,
        row_count=20,
        source_prefix=paths.tmp_prefix,
        target_prefix=paths.canonical_prefix,
    )

    result = core_commit.publish_non_partition(store=store, paths=paths, manifest=manifest)

    assert result["manifest_path"] == paths.manifest_path
    assert result["success_flag_path"] == paths.success_flag_path

    assert store.exists(f"{paths.canonical_prefix}/file_1.parquet")
    assert store.exists(f"{paths.canonical_prefix}/file_2.parquet")
    assert store.exists(paths.manifest_path)
    assert store.exists(paths.success_flag_path)
