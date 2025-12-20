from lakehouse_core import prepare_paths


def test_prepare_paths_partitioned_uses_store_namespace_bucket():
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )
    assert paths.canonical_prefix == "s3://bucket/lake/ods/t/dt=2024-01-01"


def test_prepare_paths_non_partitioned_uses_store_namespace_uri():
    paths = prepare_paths(
        base_prefix="lake/dim/t",
        run_id="r1",
        partition_date=None,
        is_partitioned=False,
        store_namespace="s3://bucket",
    )
    assert paths.canonical_prefix == "s3://bucket/lake/dim/t"


def test_cleanup_tmp_deletes_tmp_prefix(tmp_path):
    from lakehouse_core import cleanup_tmp
    from lakehouse_core.testing.local_store import LocalS3StyleStore

    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )
    store.write_bytes(f"{paths.tmp_partition_prefix}/a.parquet", b"x")
    store.write_bytes(f"{paths.tmp_partition_prefix}/b.parquet", b"y")

    cleanup_tmp(store=store, paths=paths)
    assert store.list_keys(paths.tmp_prefix) == []


def test_validate_output_checks_file_count(tmp_path):
    from lakehouse_core import validate_output
    from lakehouse_core.testing.local_store import LocalS3StyleStore

    store = LocalS3StyleStore(tmp_path)
    paths = prepare_paths(
        base_prefix="lake/ods/t",
        run_id="r1",
        partition_date="2024-01-01",
        is_partitioned=True,
        store_namespace="bucket",
    )

    store.write_bytes(f"{paths.tmp_partition_prefix}/a.parquet", b"x")
    store.write_bytes(f"{paths.tmp_partition_prefix}/b.parquet", b"y")

    metrics = {"has_data": 1, "file_count": 2, "row_count": 10}
    assert validate_output(store=store, paths=paths, metrics=metrics) == metrics
