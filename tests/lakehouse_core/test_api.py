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
