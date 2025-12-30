from __future__ import annotations

from pathlib import Path

from lakehouse_core.ingestion.runner import run_ingestion_config
from lakehouse_core.testing.local_store import LocalS3StyleStore


def test_runner_writes_partitions(tmp_path: Path):
    store_root = tmp_path / "store"
    store = LocalS3StyleStore(store_root)

    config = {
        "target": "sample_target",
        "partitioner": {"class": "lakehouse_core.ingestion.partitioners.SingleJobPartitioner"},
        "extractor": {
            "class": "lakehouse_core.ingestion.extractors.SimpleFunctionExtractor",
            "kwargs": {
                "function_ref": "lakehouse_core.testing.ingestion_fixtures:sample_fetch",
            },
        },
        "compactor": {
            "class": "lakehouse_core.ingestion.compactors.DuckDBCompactor",
            "kwargs": {
                "bucket": "test-bucket",
                "prefix_template": "lake/raw/daily/sample_target",
                "file_format": "csv",
                "dedup_cols": ["symbol", "trade_date"],
                "partition_column": "trade_date",
            },
        },
    }

    result = run_ingestion_config(
        config=config,
        start_date="2024-01-01",
        end_date="2024-01-02",
        run_id="test_run",
        local_tmp_dir=tmp_path / "local_tmp",
        store=store,
        base_uri="s3://test-bucket",
    )

    assert result.status == "ok"
    success_1 = "s3://test-bucket/lake/raw/daily/sample_target/dt=2024-01-01/_SUCCESS"
    success_2 = "s3://test-bucket/lake/raw/daily/sample_target/dt=2024-01-02/_SUCCESS"
    assert store.exists(success_1)
    assert store.exists(success_2)
