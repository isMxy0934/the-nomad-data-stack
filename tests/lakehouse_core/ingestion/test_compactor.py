from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakehouse_core.ingestion.compactors import DuckDBCompactor
from lakehouse_core.testing.local_store import LocalS3StyleStore


def _write_parquet(path: Path, rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def _read_partition_csv(store: LocalS3StyleStore, prefix: str) -> str:
    keys = [uri for uri in store.list_keys(prefix) if uri.endswith(".csv")]
    assert keys, f"No CSV files found under {prefix}"
    return store.read_bytes(keys[0]).decode("utf-8")


def test_duckdb_compactor_partitions_and_dedup(tmp_path: Path):
    store_root = tmp_path / "store"
    store = LocalS3StyleStore(store_root)
    local_dir = tmp_path / "local"

    part1 = _write_parquet(
        local_dir / "results" / "part_000000.parquet",
        [
            {"symbol": "AAA", "trade_date": "20240101", "value": 1},
            {"symbol": "BBB", "trade_date": "20240102", "value": 2},
        ],
    )
    part2 = _write_parquet(
        local_dir / "results" / "part_000001.parquet",
        [{"symbol": "AAA", "trade_date": "20240101", "value": 3}],
    )

    compactor = DuckDBCompactor(
        bucket="test-bucket",
        prefix_template="lake/raw/daily/test_target",
        file_format="csv",
        dedup_cols=["symbol", "trade_date"],
        partition_column="trade_date",
    )

    result = compactor.compact(
        results=[{"local_path": str(part1)}, {"local_path": str(part2)}],
        target="test_target",
        partition_date="2024-01-03",
        store=store,
        base_uri="s3://test-bucket",
        run_id="run_001",
        local_dir=local_dir,
        write_mode="overwrite",
    )

    assert result["status"] == "partitioned"
    assert result["partition_count"] == 2

    success_1 = "s3://test-bucket/lake/raw/daily/test_target/dt=2024-01-01/_SUCCESS"
    success_2 = "s3://test-bucket/lake/raw/daily/test_target/dt=2024-01-02/_SUCCESS"
    assert store.exists(success_1)
    assert store.exists(success_2)

    data_prefix = "s3://test-bucket/lake/raw/daily/test_target/dt=2024-01-01/"
    data = _read_partition_csv(store, data_prefix)
    assert "value" in data
    assert "3" in data

    skipped = compactor.compact(
        results=[{"local_path": str(part1)}, {"local_path": str(part2)}],
        target="test_target",
        partition_date="2024-01-03",
        store=store,
        base_uri="s3://test-bucket",
        run_id="run_002",
        local_dir=local_dir,
        write_mode="skip_existing",
    )
    assert "2024-01-01" in skipped.get("skipped_partitions", [])
    assert "2024-01-02" in skipped.get("skipped_partitions", [])
