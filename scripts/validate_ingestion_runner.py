from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakehouse_core.ingestion.runner import run_ingestion_config
from lakehouse_core.store import LocalFileStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate ingestion runner locally.")
    parser.add_argument("--local-root", type=Path, default=Path(".local_lake"))
    parser.add_argument("--local-tmp", type=Path, default=Path(".local_tmp"))
    parser.add_argument("--start-date", type=str, default="2024-01-01")
    parser.add_argument("--end-date", type=str, default="2024-01-02")
    parser.add_argument("--write-mode", type=str, default="overwrite")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    store_root = args.local_root.resolve()
    store = LocalFileStore(root_dir=store_root)
    base_uri = store_root.as_uri()

    config = {
        "target": "validate_sample",
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
                "bucket": "local",
                "prefix_template": "lake/raw/daily/validate_sample",
                "file_format": "csv",
                "dedup_cols": ["symbol", "trade_date"],
                "partition_column": "trade_date",
            },
        },
    }

    result = run_ingestion_config(
        config=config,
        start_date=args.start_date,
        end_date=args.end_date,
        run_id="validate_run",
        write_mode=args.write_mode,
        local_tmp_dir=args.local_tmp,
        store=store,
        base_uri=base_uri,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
