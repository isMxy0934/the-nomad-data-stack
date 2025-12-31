from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from lakehouse_core.ingestion.runner import run_ingestion_config, run_ingestion_configs
from lakehouse_core.store import Boto3S3Store, LocalFileStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ingestion without Airflow.")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--config-dir", type=Path, default=Path("dags/ingestion/configs"))
    parser.add_argument("--target", action="append", default=None)

    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--partition-date", type=str, default=None)
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--write-mode", type=str, default="overwrite")
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--local-tmp", type=Path, default=None)

    parser.add_argument("--store", choices=["s3", "local"], default="s3")
    parser.add_argument("--bucket", type=str, default=os.getenv("S3_BUCKET_NAME"))
    parser.add_argument("--endpoint-url", type=str, default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--access-key", type=str, default=os.getenv("S3_ACCESS_KEY_ID"))
    parser.add_argument("--secret-key", type=str, default=os.getenv("S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--region", type=str, default=os.getenv("S3_REGION", "us-east-1"))
    parser.add_argument("--url-style", type=str, default=os.getenv("S3_URL_STYLE", "path"))
    parser.add_argument("--use-ssl", action="store_true", default=False)
    parser.add_argument("--local-root", type=Path, default=Path(".local_lake"))
    return parser


def _build_store(args: argparse.Namespace):
    if args.store == "local":
        root = args.local_root.resolve()
        store = LocalFileStore(root_dir=root)
        return store, root.as_uri()

    if not args.bucket:
        raise ValueError("--bucket is required for --store=s3")
    if not args.endpoint_url:
        raise ValueError("--endpoint-url is required for --store=s3")
    if not args.access_key or not args.secret_key:
        raise ValueError("--access-key/--secret-key are required for --store=s3")

    store = Boto3S3Store(
        endpoint_url=args.endpoint_url,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=args.region,
        use_ssl=bool(args.use_ssl),
        url_style=args.url_style,
    )
    return store, f"s3://{args.bucket}"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    store, base_uri = _build_store(args)
    targets = args.target or []

    if args.config:
        result = run_ingestion_config(
            config_path=args.config,
            start_date=args.start_date,
            end_date=args.end_date,
            partition_date=args.partition_date,
            run_id=args.run_id,
            write_mode=args.write_mode,
            max_workers=args.max_workers,
            local_tmp_dir=args.local_tmp,
            store=store,
            base_uri=base_uri,
        )
        print(result)
        return 0

    results = run_ingestion_configs(
        config_dir=args.config_dir,
        targets=targets,
        start_date=args.start_date,
        end_date=args.end_date,
        partition_date=args.partition_date,
        run_id=args.run_id,
        write_mode=args.write_mode,
        max_workers=args.max_workers,
        local_tmp_dir=args.local_tmp,
        store=store,
        base_uri=base_uri,
    )
    for res in results:
        print(res)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
