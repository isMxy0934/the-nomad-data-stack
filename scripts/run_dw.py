from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from lakehouse_core.api import prepare_paths
from lakehouse_core.catalog import attach_catalog_if_available
from lakehouse_core.dw_planner import DirectoryDWPlanner
from lakehouse_core.execution import S3ConnectionConfig, configure_s3_access, temporary_connection
from lakehouse_core.inputs import OdsCsvRegistrar
from lakehouse_core.models import RunContext
from lakehouse_core.pipeline import cleanup, commit, load, validate
from lakehouse_core.stores import Boto3S3Store, LocalFileStore
from lakehouse_core.time import get_default_partition_date_str

logger = logging.getLogger(__name__)


def _split_targets(values: list[str] | None) -> list[str]:
    if not values:
        return []
    targets: list[str] = []
    for raw in values:
        if raw is None:
            continue
        for item in str(raw).split(","):
            item = item.strip()
            if item:
                targets.append(item)
    return targets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DW tables without Airflow.")
    parser.add_argument("--dw-config", type=Path, default=Path("dags/dw_config.yaml"))
    parser.add_argument("--sql-base-dir", type=Path, default=Path("dags"))
    parser.add_argument("--catalog-path", type=Path, default=Path(".duckdb/catalog.duckdb"))

    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--dt", dest="partition_date", type=str, default=None)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)

    parser.add_argument("--layer", type=str, default=None)
    parser.add_argument("--table", type=str, default=None)
    parser.add_argument("--targets", action="append", default=None, help="comma-separated targets")

    parser.add_argument("--store", choices=["s3", "local"], default="s3")
    parser.add_argument("--dry-run", action="store_true", help="print planned RunSpecs only")

    parser.add_argument("--bucket", type=str, default=os.getenv("S3_BUCKET_NAME", "stock-data"))
    parser.add_argument("--endpoint-url", type=str, default=os.getenv("S3_ENDPOINT_URL"))
    parser.add_argument("--access-key", type=str, default=os.getenv("S3_ACCESS_KEY_ID"))
    parser.add_argument("--secret-key", type=str, default=os.getenv("S3_SECRET_ACCESS_KEY"))
    parser.add_argument("--region", type=str, default=os.getenv("S3_REGION", "us-east-1"))
    parser.add_argument("--url-style", type=str, default=os.getenv("S3_URL_STYLE", "path"))
    parser.add_argument("--use-ssl", action="store_true", default=False)

    parser.add_argument("--local-root", type=Path, default=Path(".local_lake"))
    return parser


def _context_from_args(args: argparse.Namespace) -> RunContext:
    targets = _split_targets(args.targets)
    if args.layer and args.table:
        targets.append(f"{args.layer}.{args.table}")
    elif args.layer:
        targets.append(args.layer)

    extra: dict[str, Any] = {}
    if not args.partition_date and not (args.start_date and args.end_date):
        args.partition_date = get_default_partition_date_str()

    if not args.run_id:
        args.run_id = f"manual_{args.partition_date.replace('-', '')}" if args.partition_date else "manual"
    return RunContext(
        run_id=str(args.run_id),
        partition_date=args.partition_date,
        start_date=args.start_date,
        end_date=args.end_date,
        targets=targets,
        extra=extra,
    )


def _build_store(args: argparse.Namespace):
    if args.store == "local":
        root = args.local_root.resolve()
        return LocalFileStore(root_dir=root), root.as_uri()

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
    context = _context_from_args(args)
    planner = DirectoryDWPlanner(dw_config_path=args.dw_config, sql_base_dir=args.sql_base_dir)
    run_specs = planner.build(context)

    if args.dry_run:
        for spec in run_specs:
            print(spec.name)
        return 0

    for spec in run_specs:
        partition_date = spec.partition_date or ""
        run_id = context.run_id
        if partition_date:
            run_id = f"{run_id}_{partition_date.replace('-', '')}"

        paths = prepare_paths(
            base_prefix=spec.base_prefix,
            run_id=run_id,
            partition_date=spec.partition_date,
            is_partitioned=spec.is_partitioned,
            store_namespace=base_uri,
        )

        with temporary_connection() as connection:
            if args.store == "s3":
                s3_config = S3ConnectionConfig(
                    endpoint_url=str(args.endpoint_url),
                    access_key=str(args.access_key),
                    secret_key=str(args.secret_key),
                    region=str(args.region),
                    use_ssl=bool(args.use_ssl),
                    url_style=str(args.url_style),
                )
                configure_s3_access(connection, s3_config)

            attach_catalog_if_available(connection, catalog_path=args.catalog_path)

            registrars = [OdsCsvRegistrar()] if spec.layer == "ods" else []
            metrics = load(
                spec=spec,
                paths=paths,
                partition_date=spec.partition_date,
                store=store,
                connection=connection,
                base_uri=base_uri,
                registrars=registrars,
            )

        validate(store=store, paths=paths, metrics=metrics)

        partition_date_for_manifest = spec.partition_date or context.partition_date or ""
        publish_result, _manifest = commit(
            store=store,
            paths=paths,
            dest=spec.table,
            run_id=run_id,
            partition_date=partition_date_for_manifest,
            metrics=metrics,
            write_success_flag=True,
        )
        if publish_result.get("action") == "cleared":
            logger.info("No data for %s (dt=%s); cleared canonical prefix.", spec.name, spec.partition_date)

        cleanup(store=store, paths=paths)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
