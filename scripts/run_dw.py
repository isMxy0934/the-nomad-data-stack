from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from lakehouse_core import (
    Boto3S3Store,
    DirectoryDWPlanner,
    LocalFileStore,
    S3ConnectionConfig,
    attach_catalog_if_available,
    build_manifest,
    cleanup_tmp,
    configure_s3_access,
    materialize_query_to_tmp_and_measure,
    prepare_paths,
    publish_output,
    validate_output,
)
from lakehouse_core.models import RunContext
from lakehouse_core.ods_sources import register_ods_csv_source_view
from lakehouse_core.execution import temporary_connection
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
        store.delete_prefix(paths.tmp_prefix)

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

            if spec.base_prefix.startswith("lake/ods/"):
                source = (spec.inputs or {}).get("source") or {}
                source_path = str(source.get("path") or "")
                if not source_path:
                    raise ValueError(f"Missing ODS source info in RunSpec for {spec.name}")
                if not spec.partition_date:
                    raise ValueError(f"partition_date is required for ODS table {spec.name}")
                has_source = register_ods_csv_source_view(
                    connection=connection,
                    store=store,
                    base_uri=base_uri,
                    source_path=source_path,
                    partition_date=spec.partition_date,
                    view_name=f"tmp_{spec.base_prefix.split('/')[-1]}",
                )
                if not has_source:
                    metrics = {"row_count": 0, "file_count": 0, "has_data": 0}
                else:
                    metrics = materialize_query_to_tmp_and_measure(
                        connection=connection,
                        store=store,
                        query=str(spec.sql),
                        destination_prefix=paths.tmp_prefix,
                        partitioned=spec.is_partitioned,
                        tmp_partition_prefix=getattr(paths, "tmp_partition_prefix", None),
                        partition_column="dt",
                        filename_pattern="file_{uuid}",
                        use_tmp_file=True,
                    )
            else:
                metrics = materialize_query_to_tmp_and_measure(
                    connection=connection,
                    store=store,
                    query=str(spec.sql),
                    destination_prefix=paths.tmp_prefix,
                    partitioned=spec.is_partitioned,
                    tmp_partition_prefix=getattr(paths, "tmp_partition_prefix", None),
                    partition_column="dt",
                    filename_pattern="file_{uuid}",
                    use_tmp_file=True,
                )

        validate_output(store=store, paths=paths, metrics=metrics)

        if not int(metrics.get("has_data", 1)):
            logger.info("No data for %s (dt=%s); clearing canonical prefix.", spec.name, spec.partition_date)
            store.delete_prefix(paths.canonical_prefix)
            cleanup_tmp(store=store, paths=paths)
            continue

        partition_date_for_manifest = spec.partition_date or context.partition_date or ""
        manifest = build_manifest(
            dest=spec.name,
            partition_date=partition_date_for_manifest,
            run_id=run_id,
            file_count=int(metrics["file_count"]),
            row_count=int(metrics["row_count"]),
            source_prefix=getattr(paths, "tmp_partition_prefix", paths.tmp_prefix),
            target_prefix=paths.canonical_prefix,
        )
        publish_output(store=store, paths=paths, manifest=manifest, write_success_flag=True)
        cleanup_tmp(store=store, paths=paths)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
