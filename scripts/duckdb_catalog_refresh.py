#!/usr/bin/env python3
"""Refresh a persistent DuckDB catalog (metadata-only) for interactive analysis.

The catalog database contains only SCHEMA + VIEW + MACRO definitions.
All data continues to live in MinIO (S3) as Parquet/CSV under `lake/...`.
"""

from __future__ import annotations

import argparse
import logging
import os
from collections.abc import Iterable
from pathlib import Path

import boto3
from botocore.config import Config

from dags.utils.catalog_utils import (
    LayerSpec,
    build_layer_dt_macro_sql,
    build_layer_view_sql,
    build_schema_sql,
)
from dags.utils.duckdb_utils import (
    S3ConnectionConfig,
    configure_s3_access,
    create_temporary_connection,
)

logger = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value or default


def discover_tables(*, s3_client, bucket: str, base_prefix: str) -> list[str]:
    """Discover table names under a layer prefix using S3 delimiter listing."""

    prefix = base_prefix.strip("/") + "/"
    paginator = s3_client.get_paginator("list_objects_v2")
    tables: set[str] = set()

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for entry in page.get("CommonPrefixes", []) or []:
            raw = (entry.get("Prefix") or "").rstrip("/")
            if not raw:
                continue
            table = raw.split("/")[-1]
            if table:
                tables.add(table)

    return sorted(tables)


def apply_catalog(
    *,
    catalog_path: Path,
    bucket: str,
    s3_config: S3ConnectionConfig,
    layer: LayerSpec,
    tables: Iterable[str],
    dry_run: bool,
) -> None:
    catalog_path.parent.mkdir(parents=True, exist_ok=True)

    conn = create_temporary_connection(database=catalog_path)
    configure_s3_access(conn, s3_config)

    ddl: list[str] = [build_schema_sql(layer.schema)]
    for table in tables:
        ddl.append(build_layer_view_sql(bucket=bucket, layer=layer, table=table))
        ddl.append(build_layer_dt_macro_sql(bucket=bucket, layer=layer, table=table))

    if dry_run:
        print("\n\n".join(ddl))
        return

    for statement in ddl:
        conn.execute(statement)


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh DuckDB analysis catalog (metadata-only).")
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path(".duckdb/catalog.duckdb"),
        help="Path to the persistent DuckDB catalog database file.",
    )
    parser.add_argument(
        "--bucket",
        default=_env("S3_BUCKET_NAME", "stock-data"),
        help="S3 bucket name (MinIO).",
    )
    parser.add_argument(
        "--endpoint",
        default=_env("S3_ENDPOINT", "http://localhost:9000"),
        help="S3 endpoint URL, e.g. http://localhost:9000",
    )
    parser.add_argument(
        "--access-key",
        default=_env("S3_ACCESS_KEY", "minioadmin"),
        help="S3 access key.",
    )
    parser.add_argument(
        "--secret-key",
        default=_env("S3_SECRET_KEY", "minioadmin"),
        help="S3 secret key.",
    )
    parser.add_argument(
        "--base-prefix",
        default="lake/ods",
        help="Key prefix for the layer, e.g. lake/ods or lake/_integration/ods",
    )
    parser.add_argument(
        "--schema",
        default="ods",
        help="DuckDB schema name for views/macros, e.g. ods",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print generated DDL without executing it.",
    )
    parser.add_argument(
        "--log-level",
        default=_env("LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG/INFO/WARNING/ERROR).",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s"
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=_env("S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )

    tables = discover_tables(s3_client=s3_client, bucket=args.bucket, base_prefix=args.base_prefix)
    if not tables:
        logger.warning("No tables discovered under s3://%s/%s", args.bucket, args.base_prefix)

    layer = LayerSpec(schema=args.schema, base_prefix=args.base_prefix, partitioned_by_dt=True)

    s3_config = S3ConnectionConfig(
        endpoint_url=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=_env("S3_REGION", "us-east-1"),
        use_ssl=args.endpoint.startswith("https"),
        url_style=_env("S3_URL_STYLE", "path"),
    )
    logger.info(
        "Refreshing catalog %s with %d tables from %s",
        args.catalog_path,
        len(tables),
        args.base_prefix,
    )
    apply_catalog(
        catalog_path=args.catalog_path,
        bucket=args.bucket,
        s3_config=s3_config,
        layer=layer,
        tables=tables,
        dry_run=bool(args.dry_run),
    )
    logger.info("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
