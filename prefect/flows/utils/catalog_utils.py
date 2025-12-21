"""Catalog refresh helpers for Prefect flows (metadata-only)."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

from flows.utils.config import build_s3_connection_config_from_env, get_s3_bucket_name
from flows.utils.s3_utils import get_boto3_client
from lakehouse_core.catalog import (
    LayerSpec,
    build_layer_dt_macro_sql,
    build_layer_view_sql,
    build_schema_sql,
)
from lakehouse_core.compute import configure_s3_access, temporary_connection

logger = logging.getLogger(__name__)


def discover_tables(*, bucket: str, base_prefix: str) -> list[str]:
    prefix = base_prefix.strip("/") + "/"
    client = get_boto3_client()
    paginator = client.get_paginator("list_objects_v2")
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


def refresh_catalog(
    *,
    catalog_path: Path,
    base_prefix: str,
    schema: str,
    tables: Iterable[str],
) -> None:
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    s3_config = build_s3_connection_config_from_env()

    layer = LayerSpec(schema=schema, base_prefix=base_prefix, partitioned_by_dt=True)

    ddl: list[str] = [build_schema_sql(layer.schema)]
    for table in tables:
        ddl.append(build_layer_view_sql(bucket=get_s3_bucket_name(), layer=layer, table=table))
        ddl.append(build_layer_dt_macro_sql(bucket=get_s3_bucket_name(), layer=layer, table=table))

    if not ddl:
        logger.info("Catalog refresh skipped: no DDL generated.")
        return

    with temporary_connection(database=catalog_path) as conn:
        configure_s3_access(conn, s3_config)
        for statement in ddl:
            conn.execute(statement)
