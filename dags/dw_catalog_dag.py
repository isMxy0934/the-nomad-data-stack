"""Airflow DAG to maintain the DuckDB analysis catalog (metadata-only).

This DAG applies catalog migrations (versioned SQL under `catalog/migrations`)
and refreshes layer views/macros so analysts can query `SELECT * FROM ods.xxx`.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.utils.catalog_migrations import apply_migrations
from dags.utils.catalog_utils import LayerSpec, build_layer_dt_macro_sql, build_layer_view_sql
from dags.utils.duckdb_utils import (
    S3ConnectionConfig,
    configure_s3_access,
    create_temporary_connection,
)

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")

DEFAULT_CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", "/opt/airflow/.duckdb/catalog.duckdb"))
DEFAULT_MIGRATIONS_DIR = Path(
    os.getenv("DUCKDB_CATALOG_MIGRATIONS_DIR", "/opt/airflow/catalog/migrations")
)

DEFAULT_LAYER_BASE_PREFIX = os.getenv("DUCKDB_CATALOG_ODS_PREFIX", "lake/ods")
DEFAULT_LAYER_SCHEMA = os.getenv("DUCKDB_CATALOG_ODS_SCHEMA", "ods")

CATALOG_POOL = os.getenv("DUCKDB_CATALOG_POOL", "duckdb_catalog_pool")


def _build_s3_connection_config(s3_hook: S3Hook) -> S3ConnectionConfig:
    connection = s3_hook.get_connection(s3_hook.aws_conn_id)
    extras = connection.extra_dejson or {}

    endpoint = extras.get("endpoint_url") or extras.get("host")
    if not endpoint:
        raise ValueError("S3 connection must define endpoint_url")

    url_style = extras.get("s3_url_style", "path")
    region = extras.get("region_name", "us-east-1")
    use_ssl = bool(extras.get("use_ssl", endpoint.startswith("https")))

    return S3ConnectionConfig(
        endpoint_url=endpoint,
        access_key=connection.login or "",
        secret_key=connection.password or "",
        region=region,
        use_ssl=use_ssl,
        url_style=url_style,
        session_token=extras.get("session_token"),
    )


def _discover_tables(*, s3_hook: S3Hook, bucket: str, base_prefix: str) -> list[str]:
    client = s3_hook.get_conn()
    prefix = base_prefix.strip("/") + "/"
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


def migrate_catalog(*, catalog_path: str, migrations_dir: str, **_: Any) -> list[str]:
    catalog = Path(catalog_path)
    catalog.parent.mkdir(parents=True, exist_ok=True)

    conn = create_temporary_connection(database=catalog)
    applied = apply_migrations(conn, migrations_dir=Path(migrations_dir))
    return applied


def refresh_ods_catalog(
    *,
    catalog_path: str,
    bucket_name: str,
    base_prefix: str,
    schema: str,
    **_: Any,
) -> dict[str, int]:
    catalog = Path(catalog_path)
    catalog.parent.mkdir(parents=True, exist_ok=True)

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = _build_s3_connection_config(s3_hook)

    conn = create_temporary_connection(database=catalog)
    configure_s3_access(conn, s3_config)

    tables = _discover_tables(s3_hook=s3_hook, bucket=bucket_name, base_prefix=base_prefix)
    layer = LayerSpec(schema=schema, base_prefix=base_prefix, partitioned_by_dt=True)

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    for table in tables:
        conn.execute(build_layer_view_sql(bucket=bucket_name, layer=layer, table=table))
        conn.execute(build_layer_dt_macro_sql(bucket=bucket_name, layer=layer, table=table))

    return {"table_count": len(tables)}


def create_catalog_dag() -> DAG:
    with DAG(
        dag_id="duckdb_catalog_dag",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["catalog", "duckdb"],
    ) as dag:
        migrate = PythonOperator(
            task_id="migrate_catalog",
            python_callable=migrate_catalog,
            op_kwargs={
                "catalog_path": str(DEFAULT_CATALOG_PATH),
                "migrations_dir": str(DEFAULT_MIGRATIONS_DIR),
            },
            pool=CATALOG_POOL,
        )

        refresh_ods = PythonOperator(
            task_id="refresh_ods_catalog",
            python_callable=refresh_ods_catalog,
            op_kwargs={
                "catalog_path": str(DEFAULT_CATALOG_PATH),
                "bucket_name": DEFAULT_BUCKET_NAME,
                "base_prefix": DEFAULT_LAYER_BASE_PREFIX,
                "schema": DEFAULT_LAYER_SCHEMA,
            },
            pool=CATALOG_POOL,
        )

        migrate >> refresh_ods

    return dag


dag = create_catalog_dag()
