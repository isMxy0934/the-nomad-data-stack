"""Airflow DAG to maintain the DuckDB analysis catalog (metadata-only).

This DAG applies catalog migrations (versioned SQL under `catalog/migrations`),
then triggers dw_ods for the requested partition_date (or range).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters import build_s3_connection_config
from lakehouse_core.catalog import apply_migrations
from lakehouse_core.compute import configure_s3_access, create_temporary_connection

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DEFAULT_AWS_CONN_ID = "MINIO_S3"

DEFAULT_CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", ".duckdb/catalog.duckdb"))
DEFAULT_MIGRATIONS_DIR = Path(
    os.getenv("DUCKDB_CATALOG_MIGRATIONS_DIR", "/opt/airflow/catalog/migrations")
)


def migrate_catalog(*, catalog_path: str, migrations_dir: str, **_: Any) -> list[str]:
    catalog = Path(catalog_path)
    catalog.parent.mkdir(parents=True, exist_ok=True)

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    conn = create_temporary_connection(database=catalog)
    try:
        configure_s3_access(conn, s3_config)
        return apply_migrations(conn, migrations_dir=Path(migrations_dir))
    finally:
        conn.close()


def create_catalog_dag() -> DAG:
    with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["catalog", "duckdb"],
    ) as dag:
        migrate = PythonOperator(
            task_id="migrate_catalog",
            python_callable=migrate_catalog,
            pool="duckdb_catalog_pool",
            op_kwargs={
                "catalog_path": str(DEFAULT_CATALOG_PATH),
                "migrations_dir": str(DEFAULT_MIGRATIONS_DIR),
            },
        )

        trigger_ods = TriggerDagRunOperator(
            task_id="trigger_dw_ods",
            trigger_dag_id="dw_ods",
            wait_for_completion=False,
            reset_dag_run=True,
            conf={
                "partition_date": "{{ dag_run.conf.get('partition_date') }}",
                "start_date": "{{ dag_run.conf.get('start_date') }}",
                "end_date": "{{ dag_run.conf.get('end_date') }}",
                "targets": "{{ dag_run.conf.get('targets') }}",
                "init": "{{ dag_run.conf.get('init') }}",
            },
        )

        migrate >> trigger_ods

    return dag


dag = create_catalog_dag()
