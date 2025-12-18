"""Airflow DAG to maintain the DuckDB analysis catalog (metadata-only).

This DAG applies catalog migrations (versioned SQL under `catalog/migrations`).
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

from dags.utils.catalog_migrations import apply_migrations
from dags.utils.duckdb_utils import (
    configure_s3_access,
    create_temporary_connection,
)
from dags.utils.etl_utils import build_s3_connection_config

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DEFAULT_AWS_CONN_ID = "MINIO_S3"

DEFAULT_CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", "/opt/airflow/.duckdb/catalog.duckdb"))
DEFAULT_MIGRATIONS_DIR = Path(
    os.getenv("DUCKDB_CATALOG_MIGRATIONS_DIR", "/opt/airflow/catalog/migrations")
)


def migrate_catalog(*, catalog_path: str, migrations_dir: str, **_: Any) -> list[str]:
    catalog = Path(catalog_path)
    catalog.parent.mkdir(parents=True, exist_ok=True)

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    conn = create_temporary_connection(database=catalog)
    configure_s3_access(conn, s3_config)

    applied = apply_migrations(conn, migrations_dir=Path(migrations_dir))
    return applied


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
            op_kwargs={
                "catalog_path": str(DEFAULT_CATALOG_PATH),
                "migrations_dir": str(DEFAULT_MIGRATIONS_DIR),
            },
        )

        trigger_ods = TriggerDagRunOperator(
            task_id="trigger_ods_loader",
            trigger_dag_id="ods_loader_dag",
            wait_for_completion=False,  # Fire and forget
            reset_dag_run=True,  # Allow re-triggering same date
        )

        migrate >> trigger_ods

    return dag


dag = create_catalog_dag()



dag = create_catalog_dag()
