"""Airflow DAG to maintain the DuckDB analysis catalog (metadata-only).

This DAG applies catalog migrations (versioned SQL under `catalog/migrations`),
then triggers dw_ods for the requested partition_date (or default).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.utils.catalog_migrations import apply_migrations
from dags.utils.duckdb_utils import configure_s3_access, create_temporary_connection
from dags.utils.etl_utils import build_s3_connection_config
from dags.utils.dag_run_utils import build_downstream_conf

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

        def _trigger_ods(**context) -> None:  # noqa: ANN001
            dag_run = context.get("dag_run")
            conf = build_downstream_conf(dag_run.conf if dag_run else {})
            run_id = f"dw_catalog__ods__{conf.get('partition_date') or 'unknown'}"
            trigger_dag(
                dag_id="dw_ods",
                run_id=run_id,
                conf=conf,
                replace_microseconds=False,
            )

        trigger_ods = PythonOperator(
            task_id="trigger_dw_ods",
            python_callable=_trigger_ods,
        )

        migrate >> trigger_ods

    return dag


dag = create_catalog_dag()
