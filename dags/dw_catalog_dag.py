"""Airflow DAG to maintain the DuckDB analysis catalog (metadata-only).

This DAG applies catalog migrations (versioned SQL under `catalog/migrations`).
"""

from __future__ import annotations

import os
import re
import logging
from pathlib import Path
from typing import Any
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.utils.catalog_migrations import (
    apply_migrations,
    load_applied_migrations,
    list_migration_files,
)
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

logger = logging.getLogger(__name__)


def _seed_s3_paths_from_sqls(connection, sql_files: list[Path]) -> None:
    """Extract S3 paths and column schemas from SQL files and ensure rich seed files exist."""
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    
    # Pattern to find S3 paths in read_parquet calls
    s3_pattern = re.compile(r"s3://([a-zA-Z0-9.\-_]+)/([^'\s*?]+)")
    # Pattern to find schema macros: name: 'trade_date', type: 'DATE'
    col_pattern = re.compile(r"name:\s*'([^']+)',\s*type:\s*'([^']+)'")
    
    for sql_file in sql_files:
        content = sql_file.read_text(encoding="utf-8")
        
        # Split file into sections by table (delimited by long comment lines)
        # This prevents schema leakage between multiple tables in the same file
        sections = re.split(r"-{20,}", content)
        
        for section in sections:
            # Find columns for THIS section only
            cols = col_pattern.findall(section)
            if not cols:
                continue
                
            # Build a "Rich Seed" SQL part: CAST(NULL AS TYPE) AS name
            # Exclude 'dt' as it's a Hive partition column managed by DuckDB
            seed_select_parts = [
                f"CAST(NULL AS {ctype}) AS {name}" 
                for name, ctype in cols if name.lower() != 'dt'
            ]
            seed_sql = f"SELECT {', '.join(seed_select_parts)} WHERE 1=0"
            
            # Find S3 paths in THIS section only
            matches = s3_pattern.findall(section)
            unique_prefixes = set(matches)
            
            for bucket, prefix in unique_prefixes:
                is_partitioned = prefix.endswith("dt=")
                search_prefix = prefix.strip("/") + "/"
                
                # Check for real data (parquet files that are NOT our seeds)
                all_keys = s3_hook.list_keys(bucket_name=bucket, prefix=search_prefix) or []
                has_real_data = any(
                    k.endswith(".parquet") and "seed.parquet" not in k 
                    for k in all_keys
                )
                
                if not has_real_data:
                    # Target path for the seed
                    if is_partitioned:
                        seed_path = f"s3://{bucket}/{prefix}__seed__/seed.parquet"
                    else:
                        seed_path = f"s3://{bucket}/{prefix}/seed.parquet"
                        
                    logger.info("Precision Seeding (Scoped): Creating dummy for %s", seed_path)
                    connection.execute(f"COPY ({seed_sql}) TO '{seed_path}' (FORMAT PARQUET);")


def migrate_catalog(*, catalog_path: str, migrations_dir: str, **_: Any) -> list[str]:
    catalog = Path(catalog_path)
    catalog.parent.mkdir(parents=True, exist_ok=True)

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    # Use a temporary connection to identify pending migrations and seed them
    conn = create_temporary_connection(database=catalog)
    configure_s3_access(conn, s3_config)

    # 1. Identify pending migration files
    applied = load_applied_migrations(conn)
    all_files = list_migration_files(Path(migrations_dir))
    pending_files = [f for f in all_files if f.name not in applied]

    if not pending_files:
        logger.info("No pending migrations to apply.")
        return []

    # 2. Rich Seed S3 paths found in the pending SQLs
    _seed_s3_paths_from_sqls(conn, pending_files)

    # 3. Finally apply migrations
    applied_now = apply_migrations(conn, migrations_dir=Path(migrations_dir))
    return applied_now


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
            wait_for_completion=False,
            reset_dag_run=True,
        )

        migrate >> trigger_ods

    return dag


dag = create_catalog_dag()
