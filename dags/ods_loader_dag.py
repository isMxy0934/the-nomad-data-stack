"""Airflow DAG to load ODS partitions using DuckDB and S3-compatible storage."""

from __future__ import annotations

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.external_task import ExternalTaskSensor

from dags.utils.duckdb_utils import (
    configure_s3_access,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
)
from dags.utils.dw_config_utils import load_dw_config
from dags.utils.etl_utils import (
    DEFAULT_AWS_CONN_ID,
    build_etl_task_group,
    build_s3_connection_config,
    list_parquet_keys,
)
from dags.utils.partition_utils import PartitionPaths
from dags.utils.sql_utils import load_and_render_sql

CONFIG_PATH = Path(__file__).parent / "ods" / "config.yaml"
DW_CONFIG_PATH = Path(__file__).parent / "dw_config.yaml"
SQL_DIR = Path(__file__).parent / "ods"

logger = logging.getLogger(__name__)

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

def load_ods_config(path: Path) -> list[dict[str, Any]]:
    """Load and validate the ODS configuration file."""

    if not path.exists():
        raise FileNotFoundError(f"ODS config not found: {path}")

    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(config, list):
        raise ValueError("ODS config must be a list of table definitions")

    validated: list[dict[str, Any]] = []
    for entry in config:
        if not isinstance(entry, dict):
            raise ValueError("Each ODS entry must be a mapping")
        dest = entry.get("dest")
        src = entry.get("src")
        if not dest or not src:
            raise ValueError("ODS entry requires both 'dest' and 'src'")
        validated.append(entry)
    return validated


def load_partition(
    table_config: dict[str, Any],
    bucket_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, int]:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    paths = PartitionPaths(
        partition_date=paths_dict["partition_date"],
        canonical_prefix=paths_dict["canonical_prefix"],
        tmp_prefix=paths_dict["tmp_prefix"],
        tmp_partition_prefix=paths_dict["tmp_partition_prefix"],
        manifest_path=paths_dict["manifest_path"],
        success_flag_path=paths_dict["success_flag_path"],
    )
    partition_date = paths.partition_date

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    source_path = str(table_config["src"]["properties"]["path"]).strip("/")
    source_prefix = f"{source_path}/dt={partition_date}/"
    source_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=source_prefix) or []
    csv_keys = [key for key in source_keys if key.endswith(".csv")]
    if not csv_keys:
        logger.info(
            "No source CSV found under s3://%s/%s (partition_date=%s, table=%s); no-op.",
            bucket_name,
            source_prefix,
            partition_date,
            table_config.get("dest"),
        )
        metrics = {"row_count": 0, "file_count": 0, "has_data": 0}
        ti.xcom_push(key="load_metrics", value=metrics)
        return metrics

    s3_config = build_s3_connection_config(s3_hook)

    connection = create_temporary_connection()
    configure_s3_access(connection, s3_config)

    source_uri = f"s3://{bucket_name}/{source_path}/dt={partition_date}/*.csv"
    staging_view = f"tmp_{table_config['dest']}"

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW {staging_view} AS
        SELECT * FROM read_csv_auto('{source_uri}', hive_partitioning=false);
        """
    )

    rendered_sql = load_and_render_sql(
        SQL_DIR / f"{table_config['dest']}.sql",
        {"PARTITION_DATE": partition_date},
    )

    copy_partitioned_parquet(
        connection,
        query=rendered_sql,
        destination=paths.tmp_prefix,
        partition_column="dt",
        filename_pattern="file_{uuid}",
        use_tmp_file=True,
    )

    row_count_relation = execute_sql(
        connection, f"SELECT COUNT(*) AS row_count FROM ({rendered_sql})"
    )
    row_count = int(row_count_relation.fetchone()[0])

    file_count = len(list_parquet_keys(s3_hook, paths.tmp_prefix))

    metrics = {"row_count": row_count, "file_count": file_count, "has_data": 1}
    ti.xcom_push(key="load_metrics", value=metrics)
    return metrics


def create_ods_loader_dag() -> DAG:
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    }

    dag = DAG(
        dag_id=DAG_ID,
        description="Load ODS partitions from raw data into partitioned parquet",
        default_args=default_args,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
    )

    with dag:
        configs = load_ods_config(CONFIG_PATH)
        etl_tasks = []
        for table_config in configs:
            dest = table_config["dest"]
            tg = build_etl_task_group(
                dag=dag,
                task_group_id=dest,
                dest_name=dest,
                base_prefix=f"lake/ods/{dest}",
                load_callable=load_partition,
                load_op_kwargs={"table_config": table_config},
                is_partitioned=True,
            )
            etl_tasks.append(tg)
        
        # Trigger Downstream DW DAGs
        try:
            dw_config = load_dw_config(DW_CONFIG_PATH)
            # Find layers that depend on 'ods'
            downstream_layers = [
                layer
                for layer, deps in dw_config.layer_dependencies.items()
                if "ods" in deps
            ]

            for layer in downstream_layers:
                trigger = TriggerDagRunOperator(
                    task_id=f"trigger_dw_{layer}",
                    trigger_dag_id=f"dw_{layer}",
                    wait_for_completion=False,
                    reset_dag_run=True,
                )
                # Trigger runs after ALL ODS tables are loaded
                for tg in etl_tasks:
                    tg >> trigger
        except Exception:
            logger.exception("Failed to load DW config for downstream triggers")

    return dag


dag = create_ods_loader_dag()
