"""Dynamically generate DW DAGs based on dw_config.yaml and SQL directories."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup

from dags.utils.duckdb_utils import (
    configure_s3_access,
    copy_parquet,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
)
from dags.utils.dw_config_utils import (
    DWConfig,
    DWConfigError,
    TableSpec,
    discover_tables_for_layer,
    load_dw_config,
    order_layers,
    order_tables_within_layer,
)
from dags.utils.etl_utils import (
    DEFAULT_AWS_CONN_ID,
    build_etl_task_group,
    build_s3_connection_config,
    list_parquet_keys,
)
from dags.utils.sql_utils import load_and_render_sql

CONFIG_PATH = Path(__file__).parent / "dw_config.yaml"
SQL_BASE_DIR = Path(__file__).parent
CATALOG_PATH = Path(".duckdb") / "catalog.duckdb"

logger = logging.getLogger(__name__)


def _attach_catalog_if_available(connection) -> None:
    if not CATALOG_PATH.exists():
        logger.info("Catalog file %s not found; skipping attach.", CATALOG_PATH)
        return
    connection.execute(f"ATTACH '{CATALOG_PATH}' AS catalog (READ_ONLY);")
    connection.execute("USE catalog;")


def load_table(
    table_spec: TableSpec,
    bucket_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, int]:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = paths_dict.get("partition_date")

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    connection = create_temporary_connection()
    configure_s3_access(connection, s3_config)
    _attach_catalog_if_available(connection)

    rendered_sql = load_and_render_sql(
        table_spec.sql_path,
        {"PARTITION_DATE": partition_date},
    )

    row_count_relation = execute_sql(
        connection, f"SELECT COUNT(*) AS row_count FROM ({rendered_sql})"
    )
    row_count = int(row_count_relation.fetchone()[0])

    if row_count == 0:
        metrics = {"row_count": 0, "file_count": 0, "has_data": 0, "partitioned": int(partitioned)}
        ti.xcom_push(key="load_metrics", value=metrics)
        return metrics

    destination_prefix = paths_dict["tmp_prefix"]
    if partitioned:
        copy_partitioned_parquet(
            connection,
            query=rendered_sql,
            destination=destination_prefix,
            partition_column="dt",
            filename_pattern="file_{uuid}",
            use_tmp_file=True,
        )
        file_count = len(list_parquet_keys(s3_hook, paths_dict["tmp_partition_prefix"]))
    else:
        copy_parquet(
            connection,
            query=rendered_sql,
            destination=destination_prefix,
            filename_pattern="file_{uuid}",
            use_tmp_file=True,
        )
        file_count = len(list_parquet_keys(s3_hook, destination_prefix))

    metrics = {
        "row_count": row_count,
        "file_count": file_count,
        "has_data": 1,
        "partitioned": int(partitioned),
    }
    ti.xcom_push(key="load_metrics", value=metrics)
    return metrics


def create_layer_dag(layer: str, config: DWConfig) -> DAG | None:
    tables = discover_tables_for_layer(layer, SQL_BASE_DIR)
    if not tables:
        return None

    dependencies = config.table_dependencies.get(layer, {})
    ordered_tables = order_tables_within_layer(tables, dependencies)

    default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}
    dag = DAG(
        dag_id=f"dw_{layer}",
        description=f"DW layer DAG for {layer}",
        default_args=default_args,
        schedule=None,  # Triggered by upstream layers
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
    )

    with dag:
        task_groups: dict[str, TaskGroup] = {}
        for table in ordered_tables:
            tg = build_etl_task_group(
                dag=dag,
                task_group_id=table.name,
                dest_name=table.name,
                base_prefix=f"lake/{table.layer}/{table.name}",
                load_callable=load_table,
                load_op_kwargs={"table_spec": table},
                is_partitioned=table.is_partitioned,
            )
            task_groups[table.name] = tg

        for table, deps in dependencies.items():
            if table not in task_groups:
                continue
            for dep in deps:
                if dep in task_groups:
                    task_groups[dep] >> task_groups[table]

        # Trigger Downstream Layers
        potential_downstream = [
            ds_layer for ds_layer, deps in config.layer_dependencies.items()
            if layer in deps
        ]

        # Filter out downstream layers that have no SQL tables
        valid_downstream = []
        for ds_layer in potential_downstream:
            if discover_tables_for_layer(ds_layer, SQL_BASE_DIR):
                valid_downstream.append(ds_layer)
            else:
                logger.warning(
                    "Layer '%s' depends on %s but has no tables; skipping trigger.",
                    ds_layer, layer
                )

        if valid_downstream:
            for ds_layer in valid_downstream:
                trigger = TriggerDagRunOperator(
                    task_id=f"trigger_dw_{ds_layer}",
                    trigger_dag_id=f"dw_{ds_layer}",
                    wait_for_completion=False,
                    reset_dag_run=True,
                )
                # Trigger only after ALL tables in current layer are done
                for tg in task_groups.values():
                    tg >> trigger
        else:
            # No valid downstream layers, trigger finish
            trigger_finish = TriggerDagRunOperator(
                task_id="trigger_dw_finish",
                trigger_dag_id="dw_finish_dag",
                wait_for_completion=False,
                reset_dag_run=True,
            )
            for tg in task_groups.values():
                tg >> trigger_finish

    return dag


def build_dw_dags() -> dict[str, DAG]:
    config = load_dw_config(CONFIG_PATH)
    dag_map: dict[str, DAG] = {}
    for layer in order_layers(config):
        # ODS layer is handled by ods_loader_dag.py with special CSV loading logic
        if layer == "ods":
            continue

        try:
            dag = create_layer_dag(layer, config)
        except DWConfigError:
            logger.exception("Failed to build DAG for layer %s", layer)
            continue
        if dag:
            dag_map[dag.dag_id] = dag
    return dag_map


dw_dags = build_dw_dags()
globals().update(dw_dags)
