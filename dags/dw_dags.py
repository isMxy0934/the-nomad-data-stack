"""Dynamically generate DW DAGs based on dw_config.yaml and SQL directories."""

from __future__ import annotations

import logging
import os
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
    execute_sql,
    temporary_connection,
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
CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", ".duckdb/catalog.duckdb"))

logger = logging.getLogger(__name__)


def _attach_catalog_if_available(connection) -> None:
    if not CATALOG_PATH.exists():
        logger.info("Catalog file %s not found; skipping attach.", CATALOG_PATH)
        return
    connection.execute(f"ATTACH '{CATALOG_PATH}' AS catalog (READ_ONLY);")
    connection.execute("USE catalog;")


def _register_ods_raw_view(
    *,
    connection,
    s3_hook: S3Hook,
    config: DWConfig,
    table_name: str,
    bucket_name: str,
    partition_date: str,
) -> bool:
    source = config.sources.get(table_name)
    if not source:
        raise ValueError(
            f"Missing sources entry for ODS table '{table_name}'. "
            f"Add sources.{table_name} to {CONFIG_PATH}."
        )

    if source.format.lower() != "csv":
        raise ValueError(
            f"Unsupported ODS source format for '{table_name}': '{source.format}' (only csv supported)"
        )

    source_path = source.path.strip("/")
    source_prefix = f"{source_path}/dt={partition_date}/"
    source_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=source_prefix) or []
    csv_keys = [key for key in source_keys if key.endswith(".csv")]
    if not csv_keys:
        logger.info(
            "No source CSV found under s3://%s/%s (partition_date=%s, table=%s); no-op.",
            bucket_name,
            source_prefix,
            partition_date,
            table_name,
        )
        return False

    source_uri = f"s3://{bucket_name}/{source_path}/dt={partition_date}/*.csv"
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW tmp_{table_name} AS
        SELECT * FROM read_csv_auto('{source_uri}', hive_partitioning=false);
        """
    )
    return True


def load_table(
    table_spec: TableSpec,
    bucket_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, int]:
    def empty_metrics(*, partitioned: bool) -> dict[str, int]:
        metrics = {
            "row_count": 0,
            "file_count": 0,
            "has_data": 0,
            "partitioned": int(partitioned),
        }
        ti.xcom_push(key="load_metrics", value=metrics)
        return metrics

    def ensure_table_registered(connection) -> None:
        check_sql = f"""
            SELECT COUNT(*)
            FROM information_schema.views
            WHERE table_schema = '{table_spec.layer}'
            AND table_name = '{table_spec.name}'
        """
        exists = connection.execute(check_sql).fetchone()[0]
        if not exists:
            raise ValueError(
                f"Table '{table_spec.layer}.{table_spec.name}' is not registered in the catalog. "
                f"Please add a migration script in 'catalog/migrations/' to define this table."
            )

    def compute_row_count(connection, rendered_sql: str) -> int:
        relation = execute_sql(
            connection,
            f"SELECT COUNT(*) AS row_count FROM ({rendered_sql}) AS src",
        )
        return int(relation.fetchone()[0])

    def write_outputs(connection, *, rendered_sql: str, destination_prefix: str) -> int:
        if partitioned:
            copy_partitioned_parquet(
                connection,
                query=rendered_sql,
                destination=destination_prefix,
                partition_column="dt",
                filename_pattern="file_{uuid}",
                use_tmp_file=True,
            )
            return len(list_parquet_keys(s3_hook, paths_dict["tmp_partition_prefix"]))

        copy_parquet(
            connection,
            query=rendered_sql,
            destination=destination_prefix,
            filename_pattern="file_{uuid}",
            use_tmp_file=True,
        )
        return len(list_parquet_keys(s3_hook, destination_prefix))

    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = paths_dict.get("partition_date")

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    with temporary_connection() as connection:
        configure_s3_access(connection, s3_config)
        _attach_catalog_if_available(connection)
        ensure_table_registered(connection)

        if table_spec.layer == "ods":
            config = load_dw_config(CONFIG_PATH)
            has_source_data = _register_ods_raw_view(
                connection=connection,
                s3_hook=s3_hook,
                config=config,
                table_name=table_spec.name,
                bucket_name=bucket_name,
                partition_date=str(partition_date),
            )
            if not has_source_data:
                return empty_metrics(partitioned=partitioned)

        rendered_sql = load_and_render_sql(
            table_spec.sql_path,
            {"PARTITION_DATE": partition_date},
        )
        row_count = compute_row_count(connection, rendered_sql)
        if row_count == 0:
            return empty_metrics(partitioned=partitioned)

        destination_prefix = paths_dict["tmp_prefix"]
        file_count = write_outputs(
            connection,
            rendered_sql=rendered_sql,
            destination_prefix=destination_prefix,
        )

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
        tags=["dw"],
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

        potential_downstream = [
            ds_layer for ds_layer, deps in config.layer_dependencies.items() if layer in deps
        ]

        valid_downstream = []
        for ds_layer in potential_downstream:
            if discover_tables_for_layer(ds_layer, SQL_BASE_DIR):
                valid_downstream.append(ds_layer)
            else:
                logger.warning(
                    "Layer '%s' depends on %s but has no tables; skipping trigger.", ds_layer, layer
                )

        if valid_downstream:
            for ds_layer in valid_downstream:
                trigger = TriggerDagRunOperator(
                    task_id=f"trigger_dw_{ds_layer}",
                    trigger_dag_id=f"dw_{ds_layer}",
                    wait_for_completion=False,
                    reset_dag_run=True,
                )
                for tg in task_groups.values():
                    tg >> trigger
        else:
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
