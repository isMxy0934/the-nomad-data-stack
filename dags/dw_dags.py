"""Dynamically generate DW DAGs based on dw_config.yaml and SQL directories."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

from dags.utils.dag_run_utils import parse_targets
from dags.utils.duckdb_utils import (
    configure_s3_access,
    execute_sql,
    temporary_connection,
    copy_parquet,
    copy_partitioned_parquet,
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
    DEFAULT_BUCKET_NAME,
    build_s3_connection_config,
    list_parquet_keys,
    prepare_dataset,
    validate_dataset,
    commit_dataset,
    cleanup_dataset,
)
from dags.utils.sql_utils import load_and_render_sql

CONFIG_PATH = Path(__file__).parent / "dw_config.yaml"
SQL_BASE_DIR = Path(__file__).parent
CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", ".duckdb/catalog.duckdb"))

logger = logging.getLogger(__name__)


def _conf_targets(context: Mapping[str, Any]) -> list[str] | None:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    return parse_targets(conf)


def _target_matches(table_spec: TableSpec, target: str) -> bool:
    target = target.strip()
    if not target or "." not in target:
        return False
    layer, name = target.split(".", 1)
    return layer == table_spec.layer and name == table_spec.name


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
    paths_dict: dict[str, Any],
    table_spec: TableSpec,
    bucket_name: str = DEFAULT_BUCKET_NAME,
    **context: Any,
) -> dict[str, Any]:
    ti = context["ti"]
    targets = _conf_targets(context)
    if targets is not None and not any(_target_matches(table_spec, t) for t in targets):
        logger.info("Skipping table %s (not in targets).", table_spec.name)
        return {"row_count": 0, "file_count": 0, "has_data": 0, "skipped": 1}

    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = str(paths_dict.get("partition_date"))

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    with temporary_connection() as connection:
        configure_s3_access(connection, s3_config)
        _attach_catalog_if_available(connection)

        if table_spec.layer == "ods":
            config = load_dw_config(CONFIG_PATH)
            has_source_data = _register_ods_raw_view(
                connection=connection,
                s3_hook=s3_hook,
                config=config,
                table_name=table_spec.name,
                bucket_name=bucket_name,
                partition_date=partition_date,
            )
            if not has_source_data:
                return {"row_count": 0, "file_count": 0, "has_data": 0}

        rendered_sql = load_and_render_sql(
            table_spec.sql_path,
            {"PARTITION_DATE": partition_date},
        )
        relation = execute_sql(connection, f"SELECT COUNT(*) AS row_count FROM ({rendered_sql}) AS src")
        row_count = int(relation.fetchone()[0])
        if row_count == 0:
            return {"row_count": 0, "file_count": 0, "has_data": 0}

        destination_prefix = paths_dict["tmp_prefix"]
        if partitioned:
            copy_partitioned_parquet(
                connection, 
                query=rendered_sql, 
                destination=destination_prefix, 
                partition_column="dt",
                filename_pattern="file_{uuid}",
                use_tmp_file=True
            )
            file_count = len(list_parquet_keys(s3_hook, paths_dict["tmp_partition_prefix"]))
        else:
            copy_parquet(
                connection, 
                query=rendered_sql, 
                destination=destination_prefix,
                filename_pattern="file_{uuid}",
                use_tmp_file=True
            )
            file_count = len(list_parquet_keys(s3_hook, destination_prefix))

        return {"row_count": row_count, "file_count": file_count, "has_data": 1, "partitioned": int(partitioned)}


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
        max_active_runs=10,
        tags=["dw"],
    )

    with dag:
        @task
        def get_date_list(**context: Any) -> list[str]:
            conf = context.get("dag_run").conf or {}
            start_date = conf.get("start_date")
            end_date = conf.get("end_date")
            partition_date = conf.get("partition_date")

            dates = []
            if start_date and end_date:
                s_dt = date.fromisoformat(start_date)
                e_dt = date.fromisoformat(end_date)
                curr = s_dt
                while curr <= e_dt:
                    dates.append(curr.isoformat())
                    curr += timedelta(days=1)
            else:
                dates = [partition_date] if partition_date else [date.today().isoformat()]
            
            return sorted(list(set(dates)))

        date_list = get_date_list()
        table_last_tasks = {}

        for table in ordered_tables:
            from airflow.utils.task_group import TaskGroup
            with TaskGroup(group_id=table.name) as tg:
                # 1. Prepare
                prepare = PythonOperator.partial(
                    task_id="prepare",
                    python_callable=prepare_dataset,
                ).expand(
                    op_kwargs=date_list.map(lambda d: {
                        "base_prefix": f"lake/{table.layer}/{table.name}",
                        "run_id": "{{ run_id }}",
                        "is_partitioned": table.is_partitioned,
                        "partition_date": d
                    })
                )

                # 2. Load
                load = PythonOperator.partial(
                    task_id="load",
                    python_callable=load_table,
                ).expand(
                    op_kwargs=prepare.output.map(lambda p: {
                        "table_spec": table, 
                        "bucket_name": DEFAULT_BUCKET_NAME, 
                        "run_id": "{{ run_id }}", 
                        "task_group_id": table.name,
                        "paths_dict": p
                    })
                )

                # 3. Validate
                def _validate_adapter(paths_dict, metrics, **_):
                    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                    return validate_dataset(paths_dict, metrics, s3_hook)

                validate = PythonOperator.partial(
                    task_id="validate",
                    python_callable=_validate_adapter,
                ).expand(
                    op_kwargs=prepare.output.zip(load.output).map(lambda x: {
                        "paths_dict": x[0], 
                        "metrics": x[1]
                    })
                )

                # 4. Commit
                def _commit_adapter(paths_dict, metrics, dest_name, run_id, **_):
                    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                    res, _ = commit_dataset(dest_name, run_id, paths_dict, metrics, s3_hook)
                    return res

                commit = PythonOperator.partial(
                    task_id="commit",
                    python_callable=_commit_adapter,
                ).expand(
                    op_kwargs=prepare.output.zip(validate.output).map(lambda x: {
                        "paths_dict": x[0], 
                        "metrics": x[1],
                        "dest_name": table.name,
                        "run_id": "{{ run_id }}"
                    })
                )

                # 5. Cleanup
                def _cleanup_adapter(paths_dict, **_):
                    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                    return cleanup_dataset(paths_dict, s3_hook)

                cleanup = PythonOperator.partial(
                    task_id="cleanup",
                    python_callable=_cleanup_adapter,
                    trigger_rule=TriggerRule.ALL_DONE,
                ).expand(
                    op_kwargs=prepare.output.map(lambda p: {"paths_dict": p})
                )

                prepare >> load >> validate >> commit >> cleanup
            
            table_last_tasks[table.name] = tg

        for table_name, deps in dependencies.items():
            if table_name in table_last_tasks:
                for dep in deps:
                    if dep in table_last_tasks:
                        table_last_tasks[dep] >> table_last_tasks[table_name]

        potential_downstream = [ds for ds, dps in config.layer_dependencies.items() if layer in dps]
        valid_ds = [ds for ds in potential_downstream if discover_tables_for_layer(ds, SQL_BASE_DIR)]

        if valid_ds:
            for ds in valid_ds:
                trigger = TriggerDagRunOperator(
                    task_id=f"trigger_dw_{ds}",
                    trigger_dag_id=f"dw_{ds}",
                    reset_dag_run=True,
                    conf={
                        "partition_date": "{{ dag_run.conf.get('partition_date') }}",
                        "start_date": "{{ dag_run.conf.get('start_date') }}",
                        "end_date": "{{ dag_run.conf.get('end_date') }}",
                        "targets": "{{ dag_run.conf.get('targets') }}",
                        "init": "{{ dag_run.conf.get('init') }}",
                    },
                )
                for tg in table_last_tasks.values():
                    tg >> trigger
        else:
            trigger_finish = TriggerDagRunOperator(
                task_id="trigger_dw_finish",
                trigger_dag_id="dw_finish_dag",
                reset_dag_run=True,
                conf={
                    "partition_date": "{{ dag_run.conf.get('partition_date') }}",
                    "start_date": "{{ dag_run.conf.get('start_date') }}",
                    "end_date": "{{ dag_run.conf.get('end_date') }}",
                    "init": "{{ dag_run.conf.get('init') }}",
                },
            )
            for tg in table_last_tasks.values():
                tg >> trigger_finish

    return dag


def build_dw_dags() -> dict[str, DAG]:
    config = load_dw_config(CONFIG_PATH)
    dag_map = {}
    for layer in order_layers(config):
        try:
            dag = create_layer_dag(layer, config)
            if dag:
                dag_map[dag.dag_id] = dag
        except DWConfigError:
            continue
    return dag_map


globals().update(build_dw_dags())