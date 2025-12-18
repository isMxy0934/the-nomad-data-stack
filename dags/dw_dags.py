"""Dynamically generate DW DAGs based on dw_config.yaml and SQL directories."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dags.utils.dw_config_utils import (
    DWConfig,
    DWConfigError,
    TableSpec,
    discover_tables_for_layer,
    load_dw_config,
    order_layers,
    order_tables_within_layer,
)
from dags.utils.duckdb_utils import (
    S3ConnectionConfig,
    configure_s3_access,
    copy_parquet,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
)
from dags.utils.partition_utils import (
    NonPartitionPaths,
    PartitionPaths,
    build_manifest,
    build_non_partition_paths,
    build_partition_paths,
    parse_s3_uri,
    publish_non_partition,
    publish_partition,
)
from dags.utils.sql_utils import load_and_render_sql
from dags.utils.time_utils import get_partition_date_str

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")
CONFIG_PATH = Path(__file__).parent / "dw_config.yaml"
SQL_BASE_DIR = Path(__file__).parent
CATALOG_PATH = Path(".duckdb") / "catalog.duckdb"
POOL_PREFIX = "dw_pool_"

logger = logging.getLogger(__name__)


def build_s3_connection_config(s3_hook: S3Hook) -> S3ConnectionConfig:
    """Construct DuckDB S3 settings from an Airflow connection."""

    connection: Connection = s3_hook.get_connection(s3_hook.aws_conn_id)
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


def _list_parquet_keys(s3_hook: S3Hook, prefix_uri: str) -> list[str]:
    bucket, key_prefix = parse_s3_uri(prefix_uri)
    normalized_prefix = key_prefix.strip("/") + "/"
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
    return [key for key in keys if key.endswith(".parquet")]


def _delete_tmp_prefix(s3_hook: S3Hook, prefix_uri: str) -> None:
    bucket, key_prefix = parse_s3_uri(prefix_uri)
    normalized_prefix = key_prefix.strip("/") + "/"
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
    if keys:
        s3_hook.delete_objects(bucket=bucket, keys=keys)


def _attach_catalog_if_available(connection) -> None:
    if not CATALOG_PATH.exists():
        logger.info("Catalog file %s not found; skipping attach.", CATALOG_PATH)
        return
    connection.execute(f"ATTACH '{CATALOG_PATH}' AS catalog (READ_ONLY);")
    connection.execute("SET search_path=catalog, main;")


def _is_partitioned_table(table_spec: TableSpec) -> bool:
    return bool(table_spec.is_partitioned)


def prepare_table(table_spec: TableSpec, run_id: str, **_: Any) -> dict[str, Any]:
    partition_date = get_partition_date_str()
    base_prefix = f"lake/{table_spec.layer}/{table_spec.name}"

    if _is_partitioned_table(table_spec):
        paths = build_partition_paths(
            base_prefix=base_prefix,
            partition_date=partition_date,
            run_id=run_id,
            bucket_name=DEFAULT_BUCKET_NAME,
        )
        return {
            "partition_date": partition_date,
            "partitioned": True,
            "canonical_prefix": paths.canonical_prefix,
            "tmp_prefix": paths.tmp_prefix,
            "tmp_partition_prefix": paths.tmp_partition_prefix,
            "manifest_path": paths.manifest_path,
            "success_flag_path": paths.success_flag_path,
        }

    paths = build_non_partition_paths(
        base_prefix=base_prefix,
        run_id=run_id,
        bucket_name=DEFAULT_BUCKET_NAME,
    )
    return {
        "partition_date": partition_date,
        "partitioned": False,
        "canonical_prefix": paths.canonical_prefix,
        "tmp_prefix": paths.tmp_prefix,
        "manifest_path": paths.manifest_path,
        "success_flag_path": paths.success_flag_path,
    }


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
        file_count = len(_list_parquet_keys(s3_hook, paths_dict["tmp_partition_prefix"]))
    else:
        copy_parquet(
            connection,
            query=rendered_sql,
            destination=destination_prefix,
            filename_pattern="file_{uuid}",
            use_tmp_file=True,
        )
        file_count = len(_list_parquet_keys(s3_hook, destination_prefix))

    metrics = {
        "row_count": row_count,
        "file_count": file_count,
        "has_data": 1,
        "partitioned": int(partitioned),
    }
    ti.xcom_push(key="load_metrics", value=metrics)
    return metrics


def validate_table(task_group_id: str, **context: Any) -> dict[str, int]:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")
    if not metrics:
        raise ValueError("Load metrics are missing; load step did not run correctly")

    partitioned = bool(paths_dict.get("partitioned"))
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    if not int(metrics.get("has_data", 1)):
        file_count = len(_list_parquet_keys(s3_hook, paths_dict["tmp_prefix"]))
        if file_count != 0:
            raise ValueError("Expected no parquet files in tmp prefix for empty source result")
        return metrics

    file_count = len(
        _list_parquet_keys(
            s3_hook,
            paths_dict["tmp_partition_prefix"] if partitioned else paths_dict["tmp_prefix"],
        )
    )

    if file_count == 0:
        raise ValueError("No parquet files were written to the tmp prefix")
    if file_count != metrics["file_count"]:
        raise ValueError("File count mismatch between load metrics and S3 contents")
    if metrics["row_count"] < 0:
        raise ValueError("Row count cannot be negative")

    return metrics


def commit_table(
    table_spec: TableSpec,
    bucket_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, str]:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")
    if not metrics:
        raise ValueError("Load metrics are required to publish a table")

    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = paths_dict.get("partition_date")
    if not int(metrics.get("has_data", 1)):
        logger.info(
            "No data for %s (dt=%s); skipping publish.",
            table_spec.name,
            partition_date,
        )
        return {"published": "0"}

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)

    if partitioned:
        paths = PartitionPaths(**paths_dict)
        manifest = build_manifest(
            dest=table_spec.name,
            partition_date=partition_date,
            run_id=run_id,
            file_count=int(metrics["file_count"]),
            row_count=int(metrics["row_count"]),
            source_prefix=paths.tmp_partition_prefix,
            target_prefix=paths.canonical_prefix,
        )
        publish_result = publish_partition(
            s3_hook=s3_hook,
            paths=paths,
            manifest=manifest,
            write_success_flag=True,
        )
    else:
        paths = NonPartitionPaths(**paths_dict)
        manifest = build_manifest(
            dest=table_spec.name,
            partition_date=partition_date,
            run_id=run_id,
            file_count=int(metrics["file_count"]),
            row_count=int(metrics["row_count"]),
            source_prefix=paths.tmp_prefix,
            target_prefix=paths.canonical_prefix,
        )
        publish_result = publish_non_partition(
            s3_hook=s3_hook,
            paths=paths,
            manifest=manifest,
            write_success_flag=True,
        )

    ti.xcom_push(key="manifest", value=manifest)
    return publish_result


def cleanup_tmp(task_group_id: str, **context: Any) -> None:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    _delete_tmp_prefix(s3_hook, paths_dict["tmp_prefix"])


def get_table_pool_name(table_name: str) -> str:
    return f"{POOL_PREFIX}{table_name}"


def build_table_task_group(dag: DAG, table_spec: TableSpec) -> TaskGroup:
    task_group_id = table_spec.name
    with TaskGroup(group_id=task_group_id, dag=dag) as group:
        prepare = PythonOperator(
            task_id="prepare",
            python_callable=prepare_table,
            op_kwargs={
                "table_spec": table_spec,
                "run_id": "{{ run_id }}",
            },
            pool=get_table_pool_name(task_group_id),
        )

        load = PythonOperator(
            task_id="load",
            python_callable=load_table,
            op_kwargs={
                "table_spec": table_spec,
                "bucket_name": DEFAULT_BUCKET_NAME,
                "run_id": "{{ run_id }}",
                "task_group_id": task_group_id,
            },
            pool=get_table_pool_name(task_group_id),
        )

        validate = PythonOperator(
            task_id="validate",
            python_callable=validate_table,
            op_kwargs={"task_group_id": task_group_id},
        )

        commit = PythonOperator(
            task_id="commit",
            python_callable=commit_table,
            op_kwargs={
                "table_spec": table_spec,
                "bucket_name": DEFAULT_BUCKET_NAME,
                "run_id": "{{ run_id }}",
                "task_group_id": task_group_id,
            },
            pool=get_table_pool_name(task_group_id),
        )

        cleanup = PythonOperator(
            task_id="cleanup",
            python_callable=cleanup_tmp,
            op_kwargs={"task_group_id": task_group_id},
            trigger_rule=TriggerRule.ALL_DONE,
        )

        prepare >> load >> validate >> commit >> cleanup
    return group


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
        schedule="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
    )

    with dag:
        task_groups: dict[str, TaskGroup] = {}
        for table in ordered_tables:
            task_groups[table.name] = build_table_task_group(dag, table)

        for table, deps in dependencies.items():
            if table not in task_groups:
                continue
            for dep in deps:
                if dep in task_groups:
                    task_groups[dep] >> task_groups[table]

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
