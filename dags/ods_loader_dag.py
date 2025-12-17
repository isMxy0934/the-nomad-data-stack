"""Airflow DAG to load ODS partitions using DuckDB and S3-compatible storage."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from airflow import DAG
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from airflow.task.trigger_rule import TriggerRule

from dags.utils.duckdb_utils import (
    S3ConnectionConfig,
    configure_s3_access,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
)
from dags.utils.partition_utils import (
    PartitionPaths,
    build_manifest,
    build_partition_paths,
    parse_s3_uri,
    publish_partition,
)
from dags.utils.sql_utils import load_and_render_sql

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")
CONFIG_PATH = Path(__file__).parent / "ods" / "config.yaml"
SQL_DIR = Path(__file__).parent / "ods"
POOL_PREFIX = "ods_pool_"


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


def get_table_pool_name(dest: str) -> str:
    """Return the Airflow pool name for a destination table."""

    return f"{POOL_PREFIX}{dest}"


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


def prepare_partition(
    table_config: dict[str, Any],
    partition_date: str,
    bucket_name: str,
    run_id: str,
    **_: Any,
) -> dict[str, str]:
    base_prefix = f"dw/ods/{table_config['dest']}"
    paths = build_partition_paths(
        base_prefix=base_prefix,
        partition_date=partition_date,
        run_id=run_id,
        bucket_name=bucket_name,
    )
    return {
        "canonical_prefix": paths.canonical_prefix,
        "tmp_prefix": paths.tmp_prefix,
        "manifest_path": paths.manifest_path,
        "success_flag_path": paths.success_flag_path,
    }


def load_partition(
    table_config: dict[str, Any],
    partition_date: str,
    bucket_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, int]:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    paths = PartitionPaths(**paths_dict)

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    connection = create_temporary_connection()
    configure_s3_access(connection, s3_config)

    source_path = table_config["src"]["properties"]["path"]
    source_uri = f"s3://{bucket_name}/{source_path}/dt={partition_date}/*.csv"
    staging_view = f"tmp_{table_config['dest']}"

    connection.execute(
        f"""
        CREATE OR REPLACE VIEW {staging_view} AS
        SELECT * FROM read_csv_auto('{source_uri}', hive_partitioning=true);
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

    file_count = len(_list_parquet_keys(s3_hook, paths.tmp_prefix))

    metrics = {"row_count": row_count, "file_count": file_count}
    ti.xcom_push(key="load_metrics", value=metrics)
    return metrics


def validate_partition(task_group_id: str, **context: Any) -> dict[str, int]:
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")
    if not metrics:
        raise ValueError("Load metrics are missing; load step did not run correctly")

    paths = PartitionPaths(**paths_dict)
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    file_count = len(_list_parquet_keys(s3_hook, paths.tmp_prefix))

    if file_count == 0:
        raise ValueError("No parquet files were written to the tmp prefix")
    if file_count != metrics["file_count"]:
        raise ValueError("File count mismatch between load metrics and S3 contents")
    if metrics["row_count"] < 0:
        raise ValueError("Row count cannot be negative")

    return metrics


def commit_partition(
    table_config: dict[str, Any],
    partition_date: str,
    bucket_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, str]:
    ti = context["ti"]
    paths = PartitionPaths(**ti.xcom_pull(task_ids=f"{task_group_id}.prepare"))
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")
    if not metrics:
        raise ValueError("Load metrics are required to publish a partition")

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    manifest = build_manifest(
        dest=table_config["dest"],
        partition_date=partition_date,
        run_id=run_id,
        file_count=int(metrics["file_count"]),
        row_count=int(metrics["row_count"]),
        source_prefix=paths.tmp_prefix,
        target_prefix=paths.canonical_prefix,
    )

    publish_result = publish_partition(
        s3_hook=s3_hook,
        paths=paths,
        manifest=manifest,
        write_success_flag=True,
    )
    ti.xcom_push(key="manifest", value=manifest)
    return publish_result


def cleanup_tmp(task_group_id: str, **context: Any) -> None:
    ti = context["ti"]
    paths = PartitionPaths(**ti.xcom_pull(task_ids=f"{task_group_id}.prepare"))
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    _delete_tmp_prefix(s3_hook, paths.tmp_prefix)


def build_table_task_group(dag: DAG, table_config: dict[str, Any]) -> TaskGroup:
    task_group_id = table_config["dest"]
    with TaskGroup(group_id=task_group_id, dag=dag) as group:
        prepare = PythonOperator(
            task_id="prepare",
            python_callable=prepare_partition,
            op_kwargs={
                "table_config": table_config,
                "partition_date": "{{ ds }}",
                "bucket_name": DEFAULT_BUCKET_NAME,
                "run_id": "{{ run_id }}",
            },
            pool=get_table_pool_name(task_group_id),
        )

        load = PythonOperator(
            task_id="load",
            python_callable=load_partition,
            op_kwargs={
                "table_config": table_config,
                "partition_date": "{{ ds }}",
                "bucket_name": DEFAULT_BUCKET_NAME,
                "run_id": "{{ run_id }}",
                "task_group_id": task_group_id,
            },
            pool=get_table_pool_name(task_group_id),
        )

        validate = PythonOperator(
            task_id="validate",
            python_callable=validate_partition,
            op_kwargs={"task_group_id": task_group_id},
        )

        commit = PythonOperator(
            task_id="commit",
            python_callable=commit_partition,
            op_kwargs={
                "table_config": table_config,
                "partition_date": "{{ ds }}",
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


def create_ods_loader_dag() -> DAG:
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    }

    dag = DAG(
        dag_id="ods_loader",
        description="Load ODS partitions from raw data into partitioned parquet",
        default_args=default_args,
        schedule="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
    )

    with dag:
        start = EmptyOperator(task_id="start")
        finish = EmptyOperator(task_id="finish")

        configs = load_ods_config(CONFIG_PATH)
        for table_config in configs:
            task_group = build_table_task_group(dag, table_config)
            start >> task_group >> finish

    return dag


dag = create_ods_loader_dag()
