"""Common utilities and task functions for ETL DAGs."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dags.utils.catalog_utils import LayerSpec, build_layer_dt_macro_sql, build_layer_view_sql
from dags.utils.duckdb_utils import (
    S3ConnectionConfig,
    configure_s3_access,
    create_temporary_connection,
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
from dags.utils.time_utils import get_partition_date_str

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")
CATALOG_PATH = Path(".duckdb") / "catalog.duckdb"
DUCKDB_CATALOG_POOL = "duckdb_catalog_pool"

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


def list_parquet_keys(s3_hook: S3Hook, prefix_uri: str) -> list[str]:
    """List parquet files under a given S3 URI prefix."""
    bucket, key_prefix = parse_s3_uri(prefix_uri)
    normalized_prefix = key_prefix.strip("/") + "/"
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
    return [key for key in keys if key.endswith(".parquet")]


def delete_tmp_prefix(s3_hook: S3Hook, prefix_uri: str) -> None:
    """Delete all objects under a temporary S3 URI prefix."""
    bucket, key_prefix = parse_s3_uri(prefix_uri)
    normalized_prefix = key_prefix.strip("/") + "/"
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=normalized_prefix) or []
    if keys:
        s3_hook.delete_objects(bucket=bucket, keys=keys)


def prepare_dataset(
    base_prefix: str,
    run_id: str,
    is_partitioned: bool = True,
    bucket_name: str = DEFAULT_BUCKET_NAME,
    **_: Any,
) -> dict[str, Any]:
    """Prepare generic paths for a dataset write operation."""
    partition_date = get_partition_date_str()

    if is_partitioned:
        paths = build_partition_paths(
            base_prefix=base_prefix,
            partition_date=partition_date,
            run_id=run_id,
            bucket_name=bucket_name,
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
        bucket_name=bucket_name,
    )
    return {
        "partition_date": partition_date,
        "partitioned": False,
        "canonical_prefix": paths.canonical_prefix,
        "tmp_prefix": paths.tmp_prefix,
        "manifest_path": paths.manifest_path,
        "success_flag_path": paths.success_flag_path,
    }


def validate_dataset(
    task_group_id: str,
    **context: Any,
) -> dict[str, int]:
    """Validate that the load step produced the expected output."""
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")

    if not metrics:
        raise ValueError("Load metrics are missing; load step did not run correctly")

    partitioned = bool(paths_dict.get("partitioned"))
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)

    if not int(metrics.get("has_data", 1)):
        # If no data expected, ensure no files written
        file_count = len(list_parquet_keys(s3_hook, paths_dict["tmp_prefix"]))
        if file_count != 0:
            raise ValueError("Expected no parquet files in tmp prefix for empty source result")
        return metrics

    # Verify actual files on S3 match reported metrics
    target_prefix = paths_dict["tmp_partition_prefix"] if partitioned else paths_dict["tmp_prefix"]
    file_count = len(list_parquet_keys(s3_hook, target_prefix))

    if file_count == 0:
        raise ValueError("No parquet files were written to the tmp prefix")
    if file_count != metrics["file_count"]:
        raise ValueError("File count mismatch between load metrics and S3 contents")
    if metrics["row_count"] < 0:
        raise ValueError("Row count cannot be negative")

    return metrics


def commit_dataset(
    dest_name: str,
    run_id: str,
    task_group_id: str,
    **context: Any,
) -> dict[str, str]:
    """Publish the dataset by promoting tmp files to canonical location."""
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")

    if not metrics:
        raise ValueError("Load metrics are required to publish a dataset")

    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = paths_dict.get("partition_date")

    if not int(metrics.get("has_data", 1)):
        logger.info(
            "No data for %s (dt=%s); skipping publish.",
            dest_name,
            partition_date,
        )
        return {"published": "0"}

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)

    if partitioned:
        # Reconstruct PartitionPaths explicitly to avoid unexpected keyword arguments (e.g. 'partitioned')
        paths = PartitionPaths(
            partition_date=paths_dict["partition_date"],
            canonical_prefix=paths_dict["canonical_prefix"],
            tmp_prefix=paths_dict["tmp_prefix"],
            tmp_partition_prefix=paths_dict["tmp_partition_prefix"],
            manifest_path=paths_dict["manifest_path"],
            success_flag_path=paths_dict["success_flag_path"],
        )
        manifest = build_manifest(
            dest=dest_name,
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
        # Reconstruct NonPartitionPaths from dict
        paths = NonPartitionPaths(
            canonical_prefix=paths_dict["canonical_prefix"],
            tmp_prefix=paths_dict["tmp_prefix"],
            manifest_path=paths_dict["manifest_path"],
            success_flag_path=paths_dict["success_flag_path"],
        )
        manifest = build_manifest(
            dest=dest_name,
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


def cleanup_dataset(task_group_id: str, **context: Any) -> None:
    """Clean up temporary files."""
    ti = context["ti"]
    paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    delete_tmp_prefix(s3_hook, paths_dict["tmp_prefix"])


def refresh_catalog_entry(
    dest_name: str,
    base_prefix: str,
    bucket_name: str,
    is_partitioned: bool,
    task_group_id: str,
    **context: Any,
) -> None:
    """Refresh the DuckDB catalog view for a specific table."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")

    # If load step reported no data, skip refresh to avoid DuckDB "No files found" error
    if metrics and not int(metrics.get("has_data", 1)):
        logger.info("No data loaded for %s; skipping catalog refresh.", dest_name)
        return

    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Infer layer info from base_prefix (e.g., lake/ods/table -> lake/ods)
    # This assumes the standard layout: lake/{layer}/{table}
    parts = base_prefix.strip("/").split("/")
    if len(parts) >= 3:
        layer_prefix = "/".join(parts[:-1])
        layer_name = parts[1]  # lake, ods, table -> ods
    else:
        # Fallback/Edge case
        layer_prefix = "lake/unknown"
        layer_name = "public"

    layer = LayerSpec(
        schema=layer_name,
        base_prefix=layer_prefix,
        partitioned_by_dt=is_partitioned,
    )

    logger.info("Refreshing catalog for %s.%s", layer_name, dest_name)

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)

    connection = create_temporary_connection(database=CATALOG_PATH)
    configure_s3_access(connection, s3_config)

    # Ensure schema exists
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {layer_name};")

    # Update View
    view_sql = build_layer_view_sql(bucket=bucket_name, layer=layer, table=dest_name)
    connection.execute(view_sql)

    # Update Macro (if partitioned)
    if is_partitioned:
        macro_sql = build_layer_dt_macro_sql(bucket=bucket_name, layer=layer, table=dest_name)
        connection.execute(macro_sql)


def build_etl_task_group(
    dag: DAG,
    task_group_id: str,
    dest_name: str,
    base_prefix: str,
    load_callable: Callable[..., Any],
    load_op_kwargs: dict[str, Any] | None = None,
    is_partitioned: bool = True,
    pool_name: str | None = None,
) -> TaskGroup:
    """Build a standard ETL task group (prepare -> load -> validate -> commit -> cleanup -> refresh).

    Args:
        dag: The Airflow DAG.
        task_group_id: Unique ID for the TaskGroup (usually table name).
        dest_name: Destination name (table name) for manifest.
        base_prefix: S3 base prefix for storage (e.g. 'lake/ods/table').
        load_callable: The Python function that performs the actual data load (DuckDB COPY).
        load_op_kwargs: Arguments to pass to the load_callable.
        is_partitioned: Whether the dataset is partitioned by date.
        pool_name: Airflow pool to limit concurrency (single writer).

    Returns:
        The constructed TaskGroup.
    """
    if load_op_kwargs is None:
        load_op_kwargs = {}

    with TaskGroup(group_id=task_group_id, dag=dag) as group:
        prepare = PythonOperator(
            task_id="prepare",
            python_callable=prepare_dataset,
            op_kwargs={
                "base_prefix": base_prefix,
                "run_id": "{{ run_id }}",
                "is_partitioned": is_partitioned,
            },
            pool=pool_name,
        )

        # Inject runtime context into load args
        final_load_kwargs = load_op_kwargs.copy()
        final_load_kwargs.update(
            {
                "bucket_name": DEFAULT_BUCKET_NAME,
                "run_id": "{{ run_id }}",
                "task_group_id": task_group_id,
            }
        )

        load = PythonOperator(
            task_id="load",
            python_callable=load_callable,
            op_kwargs=final_load_kwargs,
            pool=pool_name,
        )

        validate = PythonOperator(
            task_id="validate",
            python_callable=validate_dataset,
            op_kwargs={"task_group_id": task_group_id},
        )

        commit = PythonOperator(
            task_id="commit",
            python_callable=commit_dataset,
            op_kwargs={
                "dest_name": dest_name,
                "run_id": "{{ run_id }}",
                "task_group_id": task_group_id,
            },
            pool=pool_name,
        )

        cleanup = PythonOperator(
            task_id="cleanup",
            python_callable=cleanup_dataset,
            op_kwargs={"task_group_id": task_group_id},
            trigger_rule=TriggerRule.ALL_DONE,
        )

        refresh = PythonOperator(
            task_id="refresh_catalog",
            python_callable=refresh_catalog_entry,
            op_kwargs={
                "dest_name": dest_name,
                "base_prefix": base_prefix,
                "bucket_name": DEFAULT_BUCKET_NAME,
                "is_partitioned": is_partitioned,
                "task_group_id": task_group_id,
            },
            pool=DUCKDB_CATALOG_POOL,  # Serialized execution
            trigger_rule=TriggerRule.ALL_SUCCESS,  # Only refresh if everything succeeded
        )

        prepare >> load >> validate >> commit >> cleanup >> refresh

    return group
