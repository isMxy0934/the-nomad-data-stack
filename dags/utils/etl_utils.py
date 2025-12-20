"""Common utilities and task functions for ETL DAGs."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Mapping, MutableMapping
from typing import Any

from airflow import DAG
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from dags.utils.duckdb_utils import (
    S3ConnectionConfig,
)
from lakehouse_core.api import prepare_paths
from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.pipeline import cleanup as pipeline_cleanup
from lakehouse_core.pipeline import commit as pipeline_commit
from lakehouse_core.pipeline import validate as pipeline_validate
from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.utils.time_utils import get_partition_date_str

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")

logger = logging.getLogger(__name__)


def partition_paths_from_xcom(paths_dict: Mapping[str, Any]) -> PartitionPaths:
    """Reconstruct ``PartitionPaths`` from XCom payloads produced by ``prepare_dataset``."""

    return PartitionPaths(
        partition_date=str(paths_dict["partition_date"]),
        canonical_prefix=str(paths_dict["canonical_prefix"]),
        tmp_prefix=str(paths_dict["tmp_prefix"]),
        tmp_partition_prefix=str(paths_dict["tmp_partition_prefix"]),
        manifest_path=str(paths_dict["manifest_path"]),
        success_flag_path=str(paths_dict["success_flag_path"]),
    )


def non_partition_paths_from_xcom(paths_dict: Mapping[str, Any]) -> NonPartitionPaths:
    """Reconstruct ``NonPartitionPaths`` from XCom payloads produced by ``prepare_dataset``."""

    return NonPartitionPaths(
        canonical_prefix=str(paths_dict["canonical_prefix"]),
        tmp_prefix=str(paths_dict["tmp_prefix"]),
        manifest_path=str(paths_dict["manifest_path"]),
        success_flag_path=str(paths_dict["success_flag_path"]),
    )


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


def _strip_slashes(path: str) -> str:
    return path.strip("/")


def prepare_dataset(
    base_prefix: str,
    run_id: str,
    is_partitioned: bool = True,
    bucket_name: str = DEFAULT_BUCKET_NAME,
    partition_date: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    """Prepare generic paths for a dataset write operation."""
    if partition_date is None or str(partition_date).strip() in {"", "None", "null"}:
        partition_date = get_partition_date_str()
    else:
        partition_date = str(partition_date).strip()

    if is_partitioned:
        paths = prepare_paths(
            base_prefix=_strip_slashes(base_prefix),
            run_id=run_id,
            partition_date=partition_date,
            is_partitioned=True,
            store_namespace=bucket_name,
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

    paths = prepare_paths(
        base_prefix=_strip_slashes(base_prefix),
        run_id=run_id,
        partition_date=None,
        is_partitioned=False,
        store_namespace=bucket_name,
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
    paths_dict: Mapping[str, Any],
    metrics: Mapping[str, int],
    s3_hook: S3Hook,
) -> dict[str, int]:
    """Core logic to validate dataset output (delegates to lakehouse_core API)."""
    store = AirflowS3Store(s3_hook)
    partitioned = bool(paths_dict.get("partitioned"))

    # Backwards-compatible: some unit tests pass a partial paths_dict with only tmp_prefix.
    if partitioned:
        try:
            paths = partition_paths_from_xcom(paths_dict)
        except KeyError:
            tmp_prefix = str(paths_dict["tmp_prefix"])
            partition_date = str(paths_dict.get("partition_date") or "").strip()
            tmp_partition_prefix = str(paths_dict.get("tmp_partition_prefix") or "").strip()
            if not tmp_partition_prefix and partition_date:
                tmp_partition_prefix = f"{tmp_prefix}/dt={partition_date}"
            paths = PartitionPaths(
                partition_date=partition_date,
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=tmp_prefix,
                tmp_partition_prefix=tmp_partition_prefix,
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )
    else:
        try:
            paths = non_partition_paths_from_xcom(paths_dict)
        except KeyError:
            paths = NonPartitionPaths(
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=str(paths_dict["tmp_prefix"]),
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )
    return pipeline_validate(store=store, paths=paths, metrics=metrics)


def commit_dataset(
    dest_name: str,
    run_id: str,
    paths_dict: Mapping[str, Any],
    metrics: Mapping[str, int],
    s3_hook: S3Hook,
) -> tuple[dict[str, str], MutableMapping[str, object]]:
    """Commit tmp outputs into canonical and write markers (pipeline-backed)."""
    partitioned = bool(paths_dict.get("partitioned"))
    partition_date = str(paths_dict.get("partition_date"))

    store = AirflowS3Store(s3_hook)
    if partitioned:
        paths = partition_paths_from_xcom(paths_dict)
        canonical_prefix = paths.canonical_prefix
    else:
        paths = non_partition_paths_from_xcom(paths_dict)
        canonical_prefix = paths.canonical_prefix

    publish_result, manifest = pipeline_commit(
        store=store,
        paths=paths,
        dest=dest_name,
        run_id=run_id,
        partition_date=partition_date,
        metrics=metrics,
        write_success_flag=True,
    )
    if publish_result.get("action") == "cleared":
        logger.info(
            "No data for %s (dt=%s); clearing partition and skipping publish.",
            dest_name,
            partition_date,
        )
    return publish_result, manifest


def cleanup_dataset(
    paths_dict: Mapping[str, Any],
    s3_hook: S3Hook,
) -> None:
    """Clean up temporary files (delegates to lakehouse_core API)."""
    store = AirflowS3Store(s3_hook)
    tmp_prefix = str(paths_dict["tmp_prefix"])
    partitioned = bool(paths_dict.get("partitioned"))
    if partitioned:
        try:
            paths = partition_paths_from_xcom(paths_dict)
        except KeyError:
            paths = PartitionPaths(
                partition_date=str(paths_dict.get("partition_date") or ""),
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=tmp_prefix,
                tmp_partition_prefix=str(paths_dict.get("tmp_partition_prefix") or ""),
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )
    else:
        try:
            paths = non_partition_paths_from_xcom(paths_dict)
        except KeyError:
            paths = NonPartitionPaths(
                canonical_prefix=str(paths_dict.get("canonical_prefix") or ""),
                tmp_prefix=tmp_prefix,
                manifest_path=str(paths_dict.get("manifest_path") or ""),
                success_flag_path=str(paths_dict.get("success_flag_path") or ""),
            )

    pipeline_cleanup(store=store, paths=paths)


def build_etl_task_group(
    dag: DAG,
    task_group_id: str,
    dest_name: str,
    base_prefix: str,
    load_callable: Callable[..., Any],
    load_op_kwargs: dict[str, Any] | None = None,
    is_partitioned: bool = True,
    pool_name: str | None = None,
    partition_date: str | None = None,
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
        partition_date: Optional partition date (string or Jinja template).

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
                "partition_date": partition_date,
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

        # --- Internal Adapters (Bridge XCom -> Pure Functions) ---
        def _validate_adapter(task_group_id: str, **context: Any) -> dict[str, int]:
            ti = context["ti"]
            paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
            metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")
            if not metrics:
                raise ValueError("Load metrics are missing")
            if metrics.get("skipped"):
                return dict(metrics)
            s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
            return validate_dataset(paths_dict, metrics, s3_hook)

        def _commit_adapter(
            dest_name: str, run_id: str, task_group_id: str, **context: Any
        ) -> dict[str, str]:
            ti = context["ti"]
            paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
            metrics = ti.xcom_pull(task_ids=f"{task_group_id}.load", key="load_metrics")
            if not metrics:
                raise ValueError("Load metrics are required to publish")
            if metrics.get("skipped"):
                logger.info("Skipping publish for %s (marked skipped).", dest_name)
                return {"published": "0", "action": "skipped"}
            s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
            publish_result, manifest = commit_dataset(
                dest_name, run_id, paths_dict, metrics, s3_hook
            )
            if manifest:
                ti.xcom_push(key="manifest", value=manifest)
            return publish_result

        def _cleanup_adapter(task_group_id: str, **context: Any) -> None:
            ti = context["ti"]
            paths_dict = ti.xcom_pull(task_ids=f"{task_group_id}.prepare")
            s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
            cleanup_dataset(paths_dict, s3_hook)

        validate = PythonOperator(
            task_id="validate",
            python_callable=_validate_adapter,
            op_kwargs={"task_group_id": task_group_id},
        )

        commit = PythonOperator(
            task_id="commit",
            python_callable=_commit_adapter,
            op_kwargs={
                "dest_name": dest_name,
                "run_id": "{{ run_id }}",
                "task_group_id": task_group_id,
            },
            pool=pool_name,
        )

        cleanup = PythonOperator(
            task_id="cleanup",
            python_callable=_cleanup_adapter,
            op_kwargs={"task_group_id": task_group_id},
            trigger_rule=TriggerRule.ALL_DONE,
        )

        prepare >> load >> validate >> commit >> cleanup

    return group
