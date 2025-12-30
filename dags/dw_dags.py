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

from dags.adapters import build_s3_connection_config
from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.utils.dag_run_utils import parse_targets
from dags.utils.etl_utils import (
    DEFAULT_AWS_CONN_ID,
    DEFAULT_BUCKET_NAME,
    cleanup_dataset,
    commit_dataset,
    prepare_dataset,
    validate_dataset,
)
from lakehouse_core.catalog import attach_catalog_if_available
from lakehouse_core.compute import configure_s3_access, temporary_connection
from lakehouse_core.domain.models import RunContext, RunSpec
from lakehouse_core.inputs import OdsCsvRegistrar
from lakehouse_core.io.paths import dict_to_paths
from lakehouse_core.io.time import get_partition_date_str
from lakehouse_core.pipeline import load as pipeline_load
from lakehouse_core.planning import (
    DirectoryDWPlanner,
    DWConfig,
    DWConfigError,
    load_dw_config,
    order_layers,
)

CONFIG_PATH = Path(__file__).parent / "dw_config.yaml"
SQL_BASE_DIR = Path(__file__).parent
CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", ".duckdb/catalog.duckdb"))

logger = logging.getLogger(__name__)


def _find_next_layers_with_tables(
    current_layer: str,
    config: DWConfig,
    layers_with_tables: set[str],
) -> list[str]:
    """Find all transitive downstream layers that have tables.
    
    Skips empty intermediate layers to maintain trigger chain.
    
    Example:
        ods -> dwd (empty) -> dim (has tables)
        Result: ["dim"] (skip dwd, return dim)
    
    Args:
        current_layer: Current layer name
        config: DW configuration
        layers_with_tables: Set of layers that have SQL files
    
    Returns:
        List of downstream layer names that have tables
    """
    # Find direct downstream layers (layers that depend on current_layer)
    direct_downstream = [
        ds for ds, deps in config.layer_dependencies.items()
        if current_layer in deps
    ]
    
    if not direct_downstream:
        return []
    
    # Check which direct downstream layers have tables
    valid_layers = [ds for ds in direct_downstream if ds in layers_with_tables]
    
    if valid_layers:
        # Found layers with tables, return them
        return valid_layers
    
    # No direct downstream has tables, recursively check their downstream
    # (skip empty intermediate layers)
    next_layers = set()
    for empty_layer in direct_downstream:
        # Recursively find downstream layers of this empty layer
        next_layers.update(
            _find_next_layers_with_tables(empty_layer, config, layers_with_tables)
        )
    
    return sorted(next_layers)  # Sort for deterministic order


def _conf_targets(context: Mapping[str, Any]) -> list[str] | None:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    return parse_targets(conf)


def _target_matches(run_spec: Mapping[str, Any], target: str) -> bool:
    value = (target or "").strip()
    if not value:
        return False
    layer = str(run_spec.get("layer") or "")
    table = str(run_spec.get("table") or "")
    if "." not in value:
        return value == layer
    t_layer, t_table = value.split(".", 1)
    return t_layer == layer and t_table == table


def _attach_catalog_if_available(connection) -> None:
    attach_catalog_if_available(connection, catalog_path=CATALOG_PATH)


def load_table(
    paths_dict: dict[str, Any],
    run_spec: Mapping[str, Any],
    bucket_name: str = DEFAULT_BUCKET_NAME,
    **context: Any,
) -> dict[str, Any]:
    targets = _conf_targets(context)
    if targets is not None and not any(_target_matches(run_spec, t) for t in targets):
        table_name = str(run_spec.get("table") or "")
        logger.info("Skipping table %s (not in targets).", table_name)
        return {"row_count": 0, "file_count": 0, "has_data": 0, "skipped": 1}

    partition_date = str(paths_dict.get("partition_date"))

    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)
    store = AirflowS3Store(s3_hook)

    spec = RunSpec(**dict(run_spec))
    if bool(paths_dict.get("partitioned")) != bool(spec.is_partitioned):
        raise ValueError("paths_dict.partitioned does not match RunSpec.is_partitioned")

    # Use unified deserialization from lakehouse_core
    paths = dict_to_paths(paths_dict)

    with temporary_connection() as connection:
        configure_s3_access(connection, s3_config)
        _attach_catalog_if_available(connection)

        registrars = [OdsCsvRegistrar()] if spec.layer == "ods" else []
        metrics = pipeline_load(
            spec=spec,
            paths=paths,
            partition_date=partition_date if spec.is_partitioned else None,
            store=store,
            connection=connection,
            base_uri=f"s3://{bucket_name}",
            registrars=registrars,
        )

        metrics["partitioned"] = int(bool(spec.is_partitioned))
        return dict(metrics)


def create_layer_dag(layer: str, config: DWConfig) -> DAG | None:
    planner = DirectoryDWPlanner(dw_config_path=CONFIG_PATH, sql_base_dir=SQL_BASE_DIR)
    specs = planner.build(RunContext(run_id="plan", extra={"allow_unrendered_sql": True}))
    layers_with_tables = {spec.layer for spec in specs}
    layer_specs = [spec for spec in specs if spec.layer == layer]
    if not layer_specs:
        return None

    dependencies = config.table_dependencies.get(layer, {})
    ordered_specs = [spec.__dict__ for spec in layer_specs]

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
        def get_partition_dates(**context: Any) -> list[str]:
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
                dates = [partition_date] if partition_date else [get_partition_date_str()]

            return sorted(set(dates))

        partition_dates = get_partition_dates()
        table_last_tasks = {}

        for spec_dict in ordered_specs:
            table_name = str(spec_dict.get("table") or "")
            from airflow.utils.task_group import TaskGroup

            with TaskGroup(group_id=table_name) as tg:
                # 1. Prepare
                def _prepare_adapter(base_prefix, is_partitioned, partition_date, **context):
                    # Sanitize run_id for S3/MinIO compatibility
                    safe_run_id = "".join(
                        c if c.isalnum() or c in "-_" else "_" for c in context["run_id"]
                    )
                    # Inject partition_date into run_id to avoid parallel conflict in DuckDB COPY
                    unique_run_id = f"{safe_run_id}_{partition_date.replace('-', '')}"
                    return prepare_dataset(
                        base_prefix=base_prefix,
                        run_id=unique_run_id,
                        is_partitioned=is_partitioned,
                        partition_date=partition_date,
                    )

                prepare = PythonOperator.partial(
                    task_id="prepare",
                    python_callable=_prepare_adapter,
                ).expand(
                    op_kwargs=partition_dates.map(
                        lambda d, spec=spec_dict: {
                            "base_prefix": str(spec["base_prefix"]),
                            "is_partitioned": bool(spec.get("is_partitioned", True)),
                            "partition_date": d,
                        }
                    )
                )

                # 2. Load
                load = PythonOperator.partial(
                    task_id="load",
                    python_callable=load_table,
                ).expand(
                    op_kwargs=prepare.output.map(
                        lambda p, spec=spec_dict, table_name=table_name: {
                            "run_spec": spec,
                            "bucket_name": DEFAULT_BUCKET_NAME,
                            "task_group_id": table_name,
                            "paths_dict": p,
                        }
                    )
                )

                # 3. Validate
                def _validate_adapter(paths_dict, metrics, **_):
                    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                    return validate_dataset(paths_dict, metrics, s3_hook)

                validate = PythonOperator.partial(
                    task_id="validate",
                    python_callable=_validate_adapter,
                ).expand(
                    op_kwargs=prepare.output.zip(load.output).map(
                        lambda x: {"paths_dict": x[0], "metrics": x[1]}
                    )
                )

                # 4. Commit
                def _commit_adapter(paths_dict, metrics, dest_name, **context):
                    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                    # Use the same unique run_id as prepare - sanitize run_id first
                    safe_run_id = "".join(
                        c if c.isalnum() or c in "-_" else "_" for c in context["run_id"]
                    )
                    partition_date = str(paths_dict.get("partition_date"))
                    unique_run_id = f"{safe_run_id}_{partition_date.replace('-', '')}"
                    res, _ = commit_dataset(dest_name, unique_run_id, paths_dict, metrics, s3_hook)
                    return res

                commit = PythonOperator.partial(
                    task_id="commit",
                    python_callable=_commit_adapter,
                ).expand(
                    op_kwargs=prepare.output.zip(validate.output).map(
                        lambda x, table_name=table_name: {
                            "paths_dict": x[0],
                            "metrics": x[1],
                            "dest_name": table_name,
                        }
                    )
                )

                # 5. Cleanup
                def _cleanup_adapter(paths_dict, **_):
                    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                    return cleanup_dataset(paths_dict, s3_hook)

                cleanup = PythonOperator.partial(
                    task_id="cleanup",
                    python_callable=_cleanup_adapter,
                    trigger_rule=TriggerRule.ALL_DONE,
                ).expand(op_kwargs=prepare.output.map(lambda p: {"paths_dict": p}))

                prepare >> load >> validate >> commit >> cleanup

            table_last_tasks[table_name] = tg

        for table_name, deps in dependencies.items():
            if table_name in table_last_tasks:
                for dep in deps:
                    if dep in table_last_tasks:
                        table_last_tasks[dep] >> table_last_tasks[table_name]

        # Trigger downstream layers (skip empty intermediate layers)
        next_layers = _find_next_layers_with_tables(layer, config, layers_with_tables)

        if next_layers:
            for ds in next_layers:
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
            # No downstream layers with tables, trigger finish DAG
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
