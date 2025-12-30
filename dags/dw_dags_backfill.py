"""Backfill DAGs for DW layers - optimized for batch date range processing.

This module creates independent backfill DAGs (dw_{layer}_backfill) that process
multiple dates in a single task, reusing DuckDB connections for better performance.

Key differences from daily DAGs (dw_dags.py):
- Each table has 1 task (instead of 5 tasks per date)
- DuckDB connection is reused across all dates
- Optimized for large date ranges (e.g., 90 days)
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters import build_s3_connection_config
from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.utils.dag_run_utils import parse_bool_param, parse_int_param, parse_targets
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
    """Extract targets filter from DAG run conf."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    return parse_targets(conf)


def _target_matches(run_spec: Mapping[str, Any], target: str) -> bool:
    """Check if run_spec matches the target filter."""
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
    """Attach catalog if available (metadata-only)."""
    attach_catalog_if_available(connection, catalog_path=CATALOG_PATH)


def _generate_dates(start_date: str, end_date: str) -> list[str]:
    """Generate list of dates between start_date and end_date (inclusive)."""
    s_dt = date.fromisoformat(start_date)
    e_dt = date.fromisoformat(end_date)
    dates = []
    curr = s_dt
    while curr <= e_dt:
        dates.append(curr.isoformat())
        curr += timedelta(days=1)
    return dates


def backfill_table_range(
    run_spec: Mapping[str, Any],
    start_date: str,
    end_date: str,
    batch_size: int = 30,
    continue_on_error: bool = False,
    bucket_name: str = DEFAULT_BUCKET_NAME,
    **context: Any,
) -> dict[str, Any]:
    """Backfill a single table across a date range.

    This function processes multiple dates in a single task, reusing the DuckDB
    connection to reduce initialization overhead. Each date goes through the full
    ETL pipeline: prepare -> load -> validate -> commit -> cleanup.

    Args:
        run_spec: Table specification containing metadata for the ETL pipeline
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD, inclusive)
        batch_size: Unused (kept for API compatibility)
        continue_on_error: If True, continue processing remaining dates on error;
                          if False, stop immediately on first error
        bucket_name: S3 bucket name
        **context: Airflow context (run_id, dag_run, etc.)

    Returns:
        Dictionary containing aggregated metrics across all dates:
        - total_dates: Total number of dates to process
        - successful_count: Number of successfully processed dates
        - failed_count: Number of failed dates
        - skipped_count: Number of skipped dates (table not in targets)
        - successful_dates: List of successfully processed dates
        - failed_dates: List of failed dates with error details
    """
    # 0. Normalize parameters from Jinja rendering (TriggerDagRunOperator passes strings)
    continue_on_error = parse_bool_param(continue_on_error, False)
    batch_size = parse_int_param(batch_size, 30)
    
    spec = RunSpec(**dict(run_spec))
    table_name = str(spec.table)
    layer = str(spec.layer)

    # 1. Check targets filter early (avoid unnecessary work)
    targets = _conf_targets(context)
    if targets is not None and not any(_target_matches(run_spec, t) for t in targets):
        dates = _generate_dates(start_date, end_date)
        logger.info(
            f"[Backfill] Skipping {layer}.{table_name} (not in targets), "
            f"{len(dates)} dates skipped"
        )
        return {
            "skipped": True,
            "total_dates": len(dates),
            "skipped_count": len(dates),
            "successful_count": 0,
            "failed_count": 0,
            "first_success": None,
            "last_success": None,
            "failed_dates": [],
        }

    # 2. Handle non-partitioned tables (only process once)
    if not spec.is_partitioned:
        logger.info(
            f"[Backfill] Table {layer}.{table_name} is non-partitioned, "
            f"processing once (ignoring date range)"
        )
        # Process only once for non-partitioned tables
        dates = [start_date]  # Use start_date as a placeholder
    else:
        dates = _generate_dates(start_date, end_date)
        
    logger.info(
        f"[Backfill] Starting backfill for {layer}.{table_name}: "
        f"{len(dates)} dates ({start_date} to {end_date}), "
        f"continue_on_error={continue_on_error}, "
        f"is_partitioned={spec.is_partitioned}"
    )

    # 3. Initialize S3 connection and store
    s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
    s3_config = build_s3_connection_config(s3_hook)
    store = AirflowS3Store(s3_hook)

    # 4. Process all dates with a single DuckDB connection (key optimization)
    successful_dates = []
    failed_dates = []

    with temporary_connection() as connection:
        configure_s3_access(connection, s3_config)
        _attach_catalog_if_available(connection)

        for partition_date in dates:
            paths_dict = None
            try:
                logger.info(f"[Backfill] Processing {layer}.{table_name} dt={partition_date}")

                # Generate unique run_id for this date to avoid conflicts
                safe_run_id = "".join(
                    c if c.isalnum() or c in "-_" else "_" for c in context["run_id"]
                )
                unique_run_id = f"{safe_run_id}_{partition_date.replace('-', '')}"

                # Prepare: Create temporary partition paths
                paths_dict = prepare_dataset(
                    base_prefix=str(spec.base_prefix),
                    run_id=unique_run_id,
                    is_partitioned=bool(spec.is_partitioned),
                    partition_date=partition_date if spec.is_partitioned else None,
                    bucket_name=bucket_name,
                )

                # Load: Execute SQL and write to temporary paths (reuse connection)
                paths = dict_to_paths(paths_dict)
                registrars = [OdsCsvRegistrar()] if spec.layer == "ods" else []
                metrics = pipeline_load(
                    spec=spec,
                    paths=paths,
                    partition_date=partition_date if spec.is_partitioned else None,
                    store=store,
                    connection=connection,  # KEY: Reuse connection across dates
                    base_uri=f"s3://{bucket_name}",
                    registrars=registrars,
                )

                # Validate: Check output files exist
                validate_dataset(paths_dict, metrics, s3_hook)

                # Commit: Move temporary paths to canonical location
                commit_dataset(table_name, unique_run_id, paths_dict, metrics, s3_hook)

                successful_dates.append(partition_date)

                logger.info(
                    f"[Backfill] Completed {layer}.{table_name} dt={partition_date}: "
                    f"{metrics.get('row_count', 0)} rows, {metrics.get('file_count', 0)} files"
                )

            except Exception as e:
                error_info = {
                    "date": partition_date,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
                failed_dates.append(error_info)
                logger.error(
                    f"[Backfill] Failed {layer}.{table_name} dt={partition_date}: {e}",
                    exc_info=True,
                )

                if not continue_on_error:
                    # Terminate immediately on first error
                    raise RuntimeError(
                        f"Backfill terminated for {layer}.{table_name} due to error on "
                        f"dt={partition_date}. Successful: {len(successful_dates)}, "
                        f"Failed: {len(failed_dates)}"
                    ) from e
                # Otherwise, continue to next date

            finally:
                # Always cleanup temporary files (even on error)
                if paths_dict:
                    try:
                        cleanup_dataset(paths_dict, s3_hook)
                    except Exception as cleanup_err:
                        logger.warning(
                            f"[Backfill] Cleanup failed for {layer}.{table_name} "
                            f"dt={partition_date}: {cleanup_err}"
                        )

    # 5. Return aggregated results (minimize XCom size)
    result = {
        "total_dates": len(dates),
        "successful_count": len(successful_dates),
        "failed_count": len(failed_dates),
        "skipped_count": 0,
        # Only include failed dates (for debugging), omit successful dates to save XCom space
        "failed_dates": failed_dates if failed_dates else [],
        # Include first/last successful dates for verification
        "first_success": successful_dates[0] if successful_dates else None,
        "last_success": successful_dates[-1] if successful_dates else None,
    }

    logger.info(
        f"[Backfill] Summary for {layer}.{table_name}: "
        f"{len(successful_dates)} succeeded, {len(failed_dates)} failed "
        f"out of {len(dates)} total dates"
    )

    if failed_dates:
        logger.warning(
            f"[Backfill] Failed dates for {layer}.{table_name}: "
            f"{[f['date'] for f in failed_dates]}"
        )

    return result


def create_layer_backfill_dag(layer: str, config: DWConfig) -> DAG | None:
    """Create backfill DAG for a specific layer.

    Args:
        layer: Layer name (e.g., "ods", "dwd")
        config: DW configuration containing layer and table dependencies

    Returns:
        DAG instance or None if layer has no tables
    """
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
        dag_id=f"dw_{layer}_backfill",
        description=f"Backfill DAG for {layer} layer (optimized for date ranges)",
        default_args=default_args,
        schedule=None,  # Triggered by upstream
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,  # Backfill should be sequential
        tags=["dw", "backfill"],
    )

    with dag:
        table_tasks = {}

        for spec_dict in ordered_specs:
            table_name = str(spec_dict.get("table") or "")

            # Each table has ONE task that processes the entire date range
            backfill_task = PythonOperator(
                task_id=f"backfill_{table_name}",
                python_callable=backfill_table_range,
                op_kwargs={
                    "run_spec": spec_dict,
                    "start_date": "{{ dag_run.conf.get('start_date') }}",
                    "end_date": "{{ dag_run.conf.get('end_date') }}",
                    "batch_size": "{{ dag_run.conf.get('batch_size', 30) }}",
                    "continue_on_error": "{{ dag_run.conf.get('continue_on_error', False) }}",
                    "bucket_name": DEFAULT_BUCKET_NAME,
                },
            )

            table_tasks[table_name] = backfill_task

        # Set table-level dependencies (same as daily DAGs)
        for table_name, deps in dependencies.items():
            if table_name in table_tasks:
                for dep in deps:
                    if dep in table_tasks:
                        table_tasks[dep] >> table_tasks[table_name]

        # Trigger downstream layers (skip empty intermediate layers)
        next_layers = _find_next_layers_with_tables(layer, config, layers_with_tables)

        if next_layers:
            for ds in next_layers:
                trigger = TriggerDagRunOperator(
                    task_id=f"trigger_dw_{ds}_backfill",
                    trigger_dag_id=f"dw_{ds}_backfill",
                    reset_dag_run=True,
                    conf={
                        "start_date": "{{ dag_run.conf.get('start_date') }}",
                        "end_date": "{{ dag_run.conf.get('end_date') }}",
                        "batch_size": "{{ dag_run.conf.get('batch_size', 30) }}",
                        "continue_on_error": "{{ dag_run.conf.get('continue_on_error', False) }}",
                        "targets": "{{ dag_run.conf.get('targets') }}",
                        "init": "{{ dag_run.conf.get('init') }}",
                    },
                )
                for task in table_tasks.values():
                    task >> trigger
        else:
            # No downstream layers with tables, trigger finish DAG
            trigger_finish = TriggerDagRunOperator(
                task_id="trigger_dw_finish",
                trigger_dag_id="dw_finish_dag",
                reset_dag_run=True,
                conf={
                    "start_date": "{{ dag_run.conf.get('start_date') }}",
                    "end_date": "{{ dag_run.conf.get('end_date') }}",
                    "init": "{{ dag_run.conf.get('init') }}",
                },
            )
            for task in table_tasks.values():
                task >> trigger_finish

    return dag


def build_backfill_dags() -> dict[str, DAG]:
    """Build all backfill DAGs for all layers.

    Returns:
        Dictionary mapping DAG IDs to DAG instances
    """
    config = load_dw_config(CONFIG_PATH)
    dag_map = {}
    for layer in order_layers(config):
        try:
            dag = create_layer_backfill_dag(layer, config)
            if dag:
                dag_map[dag.dag_id] = dag
        except DWConfigError:
            continue
    return dag_map


# Register all backfill DAGs with Airflow
globals().update(build_backfill_dags())

