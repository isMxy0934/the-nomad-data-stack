"""Prefect flow for DW layer execution (config-driven)."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect.utilities.annotations import allow_failure

from flows.adapters.prefect_s3_store import PrefectS3Store
from flows.utils.dag_run_utils import parse_targets
from flows.utils.etl_utils import (
    build_s3_connection_config,
    cleanup_dataset,
    commit_dataset,
    get_default_bucket_name,
    prepare_dataset,
    validate_dataset,
)
from flows.utils.runtime import get_flow_run_id, run_deployment_sync
from lakehouse_core.catalog import attach_catalog_if_available
from lakehouse_core.compute import configure_s3_access, temporary_connection
from lakehouse_core.domain.models import RunContext, RunSpec
from lakehouse_core.inputs import OdsCsvRegistrar
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.io.time import get_partition_date_str
from lakehouse_core.pipeline import load as pipeline_load
from lakehouse_core.planning import (
    DirectoryDWPlanner,
    DWConfig,
    DWConfigError,
    load_dw_config,
    order_layers,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "dags" / "dw_config.yaml"
SQL_BASE_DIR = REPO_ROOT / "dags"
CATALOG_PATH = Path(
    os.getenv("DUCKDB_CATALOG_PATH", str(REPO_ROOT / ".duckdb" / "catalog.duckdb"))
)

logger = logging.getLogger(__name__)


def _conf_targets(run_conf: Mapping[str, Any]) -> list[str] | None:
    return parse_targets(dict(run_conf))


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


def _attach_catalog_if_available(connection) -> None:  # noqa: ANN001
    attach_catalog_if_available(connection, catalog_path=CATALOG_PATH)


@task(retries=1, retry_delay_seconds=300, task_run_name="prepare-{table_name}-{partition_date}")
def prepare_task(
    table_name: str,
    base_prefix: str,
    run_id: str,
    is_partitioned: bool,
    partition_date: str,
    bucket_name: str,
) -> dict[str, Any]:
    return prepare_dataset(
        base_prefix=base_prefix,
        run_id=run_id,
        is_partitioned=is_partitioned,
        partition_date=partition_date,
        bucket_name=bucket_name,
    )


@task(retries=1, retry_delay_seconds=300, task_run_name="build-dates")
def build_date_list(run_conf: dict[str, Any]) -> list[str]:
    start_date = run_conf.get("start_date")
    end_date = run_conf.get("end_date")
    partition_date = run_conf.get("partition_date")

    dates: list[str] = []
    if start_date and end_date:
        s_dt = date.fromisoformat(str(start_date))
        e_dt = date.fromisoformat(str(end_date))
        curr = s_dt
        while curr <= e_dt:
            dates.append(curr.isoformat())
            curr += timedelta(days=1)
    else:
        dates = [str(partition_date)] if partition_date else [get_partition_date_str()]

    return sorted(set(dates))


@task(retries=1, retry_delay_seconds=300, task_run_name="load-{table_name}-{partition_date}")
def load_table(
    table_name: str,
    partition_date: str,
    paths_dict: dict[str, Any],
    run_spec: Mapping[str, Any],
    run_conf: dict[str, Any],
    bucket_name: str,
) -> dict[str, Any]:
    targets = _conf_targets(run_conf)
    if targets is not None and not any(_target_matches(run_spec, t) for t in targets):
        table_name = str(run_spec.get("table") or "")
        logger.info("Skipping table %s (not in targets).", table_name)
        return {"row_count": 0, "file_count": 0, "has_data": 0, "skipped": 1}

    partition_date = str(paths_dict.get("partition_date"))

    s3_config = build_s3_connection_config()
    store = PrefectS3Store()

    spec = RunSpec(**dict(run_spec))
    if bool(paths_dict.get("partitioned")) != bool(spec.is_partitioned):
        raise ValueError("paths_dict.partitioned does not match RunSpec.is_partitioned")

    if spec.is_partitioned:
        paths = PartitionPaths(
            partition_date=str(paths_dict["partition_date"]),
            canonical_prefix=str(paths_dict["canonical_prefix"]),
            tmp_prefix=str(paths_dict["tmp_prefix"]),
            tmp_partition_prefix=str(paths_dict["tmp_partition_prefix"]),
            manifest_path=str(paths_dict["manifest_path"]),
            success_flag_path=str(paths_dict["success_flag_path"]),
        )
    else:
        paths = NonPartitionPaths(
            canonical_prefix=str(paths_dict["canonical_prefix"]),
            tmp_prefix=str(paths_dict["tmp_prefix"]),
            manifest_path=str(paths_dict["manifest_path"]),
            success_flag_path=str(paths_dict["success_flag_path"]),
        )

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


@task(retries=1, retry_delay_seconds=300, task_run_name="validate-{table_name}-{partition_date}")
def validate_task(
    table_name: str,
    partition_date: str,
    paths_dict: dict[str, Any],
    metrics: Mapping[str, int],
) -> dict[str, int]:
    if metrics.get("skipped"):
        return dict(metrics)
    return validate_dataset(paths_dict, metrics)


@task(retries=1, retry_delay_seconds=300, task_run_name="commit-{table_name}-{partition_date}")
def commit_task(
    table_name: str,
    partition_date: str,
    dest_name: str,
    run_id: str,
    paths_dict: dict[str, Any],
    metrics: Mapping[str, int],
) -> dict[str, str]:
    if metrics.get("skipped"):
        logger.info("Skipping publish for %s (marked skipped).", dest_name)
        return {"published": "0", "action": "skipped"}
    res, _ = commit_dataset(dest_name, run_id, paths_dict, metrics)
    return res


@task(retries=1, retry_delay_seconds=300, task_run_name="cleanup-{table_name}-{partition_date}")
def cleanup_task(
    table_name: str,
    partition_date: str,
    paths_dict: dict[str, Any],
) -> None:
    cleanup_dataset(paths_dict)


@task(retries=1, retry_delay_seconds=300, task_run_name="done-{table_name}")
def mark_table_done(table_name: str) -> str:
    return table_name


def _load_layer_specs(layer: str, config: DWConfig) -> tuple[list[Mapping[str, Any]], set[str]]:
    planner = DirectoryDWPlanner(dw_config_path=CONFIG_PATH, sql_base_dir=SQL_BASE_DIR)
    specs = planner.build(RunContext(run_id="plan", extra={"allow_unrendered_sql": True}))
    layers_with_tables = {spec.layer for spec in specs}
    layer_specs = [spec for spec in specs if spec.layer == layer]
    if not layer_specs:
        return [], layers_with_tables
    ordered_specs = [spec.__dict__ for spec in layer_specs]
    return ordered_specs, layers_with_tables


def _flow_run_name() -> str:
    from prefect.runtime import flow_run

    params = flow_run.parameters
    layer = params.get("layer") or "unknown"
    conf = params.get("run_conf") or {}
    partition_date = conf.get("partition_date") or ""
    if partition_date:
        return f"dw-layer {layer} dt={partition_date}"
    return f"dw-layer {layer}"


@flow(
    name="dw_layer_flow",
    flow_run_name=_flow_run_name,
    task_runner=ConcurrentTaskRunner(max_workers=4),
)
def dw_layer_flow(layer: str, run_conf: dict[str, Any] | None = None) -> None:
    run_conf = run_conf or {}
    config = load_dw_config(CONFIG_PATH)

    try:
        ordered_specs, layers_with_tables = _load_layer_specs(layer, config)
    except DWConfigError:
        return

    if not ordered_specs:
        return

    dependencies = config.table_dependencies.get(layer, {})
    date_list = build_date_list.submit(run_conf).result()
    run_id = get_flow_run_id()
    bucket_name = get_default_bucket_name()

    table_done: dict[str, object] = {}
    flow_logger = get_run_logger()

    for spec_dict in ordered_specs:
        table_name = str(spec_dict.get("table") or "")
        table_deps = dependencies.get(table_name, [])
        for dep in table_deps:
            if dep in table_done:
                table_done[dep].result()

        table_runs = []
        table_checks = []
        for partition_date in date_list:
            unique_run_id = f"{run_id}_{partition_date.replace('-', '')}"
            flow_logger.info("Running table %s for dt=%s", table_name, partition_date)
            paths_future = prepare_task.submit(
                table_name=table_name,
                base_prefix=str(spec_dict["base_prefix"]),
                run_id=unique_run_id,
                is_partitioned=bool(spec_dict.get("is_partitioned", True)),
                partition_date=partition_date,
                bucket_name=bucket_name,
            )
            load_future = load_table.submit(
                table_name=table_name,
                partition_date=partition_date,
                paths_dict=paths_future,
                run_spec=spec_dict,
                run_conf=run_conf,
                bucket_name=bucket_name,
                wait_for=[paths_future],
            )
            validate_future = validate_task.submit(
                table_name=table_name,
                partition_date=partition_date,
                paths_dict=paths_future,
                metrics=load_future,
                wait_for=[load_future],
            )
            commit_future = commit_task.submit(
                table_name=table_name,
                partition_date=partition_date,
                dest_name=table_name,
                run_id=unique_run_id,
                paths_dict=paths_future,
                metrics=load_future,
                wait_for=[validate_future],
            )
            cleanup_future = cleanup_task.submit(
                table_name=table_name,
                partition_date=partition_date,
                paths_dict=paths_future,
                wait_for=[allow_failure(commit_future)],
            )
            table_runs.append(cleanup_future)
            table_checks.append(commit_future)

        for check in table_checks:
            check.result()

        table_done[table_name] = mark_table_done.submit(table_name, wait_for=table_runs)

    for done_future in table_done.values():
        done_future.result()

    potential_downstream = [ds for ds, dps in config.layer_dependencies.items() if layer in dps]
    valid_ds = [ds for ds in potential_downstream if ds in layers_with_tables]

    if valid_ds:
        for ds in valid_ds:
            run_deployment_sync(
                f"dw_layer_flow/dw-layer-{ds}",
                parameters={"layer": ds, "run_conf": run_conf},
                flow_run_name=f"dw-layer {ds} dt={run_conf.get('partition_date') if run_conf else ''}",
            )
    else:
        run_deployment_sync(
            "dw_finish_flow/dw-finish",
            parameters={"run_conf": run_conf},
            flow_run_name=f"dw-finish dt={run_conf.get('partition_date') if run_conf else ''}",
        )


def build_dw_flows() -> dict[str, Any]:
    config = load_dw_config(CONFIG_PATH)
    flow_map: dict[str, Any] = {}
    for layer in order_layers(config):
        try:
            ordered_specs, _ = _load_layer_specs(layer, config)
            if ordered_specs:
                flow_map[f"dw_{layer}_flow"] = dw_layer_flow
        except DWConfigError:
            continue
    return flow_map
