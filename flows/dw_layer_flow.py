"""Prefect flow for DW layer execution (config-driven)."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from flows.dw_finish_flow import dw_finish_flow
from flows.utils.dag_run_utils import parse_targets
from flows.utils.etl_utils import (
    cleanup_dataset,
    commit_dataset,
    get_default_bucket_name,
    prepare_dataset,
    validate_dataset,
)
from flows.utils.runtime import get_flow_run_id
from flows.utils.catalog_utils import discover_tables, refresh_catalog
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

from flows.adapters.prefect_s3_store import PrefectS3Store
from flows.utils.etl_utils import build_s3_connection_config

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "dags" / "dw_config.yaml"
SQL_BASE_DIR = REPO_ROOT / "dags"
CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", ".duckdb/catalog.duckdb"))

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


@task
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


@task
def load_table(
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


@task
def validate_task(paths_dict: dict[str, Any], metrics: Mapping[str, int]) -> dict[str, int]:
    return validate_dataset(paths_dict, metrics)


@task
def commit_task(
    dest_name: str, run_id: str, paths_dict: dict[str, Any], metrics: Mapping[str, int]
) -> dict[str, str]:
    res, _ = commit_dataset(dest_name, run_id, paths_dict, metrics)
    return res


@task
def cleanup_task(paths_dict: dict[str, Any]) -> None:
    cleanup_dataset(paths_dict)


@task
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


@flow(name="dw_layer_flow")
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

        for partition_date in date_list:
            unique_run_id = f"{run_id}_{partition_date.replace('-', '')}"
            flow_logger.info("Running table %s for dt=%s", table_name, partition_date)
            paths = prepare_dataset(
                base_prefix=str(spec_dict["base_prefix"]),
                run_id=unique_run_id,
                is_partitioned=bool(spec_dict.get("is_partitioned", True)),
                partition_date=partition_date,
                bucket_name=bucket_name,
            )
            try:
                metrics = load_table.submit(
                    paths_dict=paths,
                    run_spec=spec_dict,
                    run_conf=run_conf,
                    bucket_name=bucket_name,
                ).result()
                _ = validate_task.submit(paths_dict=paths, metrics=metrics).result()
                _ = commit_task.submit(
                    dest_name=table_name,
                    run_id=unique_run_id,
                    paths_dict=paths,
                    metrics=metrics,
                ).result()
            finally:
                cleanup_task.submit(paths_dict=paths).result()

        table_done[table_name] = mark_table_done.submit(table_name)

    potential_downstream = [ds for ds, dps in config.layer_dependencies.items() if layer in dps]
    valid_ds = [ds for ds in potential_downstream if ds in layers_with_tables]

    if layer == "ods":
        tables = discover_tables(bucket=bucket_name, base_prefix="lake/ods")
        if tables:
            refresh_catalog(
                catalog_path=CATALOG_PATH,
                base_prefix="lake/ods",
                schema="ods",
                tables=tables,
            )
        else:
            flow_logger.info("Catalog refresh skipped: no ODS tables discovered.")

    if valid_ds:
        for ds in valid_ds:
            dw_layer_flow(layer=ds, run_conf=run_conf)
    else:
        dw_finish_flow(run_conf=run_conf)


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
