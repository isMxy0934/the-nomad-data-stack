from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from lakehouse_core.dw_config import (
    DWConfig,
    TableSpec,
    discover_tables_for_layer,
    load_dw_config,
    order_layers,
    order_tables_within_layer,
)
from lakehouse_core.models import RunContext, RunSpec
from lakehouse_core.planning import Planner
from lakehouse_core.sql import load_sql, render_sql


def _iter_dates_inclusive(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")
    days: list[str] = []
    current = start
    while current <= end:
        days.append(current.isoformat())
        current += timedelta(days=1)
    return days


def _context_partition_dates(context: RunContext) -> list[str | None]:
    if context.start_date and context.end_date:
        return _iter_dates_inclusive(context.start_date, context.end_date)
    if context.partition_date:
        return [context.partition_date]
    return [None]


def _target_matches(layer: str, table_name: str, target: str) -> bool:
    value = (target or "").strip()
    if not value:
        return False
    if "." not in value:
        return value == layer
    t_layer, t_name = value.split(".", 1)
    return t_layer == layer and t_name == table_name


def _render_table_sql(table: TableSpec, partition_date: str | None) -> str:
    raw = load_sql(table.sql_path)
    needs_partition_date = "${PARTITION_DATE}" in raw
    if needs_partition_date and not partition_date:
        raise ValueError(f"Missing partition_date for partitioned SQL: {table.sql_path}")
    variables = {"PARTITION_DATE": partition_date} if partition_date else {}
    return render_sql(raw, variables)


@dataclass(frozen=True)
class DirectoryDWPlanner(Planner):
    """Default planner: "directory is config".

    It reads `dw_config.yaml` and scans `<sql_base_dir>/<layer>/*.sql`, then produces
    ordered `RunSpec` items that can be mapped to Airflow/Prefect/scripts.
    """

    dw_config_path: Path
    sql_base_dir: Path

    def _load_config(self) -> DWConfig:
        return load_dw_config(self.dw_config_path)

    def build(self, context: RunContext) -> list[RunSpec]:
        config = self._load_config()
        layers = order_layers(config)
        targets = [t.strip() for t in context.targets if str(t).strip()]

        partition_dates = _context_partition_dates(context)
        multi_date = len([d for d in partition_dates if d is not None]) > 1
        allow_unrendered = bool(context.extra.get("allow_unrendered_sql"))

        run_specs: list[RunSpec] = []
        for layer in layers:
            tables = discover_tables_for_layer(layer, self.sql_base_dir)
            if not tables:
                continue

            dependencies = config.table_dependencies.get(layer, {})
            ordered = order_tables_within_layer(tables, dependencies)
            for table in ordered:
                if targets and not any(_target_matches(layer, table.name, t) for t in targets):
                    continue

                if layer == "ods":
                    source = config.sources.get(table.name)
                    if not source:
                        raise ValueError(
                            f"Missing sources entry for ODS table '{table.name}'. "
                            f"Add sources.{table.name} to {self.dw_config_path}."
                        )
                    if source.format.lower() != "csv":
                        raise ValueError(
                            f"Unsupported ODS source format for '{table.name}': '{source.format}'"
                        )
                    ods_inputs = {"source": {"path": source.path, "format": source.format}}
                else:
                    ods_inputs = {}

                for partition_date in partition_dates:
                    if table.is_partitioned and not partition_date and not allow_unrendered:
                        raise ValueError(
                            f"partition_date is required for partitioned table {layer}.{table.name}"
                        )
                    base_name = f"{layer}.{table.name}"
                    name = f"{base_name}:{partition_date}" if multi_date and partition_date else base_name
                    if table.is_partitioned and not partition_date and allow_unrendered:
                        sql = load_sql(table.sql_path)
                    else:
                        sql = _render_table_sql(table, partition_date if table.is_partitioned else None)
                    run_specs.append(
                        RunSpec(
                            layer=layer,
                            table=table.name,
                            name=name,
                            base_prefix=f"lake/{layer}/{table.name}",
                            is_partitioned=table.is_partitioned,
                            partition_date=partition_date if table.is_partitioned else None,
                            sql=sql,
                            inputs=ods_inputs,
                            storage_options=context.extra.get("storage_options"),
                        )
                    )

        return run_specs
