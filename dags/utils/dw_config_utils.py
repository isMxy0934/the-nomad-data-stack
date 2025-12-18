"""Utilities for parsing DW config and discovering DW tables."""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DWConfig:
    """Resolved DW configuration including layer and table dependencies."""

    layer_dependencies: dict[str, list[str]]
    table_dependencies: dict[str, dict[str, list[str]]]


@dataclass(frozen=True)
class TableSpec:
    """Represents a DW table discovered from SQL files."""

    layer: str
    name: str
    sql_path: Path
    is_partitioned: bool = True


class DWConfigError(ValueError):
    """Raised when DW configuration is invalid."""


def _validate_layer_dependencies(layer_dependencies: Mapping[str, Sequence[str]]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for layer, deps in layer_dependencies.items():
        if not isinstance(deps, Sequence):
            raise DWConfigError("layer dependencies must be a sequence")
        normalized[layer] = [str(dep) for dep in deps]
    return normalized


def _validate_table_dependencies(
    table_dependencies: Mapping[str, Mapping[str, Sequence[str]]],
) -> dict[str, dict[str, list[str]]]:
    normalized: dict[str, dict[str, list[str]]] = {}
    for layer, deps in table_dependencies.items():
        if not isinstance(deps, Mapping):
            raise DWConfigError("table_dependencies entries must be mappings")
        normalized[layer] = {table: [str(d) for d in depends_on] for table, depends_on in deps.items()}
    return normalized


def _toposort(nodes: set[str], edges: Mapping[str, list[str]]) -> list[str]:
    in_degree = {node: 0 for node in nodes}
    for node, deps in edges.items():
        for dep in deps:
            if dep not in nodes:
                raise DWConfigError(f"Unknown dependency '{dep}' referenced by '{node}'")
            in_degree[node] += 1

    queue: deque[str] = deque([node for node, degree in in_degree.items() if degree == 0])
    ordered: list[str] = []
    while queue:
        node = queue.popleft()
        ordered.append(node)
        for other, deps in edges.items():
            if node in deps:
                in_degree[other] -= 1
                if in_degree[other] == 0:
                    queue.append(other)

    if len(ordered) != len(nodes):
        raise DWConfigError("Cycle detected in dependencies")
    return ordered


def load_dw_config(path: Path) -> DWConfig:
    """Load DW configuration from YAML with validation."""

    if not path.exists():
        raise FileNotFoundError(f"DW config not found: {path}")

    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    layer_dependencies_raw = config.get("layer_dependencies") or {}
    table_dependencies_raw = config.get("table_dependencies") or {}

    if not isinstance(layer_dependencies_raw, Mapping):
        raise DWConfigError("layer_dependencies must be a mapping")
    if not isinstance(table_dependencies_raw, Mapping):
        raise DWConfigError("table_dependencies must be a mapping")

    layer_dependencies = _validate_layer_dependencies(layer_dependencies_raw)
    table_dependencies = _validate_table_dependencies(table_dependencies_raw)

    layer_nodes = set(layer_dependencies.keys())
    for deps in layer_dependencies.values():
        layer_nodes.update(deps)
    _toposort(layer_nodes, layer_dependencies)

    for layer, deps in table_dependencies.items():
        if layer not in layer_dependencies:
            raise DWConfigError(f"table_dependencies references unknown layer '{layer}'")
        table_nodes = set(deps.keys())
        for dependency_list in deps.values():
            table_nodes.update(dependency_list)
        _toposort(table_nodes, deps)

    return DWConfig(layer_dependencies=layer_dependencies, table_dependencies=table_dependencies)


def order_layers(config: DWConfig) -> list[str]:
    """Return layers in topological order based on dependencies."""

    layer_nodes = set(config.layer_dependencies.keys())
    for deps in config.layer_dependencies.values():
        layer_nodes.update(deps)
    return _toposort(layer_nodes, config.layer_dependencies)


def discover_tables_for_layer(layer: str, base_dir: Path) -> list[TableSpec]:
    """Discover SQL tables for a DW layer by scanning the directory."""

    layer_dir = base_dir / layer
    if not layer_dir.exists() or not layer_dir.is_dir():
        return []

    sql_files = sorted(layer_dir.glob("*.sql"))
    tables: list[TableSpec] = []
    for sql_path in sql_files:
        name = sql_path.stem
        if not name.startswith(f"{layer}_"):
            logger.warning(
                "Skipping SQL without layer prefix: layer=%s file=%s",
                layer,
                sql_path.name,
            )
            continue
        sql_text = sql_path.read_text(encoding="utf-8")
        is_partitioned = "${PARTITION_DATE}" in sql_text
        tables.append(
            TableSpec(layer=layer, name=name, sql_path=sql_path, is_partitioned=is_partitioned),
        )
    return tables


def order_tables_within_layer(tables: list[TableSpec], dependencies: Mapping[str, list[str]]) -> list[TableSpec]:
    """Topologically sort tables within a layer according to dependencies."""

    if not tables:
        return []

    table_names = {table.name for table in tables}
    for table, deps in dependencies.items():
        if table not in table_names:
            raise DWConfigError(f"Dependency declared for unknown table '{table}'")
        for dep in deps:
            if dep not in table_names:
                raise DWConfigError(f"Table '{table}' depends on unknown table '{dep}'")

    ordered_names = _toposort(table_names, dependencies)
    table_by_name = {table.name: table for table in tables}
    return [table_by_name[name] for name in ordered_names]
