from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RunContext:
    """Normalized run context passed from orchestrators to planners.

    Phase 1: this is primarily a typed container; execution still happens in Airflow DAG code.
    """

    run_id: str
    partition_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    init: bool = False
    targets: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunSpec:
    """Minimal executable specification produced by a Planner.

    A RunSpec describes "what to run" without binding to a specific orchestrator.
    """

    layer: str
    table: str
    name: str
    base_prefix: str
    is_partitioned: bool = True
    partition_date: str | None = None
    sql: str | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    storage_options: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        layer = (self.layer or "").strip()
        table = (self.table or "").strip()
        name = (self.name or "").strip()
        base_prefix = (self.base_prefix or "").strip().strip("/")
        if not layer:
            raise ValueError("RunSpec.layer is required")
        if not table:
            raise ValueError("RunSpec.table is required")
        if not name:
            raise ValueError("RunSpec.name is required")
        if not base_prefix:
            raise ValueError("RunSpec.base_prefix is required")

        expected_prefix = f"lake/{layer}/{table}"
        if base_prefix != expected_prefix:
            raise ValueError(
                f"RunSpec.base_prefix must be '{expected_prefix}', got '{self.base_prefix}'"
            )

        expected_name_prefix = f"{layer}.{table}"
        if not name.startswith(expected_name_prefix):
            raise ValueError(
                f"RunSpec.name must start with '{expected_name_prefix}', got '{self.name}'"
            )
