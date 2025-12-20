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

    name: str
    base_prefix: str
    is_partitioned: bool = True
    partition_date: str | None = None
    sql: str | None = None
    inputs: dict[str, Any] = field(default_factory=dict)
    storage_options: dict[str, Any] | None = None

