"""Airflow context adapters for converting Airflow runtime context to lakehouse_core models.

This module isolates Airflow context format from core business logic.
"""

from __future__ import annotations

from typing import Any

from lakehouse_core.domain.models import RunContext


def build_run_context_from_airflow(context: dict[str, Any]) -> RunContext:
    """Convert Airflow task context to lakehouse_core RunContext.

    This adapter extracts relevant information from Airflow's task context
    and converts it to lakehouse_core's RunContext format, isolating
    Airflow-specific logic from the core library.

    Args:
        context: Airflow task context dictionary

    Returns:
        RunContext with normalized run information

    Example:
        >>> @task
        >>> def my_task(**context):
        >>>     run_ctx = build_run_context_from_airflow(context)
        >>>     # Use run_ctx with lakehouse_core functions
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    return RunContext(
        run_id=context.get("run_id", ""),
        partition_date=conf.get("partition_date"),
        start_date=conf.get("start_date"),
        end_date=conf.get("end_date"),
        init=bool(conf.get("init", False)),
        targets=conf.get("targets", []),
        extra=conf,
    )
