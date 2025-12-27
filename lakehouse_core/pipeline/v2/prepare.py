"""Prepare stage with unified context interface."""

from __future__ import annotations

from typing import Any

from lakehouse_core.api import prepare_paths
from lakehouse_core.domain.execution_context import PipelineExecutionContext
from lakehouse_core.io.paths import paths_to_dict


def prepare(ctx: PipelineExecutionContext) -> dict[str, Any]:
    """Prepare canonical/tmp paths using unified context.

    Args:
        ctx: Pipeline execution context

    Returns:
        JSON-serializable paths dict for XCom

    Example:
        >>> ctx = PipelineExecutionContext(
        ...     spec=spec,
        ...     run_id="run_123",
        ...     partition_date="2024-01-15",
        ...     store=store,
        ...     store_namespace="s3://bucket",
        ...     base_uri="s3://bucket",
        ... )
        >>> paths_dict = prepare(ctx)
        >>> # Use paths_dict in orchestrator (e.g., Airflow XCom)
    """
    # Determine effective partition date for paths and XCom
    # For non-partitioned tables, use T-1 as default (consistent with v1 prepare_dataset)
    if ctx.spec.is_partitioned:
        effective_dt = ctx.partition_date
    else:
        # Non-partitioned tables also need a partition_date for manifest consistency
        # Use T-1 default if not provided (same as v1 prepare_dataset behavior)
        from lakehouse_core.io.time import get_partition_date_str

        effective_dt = ctx.partition_date or get_partition_date_str()

    paths = prepare_paths(
        base_prefix=ctx.spec.base_prefix,
        run_id=ctx.run_id,
        partition_date=effective_dt,
        is_partitioned=ctx.spec.is_partitioned,
        store_namespace=ctx.store_namespace,
    )
    # Use effective_dt for both paths and XCom to ensure consistency
    return dict(paths_to_dict(paths, partition_date=effective_dt))
