"""Commit stage with unified context interface."""

from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.domain.execution_context import PipelineExecutionContext
from lakehouse_core.io.time import get_partition_date_str
from lakehouse_core.io.paths import PartitionPaths
from lakehouse_core.pipeline import commit as v1_commit


def commit(ctx: PipelineExecutionContext, metrics: Mapping[str, int]) -> Mapping[str, str]:
    """Commit tmp output to canonical using unified context.

    Args:
        ctx: Pipeline execution context with paths populated
        metrics: Load metrics (row count, file_count, etc.)

    Returns:
        Commit result dict

    Raises:
        ValueError: If ctx.paths is not populated
    """
    if ctx.paths is None:
        raise ValueError("ctx.paths must be populated before commit stage")

    # Use logical table name as dest (for manifest tracking/auditing)
    dest = ctx.spec.table

    # Extract partition_date: from paths for partitioned tables, from ctx for non-partitioned
    if isinstance(ctx.paths, PartitionPaths):
        partition_date = ctx.paths.partition_date
    else:
        # For non-partitioned tables, use T-1 as default (consistent with v1)
        partition_date = ctx.partition_date or get_partition_date_str()

    commit_result, _ = v1_commit(
        store=ctx.store,
        paths=ctx.paths,
        dest=dest,
        run_id=ctx.run_id,
        partition_date=partition_date,
        metrics=metrics,
        write_success_flag=ctx.write_success_flag,
    )

    return commit_result

