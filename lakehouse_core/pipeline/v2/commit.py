"""Commit stage with unified context interface."""

from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.domain.execution_context import PipelineExecutionContext
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

    # Extract dest and partition_date from ctx based on partition type
    if isinstance(ctx.paths, PartitionPaths):
        dest = ctx.paths.tmp_partition_prefix
        partition_date = ctx.paths.partition_date
    else:
        dest = ctx.paths.tmp_prefix
        partition_date = ctx.partition_date or ""

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

