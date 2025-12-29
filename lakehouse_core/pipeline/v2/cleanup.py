"""Cleanup stage with unified context interface."""

from __future__ import annotations

from lakehouse_core.domain.execution_context import PipelineExecutionContext
from lakehouse_core.pipeline import cleanup as v1_cleanup


def cleanup(ctx: PipelineExecutionContext) -> None:
    """Cleanup tmp files using unified context.

    Args:
        ctx: Pipeline execution context with paths populated

    Raises:
        ValueError: If ctx.paths is not populated
    """
    if ctx.paths is None:
        raise ValueError("ctx.paths must be populated before cleanup stage")

    v1_cleanup(store=ctx.store, paths=ctx.paths)
