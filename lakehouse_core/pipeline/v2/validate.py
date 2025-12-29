"""Validate stage with unified context interface."""

from __future__ import annotations

from lakehouse_core.domain.execution_context import PipelineExecutionContext
from lakehouse_core.pipeline import validate as v1_validate


def validate(ctx: PipelineExecutionContext, metrics: dict[str, int]) -> dict[str, int]:
    """Validate tmp output using unified context.

    Args:
        ctx: Pipeline execution context with paths populated
        metrics: Metrics from load stage

    Returns:
        Validation result dict

    Raises:
        ValueError: If ctx.paths is not populated
    """
    if ctx.paths is None:
        raise ValueError("ctx.paths must be populated before validate stage")

    return v1_validate(
        store=ctx.store,
        paths=ctx.paths,
        metrics=metrics,
        file_format=ctx.file_format,
    )
