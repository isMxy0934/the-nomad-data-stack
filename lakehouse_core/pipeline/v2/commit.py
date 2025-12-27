"""Commit stage with unified context interface."""

from __future__ import annotations

from collections.abc import Mapping

from lakehouse_core.domain.execution_context import PipelineExecutionContext
from lakehouse_core.pipeline import commit as v1_commit


def commit(ctx: PipelineExecutionContext, manifest: Mapping[str, object]) -> Mapping[str, str]:
    """Commit tmp output to canonical using unified context.

    Args:
        ctx: Pipeline execution context with paths populated
        manifest: Manifest metadata

    Returns:
        Commit result dict

    Raises:
        ValueError: If ctx.paths is not populated
    """
    if ctx.paths is None:
        raise ValueError("ctx.paths must be populated before commit stage")

    return v1_commit(
        store=ctx.store,
        paths=ctx.paths,
        manifest=manifest,
        write_success_flag=ctx.write_success_flag,
    )
