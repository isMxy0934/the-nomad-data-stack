"""Pipeline v2 with unified execution context interface.

This module provides a new pipeline API that uses PipelineExecutionContext
for consistent parameter passing across all stages.

Benefits over v1:
- Unified interface: all functions accept PipelineExecutionContext
- Easier to extend: add new context fields without breaking signatures
- Better type safety: context object is strongly typed
- Clearer dependencies: context shows what each stage needs

Usage:
    from lakehouse_core.domain.execution_context import PipelineExecutionContext
    from lakehouse_core.pipeline import v2

    # Create context
    ctx = PipelineExecutionContext(
        spec=spec,
        run_id=run_id,
        partition_date=partition_date,
        store=store,
        store_namespace="s3://bucket",
        base_uri="s3://bucket",
    )

    # Run pipeline stages
    paths_dict = v2.prepare(ctx)
    ctx = ctx.with_paths(dict_to_paths(paths_dict))
    ctx = ctx.with_connection(connection)

    metrics = v2.load(ctx)
    v2.validate(ctx, metrics)
    v2.commit(ctx, metrics)
    v2.cleanup(ctx)

Note: v1 pipeline remains available for backward compatibility.
"""

from lakehouse_core.pipeline.v2.cleanup import cleanup
from lakehouse_core.pipeline.v2.commit import commit
from lakehouse_core.pipeline.v2.load import load
from lakehouse_core.pipeline.v2.prepare import prepare
from lakehouse_core.pipeline.v2.validate import validate

__all__ = ["cleanup", "commit", "load", "prepare", "validate"]
