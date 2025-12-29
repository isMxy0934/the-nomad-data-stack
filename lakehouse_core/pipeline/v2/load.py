"""Load stage with unified context interface."""

from __future__ import annotations

import logging

from lakehouse_core.api import materialize_query_to_tmp_and_measure
from lakehouse_core.domain.execution_context import PipelineExecutionContext
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.sql import render_sql

logger = logging.getLogger(__name__)


def load(ctx: PipelineExecutionContext) -> dict[str, int]:
    """Load data using unified context.

    Args:
        ctx: Pipeline execution context with paths and connection populated

    Returns:
        Load metrics dict (row_count, file_count, has_data)

    Raises:
        ValueError: If ctx.paths or ctx.connection is not populated
    """
    if ctx.paths is None:
        raise ValueError("ctx.paths must be populated before load stage")
    if ctx.connection is None:
        raise ValueError("ctx.connection must be populated before load stage")
    if ctx.spec.is_partitioned and not ctx.partition_date:
        raise ValueError(
            f"partition_date is required for partitioned spec {ctx.spec.layer}.{ctx.spec.table}"
        )

    # Ensure idempotency on retries
    ctx.store.delete_prefix(ctx.paths.tmp_prefix)

    # Register inputs
    for registrar in ctx.registrars:
        reg = registrar.register(
            spec=ctx.spec,
            connection=ctx.connection,
            store=ctx.store,
            base_uri=ctx.base_uri,
            partition_date=str(ctx.partition_date or ""),
        )
        if not reg.has_data:
            log_event(
                logger,
                "pipeline.v2.load",
                layer=ctx.spec.layer,
                table=ctx.spec.table,
                dt=ctx.partition_date or "",
                status="no_data",
            )
            return {"row_count": 0, "file_count": 0, "has_data": 0}

    # Render SQL
    sql_template = str(ctx.spec.sql or "")
    variables = {"PARTITION_DATE": str(ctx.partition_date)} if ctx.partition_date else {}
    rendered_sql = render_sql(sql_template, variables) if sql_template else ""

    # Materialize
    metrics = materialize_query_to_tmp_and_measure(
        connection=ctx.connection,
        store=ctx.store,
        query=rendered_sql,
        destination_prefix=ctx.paths.tmp_prefix,
        partitioned=ctx.spec.is_partitioned,
        tmp_partition_prefix=getattr(ctx.paths, "tmp_partition_prefix", None),
        partition_column="dt",
        filename_pattern="file_{uuid}",
        use_tmp_file=True,
    )

    log_event(
        logger,
        "pipeline.v2.load",
        layer=ctx.spec.layer,
        table=ctx.spec.table,
        dt=ctx.partition_date or "",
        status="ok",
        file_count=int(metrics.get("file_count", 0)),
        row_count=int(metrics.get("row_count", 0)),
        has_data=int(metrics.get("has_data", 0)),
    )
    return metrics
