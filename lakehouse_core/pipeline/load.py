from __future__ import annotations

import logging
from collections.abc import Sequence

from lakehouse_core.api import materialize_query_to_tmp_and_measure
from lakehouse_core.inputs.base import InputRegistrar
from lakehouse_core.models import RunSpec
from lakehouse_core.observability import log_event
from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.sql import render_sql
from lakehouse_core.storage import ObjectStore

logger = logging.getLogger(__name__)


def load(
    *,
    spec: RunSpec,
    paths: PartitionPaths | NonPartitionPaths,
    partition_date: str | None,
    store: ObjectStore,
    connection,
    base_uri: str,
    registrars: Sequence[InputRegistrar] = (),
) -> dict[str, int]:
    """Register inputs, render SQL, materialize to tmp, and return metrics."""

    if spec.is_partitioned and not partition_date:
        raise ValueError(f"partition_date is required for partitioned spec {spec.layer}.{spec.table}")

    # Ensure idempotency on retries: clear tmp prefix before writing.
    store.delete_prefix(paths.tmp_prefix)

    for registrar in registrars:
        reg = registrar.register(
            spec=spec,
            connection=connection,
            store=store,
            base_uri=base_uri,
            partition_date=str(partition_date or ""),
        )
        if not reg.has_data:
            log_event(
                logger,
                "pipeline.load",
                layer=spec.layer,
                table=spec.table,
                dt=partition_date or "",
                status="no_data",
            )
            return {"row_count": 0, "file_count": 0, "has_data": 0}

    sql_template = str(spec.sql or "")
    variables = {"PARTITION_DATE": str(partition_date)} if partition_date else {}
    rendered_sql = render_sql(sql_template, variables) if sql_template else ""

    metrics = materialize_query_to_tmp_and_measure(
        connection=connection,
        store=store,
        query=rendered_sql,
        destination_prefix=paths.tmp_prefix,
        partitioned=spec.is_partitioned,
        tmp_partition_prefix=getattr(paths, "tmp_partition_prefix", None),
        partition_column="dt",
        filename_pattern="file_{uuid}",
        use_tmp_file=True,
    )
    log_event(
        logger,
        "pipeline.load",
        layer=spec.layer,
        table=spec.table,
        dt=partition_date or "",
        status="ok",
        file_count=int(metrics.get("file_count", 0)),
        row_count=int(metrics.get("row_count", 0)),
        has_data=int(metrics.get("has_data", 0)),
    )
    return metrics
