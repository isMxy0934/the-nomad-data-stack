"""Unified execution context for pipeline stages.

This module provides a unified context object that encapsulates all parameters
needed across pipeline stages, improving API consistency and reducing parameter
passing complexity.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from lakehouse_core.domain.models import RunSpec
from lakehouse_core.inputs.base import InputRegistrar
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.store.object_store import ObjectStore


@dataclass(frozen=True)
class PipelineExecutionContext:
    """Unified context for pipeline execution stages.

    This context object encapsulates all parameters needed across
    pipeline stages (prepare, load, validate, commit, cleanup).

    Benefits:
    - Consistent interface across all pipeline functions
    - Reduces parameter passing complexity
    - Makes it easy to add new context fields without breaking signatures
    - Clear separation of concerns

    Usage:
        # Create context
        ctx = PipelineExecutionContext(
            spec=spec,
            run_id=run_id,
            partition_date=partition_date,
            store=store,
            store_namespace="s3://bucket",
            base_uri="s3://bucket",
        )

        # Prepare stage populates paths
        paths = prepare(ctx)
        ctx = ctx.with_paths(paths)

        # Load stage uses connection
        ctx = ctx.with_connection(connection)
        metrics = load(ctx)
    """

    # Core identifiers
    spec: RunSpec
    run_id: str
    partition_date: str | None

    # Storage
    store: ObjectStore
    store_namespace: str  # e.g., "s3://bucket" or "bucket"
    base_uri: str  # e.g., "s3://bucket"

    # Paths (populated by prepare stage)
    paths: PartitionPaths | NonPartitionPaths | None = None

    # Compute (populated by orchestrator)
    connection: Any = None  # DuckDB connection

    # Input registration
    registrars: Sequence[InputRegistrar] = field(default_factory=tuple)

    # Execution options
    write_success_flag: bool = True
    file_format: str = "parquet"

    def with_paths(
        self, paths: PartitionPaths | NonPartitionPaths
    ) -> PipelineExecutionContext:
        """Create new context with paths populated.

        Args:
            paths: Paths object from prepare stage

        Returns:
            New context with paths set
        """
        return PipelineExecutionContext(
            spec=self.spec,
            run_id=self.run_id,
            partition_date=self.partition_date,
            store=self.store,
            store_namespace=self.store_namespace,
            base_uri=self.base_uri,
            paths=paths,
            connection=self.connection,
            registrars=self.registrars,
            write_success_flag=self.write_success_flag,
            file_format=self.file_format,
        )

    def with_connection(self, connection: Any) -> PipelineExecutionContext:
        """Create new context with connection populated.

        Args:
            connection: DuckDB connection

        Returns:
            New context with connection set
        """
        return PipelineExecutionContext(
            spec=self.spec,
            run_id=self.run_id,
            partition_date=self.partition_date,
            store=self.store,
            store_namespace=self.store_namespace,
            base_uri=self.base_uri,
            paths=self.paths,
            connection=connection,
            registrars=self.registrars,
            write_success_flag=self.write_success_flag,
            file_format=self.file_format,
        )
