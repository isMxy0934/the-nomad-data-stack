"""Stable core layer for the-nomad-data-stack.

This package must not depend on any specific orchestrator (Airflow/Prefect) or
storage implementation (S3/local). Integrations live in adapters.
"""

from lakehouse_core.api import (
    cleanup_tmp,
    materialize_query_to_tmp_and_measure,
    prepare_paths,
    publish_output,
    validate_output,
)
from lakehouse_core.errors import LakehouseCoreError, PlanningError, ValidationError
from lakehouse_core.execution import (
    Executor,
    S3ConnectionConfig,
    configure_s3_access,
    copy_parquet,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
    run_query_to_parquet,
    run_query_to_partitioned_parquet,
    temporary_connection,
)
from lakehouse_core.manifest import build_manifest
from lakehouse_core.models import RunContext, RunSpec
from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.planning import Planner
from lakehouse_core.uri import parse_s3_uri

__all__ = [
    "LakehouseCoreError",
    "NonPartitionPaths",
    "Planner",
    "PlanningError",
    "PartitionPaths",
    "RunContext",
    "RunSpec",
    "ValidationError",
    "Executor",
    "S3ConnectionConfig",
    "build_manifest",
    "cleanup_tmp",
    "materialize_query_to_tmp_and_measure",
    "configure_s3_access",
    "copy_parquet",
    "copy_partitioned_parquet",
    "create_temporary_connection",
    "execute_sql",
    "parse_s3_uri",
    "prepare_paths",
    "publish_output",
    "run_query_to_parquet",
    "run_query_to_partitioned_parquet",
    "temporary_connection",
    "validate_output",
]
