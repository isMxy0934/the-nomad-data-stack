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
from lakehouse_core.catalog import attach_catalog_if_available
from lakehouse_core.errors import LakehouseCoreError, PlanningError, ValidationError
from lakehouse_core.dw_config import DWConfig, DWConfigError, SourceSpec, TableSpec, discover_tables_for_layer, load_dw_config, order_layers, order_tables_within_layer
from lakehouse_core.execution import (
    Executor,
    S3ConnectionConfig,
    configure_s3_access,
    copy_parquet,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
    normalize_duckdb_path,
    run_query_to_parquet,
    run_query_to_partitioned_parquet,
    temporary_connection,
)
from lakehouse_core.input_views import has_csv_under_prefix, register_csv_glob_as_temp_view
from lakehouse_core.manifest import build_manifest
from lakehouse_core.models import RunContext, RunSpec
from lakehouse_core.ods_sources import register_ods_csv_source_view
from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.planning import Planner
from lakehouse_core.dw_planner import DirectoryDWPlanner
from lakehouse_core.uri import parse_s3_uri
from lakehouse_core.sql import MissingTemplateVariableError, load_and_render_sql, load_sql, render_sql
from lakehouse_core.stores import Boto3S3Store, LocalFileStore

__all__ = [
    "LakehouseCoreError",
    "NonPartitionPaths",
    "Planner",
    "DirectoryDWPlanner",
    "PlanningError",
    "PartitionPaths",
    "RunContext",
    "RunSpec",
    "ValidationError",
    "Executor",
    "S3ConnectionConfig",
    "attach_catalog_if_available",
    "build_manifest",
    "cleanup_tmp",
    "materialize_query_to_tmp_and_measure",
    "configure_s3_access",
    "copy_parquet",
    "copy_partitioned_parquet",
    "create_temporary_connection",
    "execute_sql",
    "normalize_duckdb_path",
    "has_csv_under_prefix",
    "register_ods_csv_source_view",
    "Boto3S3Store",
    "LocalFileStore",
    "DWConfig",
    "DWConfigError",
    "SourceSpec",
    "TableSpec",
    "discover_tables_for_layer",
    "load_dw_config",
    "order_layers",
    "order_tables_within_layer",
    "MissingTemplateVariableError",
    "load_and_render_sql",
    "load_sql",
    "render_sql",
    "parse_s3_uri",
    "prepare_paths",
    "publish_output",
    "register_csv_glob_as_temp_view",
    "run_query_to_parquet",
    "run_query_to_partitioned_parquet",
    "temporary_connection",
    "validate_output",
]
