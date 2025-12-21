"""Compute/execution helpers (DuckDB)."""

from lakehouse_core.compute.execution import (
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

__all__ = [
    "Executor",
    "S3ConnectionConfig",
    "configure_s3_access",
    "copy_parquet",
    "copy_partitioned_parquet",
    "create_temporary_connection",
    "execute_sql",
    "normalize_duckdb_path",
    "run_query_to_parquet",
    "run_query_to_partitioned_parquet",
    "temporary_connection",
]
