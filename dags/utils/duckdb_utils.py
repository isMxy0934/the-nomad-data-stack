"""DuckDB helpers for temporary compute and S3/MinIO access.

This module remains as an Airflow/DAG-facing import surface. The underlying
execution semantics live in `lakehouse_core.execution`.
"""

from __future__ import annotations

from lakehouse_core.execution import (  # noqa: F401
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
