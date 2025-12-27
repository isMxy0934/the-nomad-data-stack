"""Adapters bridging orchestrators to lakehouse_core.

This module provides adapters that convert orchestrator-specific types
(Airflow connections, contexts, hooks) to lakehouse_core types, ensuring
the core library remains orchestrator-agnostic.
"""

from dags.adapters.airflow_config import build_s3_connection_config
from dags.adapters.airflow_context import build_run_context_from_airflow
from dags.adapters.airflow_s3_store import AirflowS3Store

__all__ = [
    "AirflowS3Store",
    "build_s3_connection_config",
    "build_run_context_from_airflow",
]
