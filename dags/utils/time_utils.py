"""Airflow-facing time helpers.

The project-wide T-1 date convention lives in `lakehouse_core.time`.
This module is kept for backwards-compatible imports in DAG code.
"""

from __future__ import annotations

from lakehouse_core.time import (
    get_current_date_str,
    get_current_partition_date_str,
    get_date_str,
    get_partition_date_str,
    get_previous_date_str,
    get_previous_partition_date_str,
)

__all__ = [
    "get_current_date_str",
    "get_current_partition_date_str",
    "get_date_str",
    "get_partition_date_str",
    "get_previous_date_str",
    "get_previous_partition_date_str",
]
