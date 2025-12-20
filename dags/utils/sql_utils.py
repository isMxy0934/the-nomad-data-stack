"""SQL template utilities for ODS and downstream tasks.

This module is kept as a backwards-compatible import surface for DAG code/tests.
Core logic lives in `lakehouse_core.sql`.
"""

from __future__ import annotations

from lakehouse_core.sql import (  # noqa: F401
    MissingTemplateVariableError,
    load_and_render_sql,
    load_sql,
    render_sql,
)
