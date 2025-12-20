"""Utilities for parsing DW config and discovering DW tables.

This module is kept as a backwards-compatible import surface for DAG code/tests.
Core logic lives in `lakehouse_core.dw_config`.
"""

from __future__ import annotations

from lakehouse_core.dw_config import (  # noqa: F401
    DWConfig,
    DWConfigError,
    SourceSpec,
    TableSpec,
    discover_tables_for_layer,
    load_dw_config,
    order_layers,
    order_tables_within_layer,
)
