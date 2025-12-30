"""Helpers for normalizing dag_run.conf values."""

from __future__ import annotations

import ast
import json
from collections.abc import Sequence
from typing import Any


def parse_bool_param(value: Any, default: bool = False) -> bool:
    """Parse boolean parameter from DAG run conf (handles Jinja string rendering).

    Args:
        value: Value from dag_run.conf (may be bool, string, or other)
        default: Default value if parsing fails

    Returns:
        Boolean value

    Examples:
        >>> parse_bool_param(True)
        True
        >>> parse_bool_param("True")
        True
        >>> parse_bool_param("False")
        False
        >>> parse_bool_param("false")
        False
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return default


def parse_int_param(value: Any, default: int = 0) -> int:
    """Parse integer parameter from DAG run conf (handles Jinja string rendering).

    Args:
        value: Value from dag_run.conf (may be int, string, or other)
        default: Default value if parsing fails

    Returns:
        Integer value

    Examples:
        >>> parse_int_param(30)
        30
        >>> parse_int_param("30")
        30
        >>> parse_int_param("invalid", 10)
        10
    """
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def parse_targets(conf: dict[str, Any] | None) -> list[str] | None:
    """Normalize dag_run.conf.targets.

    Accepts list or JSON-encoded list (with possible double-encoding).
    Supports two formats:
    - "layer.table" (e.g., "ods.fund_etf_spot")
    - "layer" (e.g., "dwd" to process entire layer)

    Returns None when targets is absent/empty.
    """

    if not conf:
        return None
    raw: Any = conf.get("targets")
    if raw is None:
        return None

    # Unwrap JSON-encoded strings (including accidental double-encoding).
    for _ in range(3):
        if not isinstance(raw, str):
            break
        text = raw.strip()
        if text in {"", "null", "None", "[]"}:
            return None
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            # Try Python literal list string (e.g. "['a', 'b']").
            try:
                raw = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                break

    if raw is None:
        return None
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("dag_run.conf.targets must be a list of strings")

    targets = [str(t).strip() for t in raw if str(t).strip()]
    if not targets:
        return None

    # Validate each target (allow both "layer" and "layer.table" formats)
    for target in targets:
        if "*" in target:
            raise ValueError("dag_run.conf.targets does not support wildcard targets")
        # Allow layer-only format (e.g., "dwd") or layer.table format (e.g., "dwd.table_name")
        if "." in target:
            parts = target.split(".", 1)
            if not parts[0] or not parts[1]:
                raise ValueError(f"Invalid target format: '{target}'. Use 'layer' or 'layer.table'")

    return targets


def build_downstream_conf(conf: dict[str, Any] | None) -> dict[str, object]:
    """Build a normalized conf payload for downstream DAGs."""

    conf = conf or {}
    targets = parse_targets(conf) or []
    return {
        "partition_date": conf.get("partition_date"),
        "start_date": conf.get("start_date"),
        "end_date": conf.get("end_date"),
        "init": conf.get("init"),
        "targets": targets,
    }
