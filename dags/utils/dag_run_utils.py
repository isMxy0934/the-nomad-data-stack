"""Helpers for normalizing dag_run.conf values."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any


def parse_targets(conf: dict[str, Any] | None) -> list[str] | None:
    """Normalize dag_run.conf.targets.

    Accepts list or JSON-encoded list (with possible double-encoding).
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
            # Not JSON, keep as-is to validate below.
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
    for target in targets:
        if "*" in target:
            raise ValueError("dag_run.conf.targets does not support wildcard targets")
        if "." not in target:
            raise ValueError("dag_run.conf.targets must use layer.table format")
    return targets
