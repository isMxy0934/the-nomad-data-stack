from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any


def _kv_pairs(fields: Mapping[str, object]) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        parts.append(f"{key}={text}")
    return " ".join(parts)


def log_event(logger: logging.Logger, message: str, **fields: object) -> None:
    """Emit a stable, Airflow-friendly structured log line.

    Airflow task logs are plain text, so we include key fields as ``k=v`` tokens.
    """

    suffix = _kv_pairs(fields)
    if suffix:
        logger.info("%s %s", message, suffix)
    else:
        logger.info("%s", message)


def manifest_log_fields(manifest: Mapping[str, object]) -> dict[str, object]:
    """Extract standard fields from manifest-like mappings."""

    def _get(name: str) -> object:
        return manifest.get(name)

    return {
        "dest": _get("dest"),
        "dt": _get("partition_date"),
        "run_id": _get("run_id"),
        "file_count": _get("file_count"),
        "row_count": _get("row_count"),
        "status": _get("status"),
    }

