from __future__ import annotations

from collections.abc import MutableMapping
from datetime import UTC, datetime


def build_manifest(
    *,
    dest: str,
    partition_date: str,
    run_id: str,
    file_count: int,
    row_count: int,
    source_prefix: str,
    target_prefix: str,
    status: str = "success",
    generated_at: str | None = None,
) -> MutableMapping[str, object]:
    if file_count < 0:
        raise ValueError("file_count cannot be negative")
    if row_count < 0:
        raise ValueError("row_count cannot be negative")

    timestamp = generated_at or datetime.now(UTC).isoformat()
    return {
        "dest": dest,
        "partition_date": partition_date,
        "run_id": run_id,
        "file_count": file_count,
        "row_count": row_count,
        "status": status,
        "source_prefix": source_prefix,
        "target_prefix": target_prefix,
        "generated_at": timestamp,
    }

