"""Prefect runtime helpers."""

from __future__ import annotations

from uuid import uuid4


def get_flow_run_id() -> str:
    try:
        from prefect.runtime import flow_run

        run_id = getattr(flow_run, "id", None)
        if run_id:
            return str(run_id)
    except Exception:
        pass
    return uuid4().hex
