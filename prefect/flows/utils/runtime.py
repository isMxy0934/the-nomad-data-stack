"""Prefect runtime helpers."""

from __future__ import annotations

import inspect
from uuid import uuid4


def run_deployment_sync(
    name: str,
    *,
    parameters: dict[str, object] | None = None,
    flow_run_name: str | None = None,
) -> object:
    from prefect.utilities.asyncutils import run_coro_as_sync

    from prefect.deployments import run_deployment

    result = run_deployment(
        name=name,
        parameters=parameters or {},
        flow_run_name=flow_run_name,
    )
    if inspect.iscoroutine(result):
        return run_coro_as_sync(result)
    return result


def get_flow_run_id() -> str:
    try:
        from prefect.runtime import flow_run

        run_id = getattr(flow_run, "id", None)
        if run_id:
            return str(run_id)
    except Exception:
        pass
    return uuid4().hex
