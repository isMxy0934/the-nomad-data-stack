from __future__ import annotations

from typing import Any

from prefect import flow, task

from flows.utils.runtime import run_deployment_sync
from flows.utils.dag_run_utils import parse_targets
from lakehouse_core.io.time import get_partition_date_str


@task
def prepare_init_conf(params: dict[str, Any]) -> dict[str, Any]:
    start_date = str(params.get("start_date") or "").strip()
    end_date = str(params.get("end_date") or "").strip()
    targets = parse_targets(params)

    if not start_date:
        raise ValueError("Init run requires 'start_date'")
    if not end_date:
        end_date = start_date

    return {
        "start_date": start_date,
        "end_date": end_date,
        "targets": targets or [],
        "init": True,
    }


def _flow_run_name() -> str:
    from prefect.runtime import flow_run

    params = flow_run.parameters
    start_date = params.get("start_date") or get_partition_date_str()
    end_date = params.get("end_date") or start_date
    return f"dw-init {start_date}~{end_date}"


@flow(name="dw_init_flow", flow_run_name=_flow_run_name)
def dw_init_flow(
    start_date: str | None = None,
    end_date: str | None = None,
    targets: list[str] | None = None,
) -> None:
    params = {
        "start_date": start_date or get_partition_date_str(),
        "end_date": end_date or "",
        "targets": targets or [],
    }
    conf = prepare_init_conf.submit(params).result()
    run_deployment_sync(
        "dw_catalog_flow/dw-catalog",
        parameters={"run_conf": conf},
        flow_run_name=f"dw-catalog init {conf.get('start_date')}~{conf.get('end_date')}",
    )


if __name__ == "__main__":
    dw_init_flow()
