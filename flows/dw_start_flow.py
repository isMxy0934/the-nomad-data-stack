from __future__ import annotations

from typing import Any

from prefect import flow, task

from flows.utils.runtime import run_deployment_sync
from lakehouse_core.io.time import get_partition_date_str


@task
def build_run_conf() -> dict[str, object]:
    partition_date = get_partition_date_str()
    return {
        "partition_date": partition_date,
        "start_date": partition_date,
        "end_date": partition_date,
        "init": False,
        "targets": [],
    }


def _flow_run_name() -> str:
    partition_date = get_partition_date_str()
    return f"dw-start dt={partition_date}"


@flow(name="dw_start_flow", flow_run_name=_flow_run_name)
def dw_start_flow(**kwargs: Any) -> None:
    conf = build_run_conf.submit().result()
    run_deployment_sync(
        "dw_extractor_flow/dw-extractor",
        parameters={"run_conf": conf},
        flow_run_name=f"dw-extractor dt={conf.get('partition_date')}",
    )


if __name__ == "__main__":
    dw_start_flow()
