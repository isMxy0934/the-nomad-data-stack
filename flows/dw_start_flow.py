from __future__ import annotations

from typing import Any

from prefect import flow, task

from flows.dw_extractor_flow import dw_extractor_flow
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


@flow(name="dw_start_flow")
def dw_start_flow(**kwargs: Any) -> None:
    conf = build_run_conf.submit().result()
    dw_extractor_flow(run_conf=conf)


if __name__ == "__main__":
    dw_start_flow()
