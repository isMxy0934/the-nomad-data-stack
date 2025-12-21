from __future__ import annotations

import json
from typing import Any

from prefect import flow, get_run_logger


@flow(name="dw_finish_flow")
def dw_finish_flow(run_conf: dict[str, Any] | None = None) -> None:
    conf = run_conf or {}
    mode = "init" if bool(conf.get("init")) else "daily"
    partition_date = str(conf.get("partition_date") or "")
    start_date = str(conf.get("start_date") or "")
    end_date = str(conf.get("end_date") or "")
    targets = conf.get("targets")
    targets_json = json.dumps(targets, ensure_ascii=True) if targets is not None else "null"
    logger = get_run_logger()
    logger.info(
        "DW finish: mode=%s partition_date=%s start_date=%s end_date=%s targets=%s",
        mode,
        partition_date,
        start_date,
        end_date,
        targets_json,
    )


if __name__ == "__main__":
    dw_finish_flow()
