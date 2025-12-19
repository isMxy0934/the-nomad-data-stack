import json
import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.decorators import task
from airflow.operators.python import get_current_context

from dags.utils.time_utils import get_partition_date_str

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def _parse_targets(conf: dict[str, Any]) -> list[str]:
    raw = conf.get("targets")
    if raw is None or raw == "" or raw == "None" or raw == "null":
        raise ValueError("init run requires targets")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("dag_run.conf.targets must be a JSON list of strings") from exc
    if not isinstance(raw, list):
        raise ValueError("dag_run.conf.targets must be a list of strings")
    targets: list[str] = []
    for target in raw:
        value = str(target).strip()
        if not value:
            continue
        if "*" in value:
            raise ValueError("dag_run.conf.targets does not support wildcard targets")
        if "." not in value:
            raise ValueError("dag_run.conf.targets must use layer.table format")
        targets.append(value)
    if not targets:
        raise ValueError("init run requires targets")
    return targets


def _build_date_list(start: str, end: str) -> list[str]:
    start_dt = date.fromisoformat(start)
    end_dt = date.fromisoformat(end)
    if end_dt < start_dt:
        raise ValueError("end_date must be >= start_date")
    dates: list[str] = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:

    @task
    def trigger_init_runs() -> int:
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}

        start_date = str(conf.get("start_date") or "").strip()
        end_date = str(conf.get("end_date") or "").strip()
        targets = _parse_targets(conf)

        if not start_date:
            raise ValueError("init run requires start_date")
        if not end_date:
            end_date = get_partition_date_str()

        partition_dates = _build_date_list(start_date, end_date)
        triggered = 0
        for dt in partition_dates:
            run_conf = {
                "partition_date": dt,
                "start_date": start_date,
                "end_date": end_date,
                "init": True,
                "targets": targets,
            }
            run_id = f"dw_init__extractor__{dt}"
            trigger_dag(
                dag_id="dw_extractor_dag",
                run_id=run_id,
                conf=run_conf,
                replace_microseconds=False,
            )
            triggered += 1
        return triggered

    trigger_init_runs()
