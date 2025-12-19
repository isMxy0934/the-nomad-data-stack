import json
import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context

from dags.utils.time_utils import get_partition_date_str

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
TRIGGER_DAG_ID = "dw_ods"


def _parse_targets(conf: dict[str, Any]) -> list[str] | None:
    raw = conf.get("targets")
    if raw is None or raw == "" or raw == "None" or raw == "null":
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("dag_run.conf.targets must be a JSON list of strings") from exc
    if not isinstance(raw, list):
        raise ValueError("dag_run.conf.targets must be a list of strings")
    return [str(t) for t in raw]


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
    def build_run_confs() -> list[dict[str, object]]:
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}

        start_date = str(conf.get("start_date") or "").strip()
        end_date = str(conf.get("end_date") or "").strip()
        targets = _parse_targets(conf)

        if start_date or end_date:
            if not start_date:
                raise ValueError("start_date is required when end_date is provided")
            if targets is None:
                raise ValueError("targets must be provided for backfill runs")
            if not end_date:
                end_date = get_partition_date_str()
        else:
            end_date = get_partition_date_str()
            start_date = end_date

        partition_dates = _build_date_list(start_date, end_date)
        return [{"partition_date": dt, "targets": targets} for dt in partition_dates]

    trigger_catalog = TriggerDagRunOperator(
        task_id="trigger_dw_catalog_dag",
        trigger_dag_id="dw_catalog_dag",
        reset_dag_run=True,
    )

    trigger_dw = TriggerDagRunOperator(
        task_id=f"trigger_{TRIGGER_DAG_ID}",
        trigger_dag_id=TRIGGER_DAG_ID,
        reset_dag_run=True,
    )

    run_confs = build_run_confs()
    trigger_catalog >> run_confs >> trigger_dw.expand(conf=run_confs)
