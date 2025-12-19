import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.utils.time_utils import get_partition_date_str
from dags.utils.dag_run_utils import parse_targets

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


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

    def trigger_init_runs(**context: Any) -> int:
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}

        start_date = str(conf.get("start_date") or "").strip()
        end_date = str(conf.get("end_date") or "").strip()
        targets = parse_targets(conf)
        if not targets:
            raise ValueError("init run requires targets")

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
            TriggerDagRunOperator(
                task_id=f"trigger_dw_extractor_dag_{dt}",
                trigger_dag_id="dw_extractor_dag",
                reset_dag_run=True,
                conf=run_conf,
            ).execute(context={})
            triggered += 1
        return triggered

    PythonOperator(
        task_id="trigger_dw_extractor_runs",
        python_callable=trigger_init_runs,
    )
