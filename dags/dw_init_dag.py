import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.utils.dag_run_utils import parse_targets
from dags.utils.time_utils import get_partition_date_str

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
    tags=["orchestration", "init"],
) as dag:

    @task
    def generate_run_confs(**context: Any) -> list[dict[str, Any]]:
        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}

        # 1. Validate inputs
        start_date = str(conf.get("start_date") or "").strip()
        end_date = str(conf.get("end_date") or "").strip()
        targets = parse_targets(conf)

        if not targets:
            raise ValueError(
                "Init run requires 'targets' (e.g., {'targets': ['fund_price_akshare']})"
            )
        if not start_date:
            raise ValueError(
                "Init run requires 'start_date' (YYYY-MM-DD)"
            )

        if not end_date:
            end_date = get_partition_date_str()

        # 2. Build configurations for each date
        partition_dates = _build_date_list(start_date, end_date)
        run_confs = []
        for dt in partition_dates:
            run_confs.append({
                "partition_date": dt,
                "start_date": start_date,
                "end_date": end_date,
                "init": True,
                "targets": targets,
            })
        
        return run_confs

    confs = generate_run_confs()

    TriggerDagRunOperator.partial(
        task_id="trigger_dw_catalog_dag",
        trigger_dag_id="dw_catalog_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    ).expand(conf=confs)
