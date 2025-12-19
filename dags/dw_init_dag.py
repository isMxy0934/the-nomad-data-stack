import os
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.utils.dag_run_utils import parse_targets
from dags.utils.time_utils import get_partition_date_str

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration", "init"],
    params={
        "start_date": Param(
            default=get_partition_date_str(),
            type="string",
            format="date",
            description="Start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default=get_partition_date_str(),
            type="string",
            format="date",
            description="End date (YYYY-MM-DD). Defaults to start_date if empty.",
        ),
        "targets": Param(
            default=[],
            type=["array", "null"],
            description="List of targets (e.g. ['ods.fund_price_akshare']). Leave empty for ALL targets.",
        ),
    },
) as dag:

    @task
    def prepare_init_conf(**context: Any) -> dict[str, Any]:
        params = context.get("params") or {}
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
            "partition_date": end_date,
            "targets": targets or [],
            "init": True
        }

    init_conf = prepare_init_conf()

    trigger_catalog = TriggerDagRunOperator(
        task_id="trigger_dw_catalog_dag",
        trigger_dag_id="dw_catalog_dag",
        reset_dag_run=True,
        wait_for_completion=False,
        conf=init_conf,
    )
