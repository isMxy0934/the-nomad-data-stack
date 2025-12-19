import json
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:

    @task(task_id="finish")
    def finish() -> None:
        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        mode = "init" if bool(conf.get("init")) else "daily"
        partition_date = str(conf.get("partition_date") or "")
        start_date = str(conf.get("start_date") or "")
        end_date = str(conf.get("end_date") or "")
        targets = conf.get("targets")
        targets_json = json.dumps(targets, ensure_ascii=True) if targets is not None else "null"
        print(
            "DW finish: "
            f"mode={mode} partition_date={partition_date} "
            f"start_date={start_date} end_date={end_date} targets={targets_json}"
        )

    finish()
