import os
from datetime import datetime

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag
from airflow.decorators import task

from dags.utils.time_utils import get_partition_date_str

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:

    @task
    def trigger_daily_run() -> None:
        partition_date = get_partition_date_str()
        conf = {
            "partition_date": partition_date,
            "start_date": partition_date,
            "end_date": partition_date,
            "init": False,
            "targets": [],
        }
        run_id = f"dw_start__extractor__{partition_date}"
        trigger_dag(
            dag_id="dw_extractor_dag",
            run_id=run_id,
            conf=conf,
            replace_microseconds=False,
        )

    trigger_daily_run()
