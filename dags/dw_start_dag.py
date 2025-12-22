import os
from datetime import datetime

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from lakehouse_core.io.time import get_partition_date_str

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:

    def build_run_conf() -> dict[str, object]:
        partition_date = get_partition_date_str()
        return {
            "partition_date": partition_date,
            "start_date": partition_date,
            "end_date": partition_date,
            "init": False,
            "targets": [],
        }

    build_conf = PythonOperator(
        task_id="build_run_conf",
        python_callable=build_run_conf,
    )

    trigger_ods = TriggerDagRunOperator(
        task_id="trigger_dw_ods_dag",
        trigger_dag_id="dw_ods",
        reset_dag_run=True,
        conf=XComArg(build_conf),
    )

    build_conf >> trigger_ods
