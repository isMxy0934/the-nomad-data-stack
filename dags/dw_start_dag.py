import os
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
Trigger_DAG_ID = "dw_catalog_dag"
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:
    trigger_catalog = TriggerDagRunOperator(
        task_id=f"trigger_{Trigger_DAG_ID}",
        trigger_dag_id=Trigger_DAG_ID,
        reset_dag_run=True,
    )
