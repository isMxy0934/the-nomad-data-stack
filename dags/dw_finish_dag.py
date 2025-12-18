import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["orchestration"],
) as dag:
    finish = EmptyOperator(task_id="finish")
