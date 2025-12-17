import logging
import os
from datetime import datetime

import tushare as ts
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from dags.utils.s3_utils import S3Uploader
from dags.utils.time_utils import get_previous_date_str, get_previous_partition_date_str

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def fetch_fund_data_from_tushare():
    target_date_str = get_previous_date_str()
    target_partition_date_str = get_previous_partition_date_str()

    logging.info(f"Start fetching fund daily data from Tushare: {target_date_str}")

    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()

    try:
        df = pro.fund_daily(trade_date=target_date_str)
    except Exception as e:
        logging.error(f"Tushare API call failed: {e}")
        raise e

    if df.empty:
        logging.warning(f"{target_date_str} has no fund data (maybe it's a holiday or weekend)")
        return None

    s3_uploader = S3Uploader()
    file_key = f"lake/raw/daily/fund_price/tushare/dt={target_partition_date_str}/data.csv"

    try:
        s3_uploader.upload_bytes(df.to_csv(index=False).encode("utf-8"), file_key, replace=True)
        logging.info(f"Uploaded fund daily data to S3: {file_key}, total {len(df)} records")
    except Exception as e:
        logging.error(f"Failed to upload to S3: {e}")
        raise e

    return file_key


dag = DAG(
    dag_id=DAG_ID,
    schedule="45 18 * * *",
    start_date=datetime(2025, 12, 13),
    catchup=False,
    tags=["extractor", "tushare", "fund"],
)

fetch_fund_task = PythonOperator(
    task_id="fetch_fund_data_from_tushare",
    python_callable=fetch_fund_data_from_tushare,
    dag=dag,
)
