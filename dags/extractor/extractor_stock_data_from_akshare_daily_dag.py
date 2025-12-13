import io
import os
import time
import logging
import pandas as pd
import tushare as ts
from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from utils.s3_utils import S3Uploader
from utils.time_utils import get_previous_date_str, get_previous_partition_date_str

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


def fetch_stock_data_from_akshare():
    """
    fetch market snapshot data from Akshare
    """
    
    target_date_str = get_previous_date_str()
    target_partition_date_str = get_previous_partition_date_str()
    
    logging.info(f"start fetching market snapshot data from Akshare: {target_date_str}")
    
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    
    df = pro.daily(trade_date=target_date_str)
    
    if df.empty:
        logging.warning(f"{target_date_str} has no data (maybe it's a holiday)")
        return None
    
    s3_uploader = S3Uploader(S3_BUCKET_NAME)
    file_key = f"akshare/stock_daily/{target_partition_date_str}.csv"
    
    s3_uploader.upload_bytes(df.to_csv(index=False).encode('utf-8'), file_key, replace=True)
    
    logging.info(f"uploaded market snapshot data to S3: {file_key}, total {len(df)} records")
    return file_key

dag = DAG(
    dag_id=DAG_ID,
    schedule="30 18 * * *",
    start_date=datetime(2025, 12, 13),
    catchup=False,
    tags=['extractor', 'tushare'],
)

fetch_market_snapshot = PythonOperator(
    task_id='fetch_stock_data_from_akshare',
    python_callable=fetch_stock_data_from_akshare,
    dag=dag,
)