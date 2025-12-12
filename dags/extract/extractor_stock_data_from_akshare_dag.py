import os
import logging
import tempfile
import akshare as ak
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.s3_utils import S3Uploader
from utils.time_utils import get_current_date_str


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "stock-data")
s3_uploader = S3Uploader(bucket_name=S3_BUCKET)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_stock_data_from_akshare(**kwargs):
    # get current date as file name
    current_date_str = get_current_date_str()
    logging.info(f"Start extracting stock data from Akshare for date: {current_date_str}")

    # 1. quickly get all market data for the current date
    try:
        # stock_zh_a_spot_em: East Money-Shanghai, Shenzhen, Beijing A-share-real-time行情
        df = ak.stock_zh_a_spot_em()
    except Exception as e:
        raise ValueError(f"Failed to call Akshare API: {e}")

    if df.empty:
        logging.warning("No data found, maybe it's a holiday or market closed.")
        return None

    # 2. simple data cleaning
    df['date'] = current_date_str
    
    # 3. use temporary file to save and upload
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=True) as tmp_file:
        
        logging.info(f"Create temporary file: {tmp_file.name}")
        
        df.to_csv(tmp_file.name, index=False, encoding='utf-8-sig')
        
        # define S3 path
        s3_key = f"extract/akshare/{current_date_str}.csv"
        
        s3_path = s3_uploader.upload_file(tmp_file.name, s3_key)
        
        logging.info(f"Uploaded to S3: {s3_path}")
    
    logging.info("Task completed.")
    return s3_path

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule='0 18 * * *', # every day at 18:00
    catchup=False,
    tags=['extractor'],
)

run_task = PythonOperator(
    task_id='extractor_stock_data_from_akshare',
    python_callable=extract_stock_data_from_akshare,
    dag=dag,
)