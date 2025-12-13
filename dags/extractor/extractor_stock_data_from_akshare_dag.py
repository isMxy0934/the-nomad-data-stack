import io
import os
import logging
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from utils.time_utils import get_current_date_str, get_current_partition_date_str
from utils.config_loader import load_metadata_config
from utils.s3_utils import S3Uploader

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")
s3_uploader = S3Uploader(bucket_name=S3_BUCKET_NAME)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id=DAG_ID, 
    default_args=default_args,
    schedule_interval="0 18 * * *",
    start_date=datetime(2025, 12, 1), 
    catchup=False,
    tags=['extractor']
)
def extractor_stock_data_from_akshare_dag():

    @task
    def get_akshare_tasks():
        target_source = "akshare"
        tasks = load_metadata_config(target_source)
        if not tasks:
            logging.warning(f"No tasks found for source: {target_source}")
            return []
        
        # add custom task_id
        for task in tasks:
            task['task_id'] = f"fetch_akshare_{task['symbol']}"
        
        return tasks

    @task(max_active_tis_per_dag=1)
    def fetch_akshare(item: dict):
        """
        Task 2: fetch single stock data and upload to S3
        """
        symbol = item['symbol']
        asset_type = item['type']
        
        target_date_str = get_current_date_str()
        target_partition_date_str = get_current_partition_date_str()
        
        logging.info(f"Start fetching stock data from Akshare for {symbol} ({asset_type}) on {target_date_str}")

        df = pd.DataFrame()
        try:
            if asset_type == 'etf':
                df = ak.fund_etf_hist_em(
                    symbol=symbol, 
                    period="daily", 
                    start_date=target_date_str, 
                    end_date=target_date_str, 
                    adjust="qfq"
                )
            elif asset_type == 'stock':
                df = ak.stock_zh_a_hist(
                    symbol=symbol, 
                    period="daily", 
                    start_date=target_date_str, 
                    end_date=target_date_str, 
                    adjust="qfq"
                )
            else:
                logging.warning(f"unknown asset type: {asset_type}, skip")
                return None

            if df.empty:
                logging.warning(f"no data found for {symbol} on {target_date_str}, maybe it's a holiday or market closed")
                return None

            file_key = f"extractor/akshare/{symbol}/{target_partition_date_str}.csv"
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            s3_uploader.upload_bytes(csv_buffer.getvalue().encode('utf-8'), file_key)
            
            logging.info(f"uploaded to S3: {file_key}")
            return file_key

        except Exception as e:
            logging.error(f"failed to fetch stock data from Akshare for {symbol}: {e}")
            # raise exception to let Airflow know this task failed, so it can trigger retry
            raise e

    task_list = get_akshare_tasks()
    
    fetch_akshare.expand(item=task_list)

dag_instance = extractor_stock_data_from_akshare_dag()