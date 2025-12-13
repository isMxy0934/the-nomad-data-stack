import os
import logging
import akshare as ak
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from utils.s3_utils import S3Uploader
from utils.time_utils import get_previous_date_str, get_previous_partition_date_str

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

def fetch_stock_data_from_akshare():
    """
    Fetch A-Share Stock daily market snapshot from Akshare.
    API: stock_zh_a_spot_em (Eastern Money Source)
    """
    
    target_date_str = get_previous_date_str()
    target_partition_date_str = get_previous_partition_date_str()
    
    logging.info(f"Start fetching A-Share Stock snapshot for date: {target_date_str}")
    
    try:
        df = ak.stock_zh_a_spot_em(trade_date=target_date_str)
    except Exception as e:
        logging.error(f"Akshare API (stock) failed: {e}")
        raise e
    
    if df.empty:
        logging.warning(f"No stock data found for {target_date_str}")
        return None

    rename_map = {
        '代码': 'symbol',
        '名称': 'name',
        '最新价': 'close',
        '开盘': 'open',
        '今开': 'open',
        '最高': 'high',
        '最低': 'low',
        '成交量': 'vol',
        '成交额': 'amount',
        '昨收': 'pre_close',
        '换手率': 'turnover_rate',
        '涨跌幅': 'pct_chg',
        '市盈率-动态': 'pe_ttm',
        '市净率': 'pb'
    }
    
    available_cols = [c for c in rename_map.keys() if c in df.columns]
    df = df[available_cols].rename(columns=rename_map)

    df['trade_date'] = target_date_str
    
    s3_uploader = S3Uploader()
    file_key = f"daily/stock_price/akshare/{target_partition_date_str}.csv"
    
    try:
        s3_uploader.upload_bytes(df.to_csv(index=False).encode('utf-8'), file_key, replace=True)
        logging.info(f"uploaded stock data to S3: {file_key}, total {len(df)} records")
    except Exception as e:
        logging.error(f"failed to upload to S3: {e}")
        raise e
        
    return file_key

dag = DAG(
    dag_id=DAG_ID,
    schedule="30 18 * * *",
    start_date=datetime(2025, 12, 13),
    catchup=False,
    tags=['extractor', 'akshare', 'stock']
)

fetch_stock_task = PythonOperator(
    task_id='fetch_stock_data_from_akshare',
    python_callable=fetch_stock_data_from_akshare,
    dag=dag,
)