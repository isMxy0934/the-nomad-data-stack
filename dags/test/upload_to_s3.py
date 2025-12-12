from datetime import datetime
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 你希望上传到的 bucket 名称

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "test-bucket")

print(f"BUCKET_NAME: {BUCKET_NAME}")
print(f"S3_ACCESS_KEY_ID: {os.getenv('S3_ACCESS_KEY_ID', 'NOT_SET')}")
print(f"S3_SECRET_ACCESS_KEY: {os.getenv('S3_SECRET_ACCESS_KEY', 'NOT_SET')}")
print(f"S3_ENDPOINT_URL: {os.getenv('S3_ENDPOINT_URL', 'NOT_SET')}")

def create_local_file():
    """
    任务 1: 在本地生成一个临时文件
    """
    local_path = "/tmp/airflow_test_upload.txt"
    with open(local_path, "w") as f:
        f.write("Hello from Airflow! 🎉\n")
        f.write(f"Uploaded at: {datetime.utcnow().isoformat()}Z\n")
    print(f"Created file at {local_path}")
    return local_path

def upload_file_to_s3(**context):
    """
    任务 2: 使用 S3Hook 上传文件到 S3
    """
    # 取前一个任务创建的文件路径
    ti = context["ti"]
    local_file = ti.xcom_pull(task_ids="create_file")

    # 用你配置的 Airflow 连接 ID 来初始化 S3Hook
    s3 = S3Hook(aws_conn_id="MINIO_S3")
    key = f"airflow_uploads/{os.path.basename(local_file)}"

    # 上传本地文件到指定 bucket
    s3.load_file(
        filename=local_file,
        bucket_name=BUCKET_NAME,
        key=key,
        replace=True,
    )
    print(f"Uploaded {local_file} to s3://{BUCKET_NAME}/{key}")

with DAG(
    dag_id="test_upload_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "s3"],
) as dag:

    create_file = PythonOperator(
        task_id="create_file",
        python_callable=create_local_file,
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_file_to_s3,
    )

    create_file >> upload_to_s3
