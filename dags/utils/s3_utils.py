from datetime import datetime
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Uploader:
    """S3 File upload utility class"""
    
    def __init__(self, aws_conn_id="MINIO_S3", bucket_name=None):
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "test-bucket")
        self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
    
    def upload_file(self, local_file_path, key=None, replace=True) -> str:
        """
        Upload local files to S3
        
        Args:
            local_file_path: local file path
            key: S3 key, if not provided, use default format
            replace: whether to replace existing file
        
        Returns:
            str: S3 file full path
        """
        if key is None:
            raise ValueError("s3 key is required")
        
        self.s3_hook.load_file(
            filename=local_file_path,
            bucket_name=self.bucket_name,
            key=key,
            replace=replace,
        )
        s3_path = f"s3://{self.bucket_name}/{key}"
        print(f"Uploaded {local_file_path} to {s3_path}")
        return s3_path