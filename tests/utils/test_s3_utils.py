import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.utils.s3_utils import S3Uploader
except ImportError as exc:
    pytest.skip(
        f"s3 utils imports unavailable in this environment: {exc}", allow_module_level=True
    )


class TestS3Uploader:
    @patch("dags.utils.s3_utils.S3Hook")
    def test_init_defaults(self, mock_s3_hook):
        uploader = S3Uploader()
        assert uploader.aws_conn_id == "MINIO_S3"
        assert uploader.bucket_name == "stock-data"
        mock_s3_hook.assert_called_once_with(aws_conn_id="MINIO_S3")

    @patch("dags.utils.s3_utils.S3Hook")
    def test_init_custom(self, mock_s3_hook):
        uploader = S3Uploader(aws_conn_id="CUSTOM_S3", bucket_name="custom-bucket")
        assert uploader.aws_conn_id == "CUSTOM_S3"
        assert uploader.bucket_name == "custom-bucket"
        mock_s3_hook.assert_called_once_with(aws_conn_id="CUSTOM_S3")

    @patch("dags.utils.s3_utils.S3Hook")
    def test_upload_file_success(self, mock_s3_hook):
        mock_hook_instance = mock_s3_hook.return_value
        uploader = S3Uploader()
        
        local_path = "/tmp/test.csv"
        key = "raw/test.csv"
        
        result = uploader.upload_file(local_path, key=key)
        
        mock_hook_instance.load_file.assert_called_once_with(
            filename=local_path,
            bucket_name="stock-data",
            key=key,
            replace=True
        )
        assert result == f"s3://stock-data/{key}"

    @patch("dags.utils.s3_utils.S3Hook")
    def test_upload_file_no_key_raises_error(self, mock_s3_hook):
        uploader = S3Uploader()
        with pytest.raises(ValueError, match="s3 key is required"):
            uploader.upload_file("/tmp/test.csv")

    @patch("dags.utils.s3_utils.S3Hook")
    def test_upload_bytes_success(self, mock_s3_hook):
        mock_hook_instance = mock_s3_hook.return_value
        uploader = S3Uploader()
        
        data = b"test data"
        key = "raw/test_bytes.csv"
        
        result = uploader.upload_bytes(data, key=key)
        
        mock_hook_instance.load_bytes.assert_called_once_with(
            bytes_data=data,
            bucket_name="stock-data",
            key=key,
            replace=True
        )
        assert result == f"s3://stock-data/{key}"

    @patch("dags.utils.s3_utils.S3Hook")
    def test_upload_bytes_no_key_raises_error(self, mock_s3_hook):
        uploader = S3Uploader()
        with pytest.raises(ValueError, match="s3 key is required"):
            uploader.upload_bytes(b"data", key="")
