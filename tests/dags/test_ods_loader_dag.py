import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from airflow.models import Connection
from airflow.utils.task_group import TaskGroup

from dags.ods_loader_dag import (
    build_s3_connection_config,
    create_ods_loader_dag,
    get_table_pool_name,
    load_ods_config,
)
from dags.utils.duckdb_utils import S3ConnectionConfig


def test_load_ods_config_parses_entries(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
- dest: sample_table
  src:
    type: s3
    properties:
      path: raw/sample
""",
        encoding="utf-8",
    )

    entries = load_ods_config(config_path)

    assert len(entries) == 1
    assert entries[0]["dest"] == "sample_table"
    assert entries[0]["src"]["properties"]["path"] == "raw/sample"


def test_get_table_pool_name_prefixes_dest():
    assert get_table_pool_name("orders") == "ods_pool_orders"


def test_build_s3_connection_config_reads_extras():
    connection = Connection(
        conn_id="MINIO_S3",
        conn_type="aws",
        login="access",
        password="secret",
        extra={
            "endpoint_url": "http://minio:9000",
            "region_name": "us-east-1",
            "s3_url_style": "path",
            "use_ssl": False,
            "session_token": "token",
        },
    )

    class DummyHook:
        def __init__(self) -> None:
            self.aws_conn_id = "MINIO_S3"

        def get_connection(self, conn_id: str) -> Connection:  # noqa: ANN401
            assert conn_id == self.aws_conn_id
            return connection

    config = build_s3_connection_config(DummyHook())

    assert isinstance(config, S3ConnectionConfig)
    assert config.endpoint_url == "http://minio:9000"
    assert config.access_key == "access"
    assert config.secret_key == "secret"
    assert config.session_token == "token"
    assert config.url_style == "path"


def test_create_ods_loader_dag_builds_task_groups():
    dag = create_ods_loader_dag()
    task_groups = [
        group
        for group in dag.task_group.children.values()
        if isinstance(group, TaskGroup)
    ]

    dest_ids = {group.group_id for group in task_groups}
    assert "ods_daily_stock_price_akshare" in dest_ids
