import sys
from pathlib import Path

import pytest
from airflow.models import Connection  # noqa: E402
from airflow.utils.task_group import TaskGroup  # noqa: E402

from dags.ods_loader_dag import (
    create_ods_loader_dag,
    load_ods_config,
    load_partition,
)
from dags.utils.duckdb_utils import S3ConnectionConfig
from dags.utils.etl_utils import build_s3_connection_config

airflow = pytest.importorskip("airflow")

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def test_load_ods_config_parses_entries(tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
- dest: sample_table
  src:
    type: s3
    properties:
      path: lake/raw/sample
""",
        encoding="utf-8",
    )

    entries = load_ods_config(config_path)

    assert len(entries) == 1
    assert entries[0]["dest"] == "sample_table"
    assert entries[0]["src"]["properties"]["path"] == "lake/raw/sample"


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
        group for group in dag.task_group.children.values() if isinstance(group, TaskGroup)
    ]

    dest_ids = {group.group_id for group in task_groups}
    config_path = ROOT_DIR / "dags" / "ods" / "config.yaml"
    entries = load_ods_config(config_path)
    expected = {entry["dest"] for entry in entries}
    assert expected.issubset(dest_ids)


def test_load_partition_noops_when_no_source_files(monkeypatch: pytest.MonkeyPatch):
    pushed: dict[str, object] = {}

    class DummyTI:
        def xcom_pull(self, task_ids: str, key: str | None = None):  # noqa: ANN001
            assert task_ids == "sample.prepare"
            return {
                "partition_date": "2025-12-17",
                "canonical_prefix": "s3://stock-data/lake/ods/sample/dt=2025-12-17",
                "tmp_prefix": "s3://stock-data/lake/ods/sample/_tmp/run_rid",
                "tmp_partition_prefix": "s3://stock-data/lake/ods/sample/_tmp/run_rid/dt=2025-12-17",
                "manifest_path": "s3://stock-data/lake/ods/sample/dt=2025-12-17/manifest.json",
                "success_flag_path": "s3://stock-data/lake/ods/sample/dt=2025-12-17/_SUCCESS",
            }

        def xcom_push(self, key: str, value: object) -> None:
            pushed[key] = value

    class DummyS3Hook:
        def __init__(self, **_kwargs):  # noqa: ANN003
            pass

        def list_keys(self, bucket_name: str, prefix: str):  # noqa: ANN001
            assert bucket_name == "stock-data"
            assert prefix == "lake/raw/sample/dt=2025-12-17/"
            return []

    import dags.ods_loader_dag as module

    monkeypatch.setattr(module, "S3Hook", DummyS3Hook)

    metrics = load_partition(
        table_config={"dest": "sample", "src": {"properties": {"path": "lake/raw/sample"}}},
        partition_date="2025-12-17",
        bucket_name="stock-data",
        run_id="rid",
        task_group_id="sample",
        ti=DummyTI(),
    )
    assert metrics["has_data"] == 0
    assert pushed["load_metrics"] == metrics
