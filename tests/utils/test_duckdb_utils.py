import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dags.utils.duckdb_utils import (  # pylint: disable=wrong-import-position
    S3ConnectionConfig,
    configure_s3_access,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
    temporary_connection,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


class DummyConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> Any:  # noqa: ANN401
        self.statements.append(sql)
        return sql


def test_create_temporary_connection_supports_basic_queries():
    conn = create_temporary_connection()

    result = conn.execute("SELECT 1 AS col").fetchone()

    assert isinstance(conn, duckdb.DuckDBPyConnection)
    assert result[0] == 1


def test_temporary_connection_context_manager_closes_connection():
    with temporary_connection() as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1

    with pytest.raises(duckdb.ConnectionException):
        conn.execute("SELECT 1")


def test_execute_sql_raises_for_empty_input():
    conn = create_temporary_connection()

    with pytest.raises(ValueError):
        execute_sql(conn, "   ")


def test_configure_s3_access_sets_expected_settings():
    conn = DummyConnection()
    config = S3ConnectionConfig(
        endpoint_url="http://minio:9000",
        access_key="minio",
        secret_key="password",
        region="us-east-1",
        use_ssl=False,
        url_style="path",
    )

    configure_s3_access(conn, config)

    assert conn.statements[0] in {"LOAD httpfs;", "INSTALL httpfs;"}
    assert "LOAD httpfs;" in conn.statements
    assert "SET s3_endpoint='minio:9000';" in conn.statements
    assert conn.statements[-1] == "SET s3_use_ssl=false;"


def test_copy_partitioned_parquet_writes_partitioned_files(tmp_path: Path):
    conn = create_temporary_connection(tmp_path / "test.duckdb")
    query = "SELECT 42 AS value, DATE '2024-01-01' AS dt"
    output_prefix = tmp_path / "output"

    copy_partitioned_parquet(
        conn,
        query=query,
        destination=str(output_prefix),
        partition_column="dt",
        filename_pattern="part_{uuid}",
    )

    partition_dir = output_prefix / "dt=2024-01-01"
    files = list(partition_dir.glob("*.parquet"))

    assert partition_dir.exists()
    assert len(files) >= 1


@pytest.mark.parametrize(
    "query,destination,partition_column",
    [
        ("", "s3://bucket/path", "dt"),
        ("SELECT 1", "", "dt"),
        ("SELECT 1", "s3://bucket/path", ""),
    ],
)
def test_copy_partitioned_parquet_validates_inputs(
    query: str, destination: str, partition_column: str
):
    conn = create_temporary_connection()

    with pytest.raises(ValueError):
        copy_partitioned_parquet(
            conn,
            query=query,
            destination=destination,
            partition_column=partition_column,
        )
