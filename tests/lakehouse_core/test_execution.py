from unittest.mock import MagicMock

import duckdb
import pytest

from lakehouse_core.compute.execution import copy_partitioned_parquet, normalize_duckdb_path


def test_copy_partitioned_parquet_falls_back_when_use_tmp_file_not_supported():
    connection = MagicMock()
    connection.execute.side_effect = [
        duckdb.NotImplementedException("nope"),
        None,
    ]

    copy_partitioned_parquet(
        connection,
        query="SELECT 1 AS x, DATE '2024-01-01' AS dt",
        destination="s3://bucket/tmp",
        partition_column="dt",
        filename_pattern="file_{uuid}",
        use_tmp_file=True,
    )

    assert connection.execute.call_count == 2
    first_sql = connection.execute.call_args_list[0].args[0]
    second_sql = connection.execute.call_args_list[1].args[0]
    assert "USE_TMP_FILE true" in first_sql
    assert "USE_TMP_FILE true" not in second_sql


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
    conn = MagicMock()
    with pytest.raises(ValueError):
        copy_partitioned_parquet(
            conn,
            query=query,
            destination=destination,
            partition_column=partition_column,
        )


def test_normalize_duckdb_path_converts_file_uri(tmp_path):
    uri = (tmp_path / "out").resolve().as_uri()
    normalized = normalize_duckdb_path(uri)
    assert not normalized.startswith("file://")
