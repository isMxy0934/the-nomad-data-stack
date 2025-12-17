from __future__ import annotations

import duckdb
import pytest

from dags.utils.catalog_utils import (
    LayerSpec,
    build_layer_dt_macro_sql,
    build_layer_view_sql,
    quote_ident,
)


def test_quote_ident_escapes_quotes():
    assert quote_ident('a"b') == '"a""b"'


def test_quote_ident_rejects_empty():
    with pytest.raises(ValueError):
        quote_ident("")


def test_build_layer_view_sql_parquet_partitioned():
    layer = LayerSpec(schema="ods", base_prefix="lake/ods", partitioned_by_dt=True)
    sql = build_layer_view_sql(bucket="stock-data", layer=layer, table="ods_daily")
    assert "CREATE OR REPLACE VIEW" in sql
    assert "read_parquet" in sql
    assert "dt=*/*.parquet" in sql
    assert "hive_partitioning=true" in sql


def test_build_layer_dt_macro_sql_executes_in_duckdb():
    layer = LayerSpec(schema="ods", base_prefix="lake/ods", partitioned_by_dt=True)
    sql = build_layer_dt_macro_sql(bucket="stock-data", layer=layer, table="ods_daily")

    conn: duckdb.DuckDBPyConnection = duckdb.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS ods;")
    conn.execute(sql)
    macro_names = {
        row[0]
        for row in conn.execute(
            "SELECT function_name FROM duckdb_functions() WHERE function_type='table_macro' AND schema_name='ods';"
        ).fetchall()
    }
    assert "ods_daily_dt" in macro_names
