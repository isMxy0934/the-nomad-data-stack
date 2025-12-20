from __future__ import annotations

import duckdb
import pytest

from lakehouse_core.catalog_sql import (
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
    assert "dt=*/**/*.parquet" in sql
    assert "hive_partitioning=true" in sql


def test_build_layer_view_sql_parquet_non_partitioned():
    layer = LayerSpec(schema="dw", base_prefix="lake/dw", partitioned_by_dt=False)
    sql = build_layer_view_sql(bucket="stock-data", layer=layer, table="dim_stock")
    assert "hive_partitioning=false" in sql
    assert "*.parquet" in sql


def test_build_layer_view_sql_csv():
    layer = LayerSpec(
        schema="raw", base_prefix="lake/raw", partitioned_by_dt=False, file_extension="csv"
    )
    sql = build_layer_view_sql(bucket="stock-data", layer=layer, table="raw_stock")
    assert "read_csv_auto" in sql


def test_build_layer_view_sql_unsupported_ext():
    layer = LayerSpec(schema="raw", base_prefix="lake/raw", file_extension="txt")
    with pytest.raises(ValueError, match="Unsupported file_extension"):
        build_layer_view_sql(bucket="stock-data", layer=layer, table="t")


def test_build_layer_dt_macro_sql_errors():
    layer = LayerSpec(schema="ods", base_prefix="lake/ods", partitioned_by_dt=False)
    with pytest.raises(ValueError, match="macro only applies to dt-partitioned layers"):
        build_layer_dt_macro_sql(bucket="b", layer=layer, table="t")

    layer_csv = LayerSpec(
        schema="ods", base_prefix="lake/ods", partitioned_by_dt=True, file_extension="csv"
    )
    with pytest.raises(ValueError, match="macro currently supports parquet only"):
        build_layer_dt_macro_sql(bucket="b", layer=layer_csv, table="t")


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
