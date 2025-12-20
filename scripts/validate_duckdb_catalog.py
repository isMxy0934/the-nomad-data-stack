#!/usr/bin/env python3
"""Validation script for DuckDB catalog SQL generation (no MinIO required)."""

from __future__ import annotations

from lakehouse_core.catalog_sql import (
    LayerSpec,
    build_layer_dt_macro_sql,
    build_layer_view_sql,
)
from lakehouse_core.execution import create_temporary_connection


def main() -> int:
    layer = LayerSpec(schema="ods", base_prefix="lake/ods", partitioned_by_dt=True)
    view_sql = build_layer_view_sql(bucket="stock-data", layer=layer, table="ods_example")
    macro_sql = build_layer_dt_macro_sql(bucket="stock-data", layer=layer, table="ods_example")

    # This script must not require MinIO credentials. Creating a VIEW that points to
    # `s3://...` may trigger S3 binding/listing, so we only validate SQL generation
    # and that DuckDB accepts the table macro definition (macro is not executed).
    if "read_parquet" not in view_sql:
        raise AssertionError("Expected view SQL to reference read_parquet")

    conn = create_temporary_connection()
    conn.execute("CREATE SCHEMA IF NOT EXISTS ods;")
    conn.execute(macro_sql)

    macro_names = {
        row[0]
        for row in conn.execute(
            "SELECT function_name FROM duckdb_functions() WHERE function_type='table_macro' AND schema_name='ods';"
        ).fetchall()
    }
    if "ods_example_dt" not in macro_names:
        raise AssertionError("Expected macro ods.ods_example_dt to exist")

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
