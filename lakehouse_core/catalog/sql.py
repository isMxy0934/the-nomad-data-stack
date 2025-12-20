from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LayerSpec:
    """Describe how to map a lake layer into a DuckDB schema."""

    schema: str
    base_prefix: str
    partitioned_by_dt: bool = True
    file_extension: str = "parquet"


def quote_ident(identifier: str) -> str:
    """Quote an identifier for DuckDB SQL."""

    if identifier == "":
        raise ValueError("identifier must not be empty")
    return '"' + identifier.replace('"', '""') + '"'


def normalize_prefix(prefix: str) -> str:
    """Normalize an S3 key prefix by stripping leading/trailing slashes."""

    normalized = prefix.strip("/")
    if not normalized:
        raise ValueError("prefix must not be empty")
    return normalized


def build_layer_view_sql(*, bucket: str, layer: LayerSpec, table: str) -> str:
    """Build SQL that creates/updates a schema-qualified view for a table."""

    if not bucket:
        raise ValueError("bucket is required")
    if not table:
        raise ValueError("table is required")

    base_prefix = normalize_prefix(layer.base_prefix)
    if layer.partitioned_by_dt:
        hive_partitioning = "true"
    else:
        path = f"s3://{bucket}/{base_prefix}/{table}/*.{layer.file_extension}"
        hive_partitioning = "false"

    schema_sql = quote_ident(layer.schema)
    table_sql = quote_ident(table)

    if layer.file_extension == "parquet":
        reader = "read_parquet"
    elif layer.file_extension == "csv":
        reader = "read_csv_auto"
    else:
        raise ValueError(f"Unsupported file_extension: {layer.file_extension}")

    if layer.partitioned_by_dt:
        # Support both layouts via recursive glob:
        # - dt=YYYY-MM-DD/*.parquet (flat)
        # - dt=YYYY-MM-DD/dt=YYYY-MM-DD/*.parquet (nested, can happen if you
        #   partition-by dt under a dt-prefixed destination)
        path = f"s3://{bucket}/{base_prefix}/{table}/dt=*/**/*.{layer.file_extension}"
        select_sql = f"SELECT * FROM {reader}('{path}', hive_partitioning={hive_partitioning})"
    else:
        select_sql = f"SELECT * FROM {reader}('{path}', hive_partitioning={hive_partitioning})"

    return f"CREATE OR REPLACE VIEW {schema_sql}.{table_sql} AS\n{select_sql};"


def build_layer_dt_macro_sql(*, bucket: str, layer: LayerSpec, table: str) -> str:
    """Build a table macro to read an exact dt partition without listing all partitions."""

    if not layer.partitioned_by_dt:
        raise ValueError("dt macro only applies to dt-partitioned layers")
    if not bucket:
        raise ValueError("bucket is required")
    if not table:
        raise ValueError("table is required")

    if layer.file_extension != "parquet":
        raise ValueError("dt macro currently supports parquet only")

    base_prefix = normalize_prefix(layer.base_prefix)
    schema_sql = quote_ident(layer.schema)
    macro_sql = quote_ident(f"{table}_dt")

    return (
        f"CREATE OR REPLACE MACRO {schema_sql}.{macro_sql}(partition_date) AS TABLE\n"
        f"SELECT * FROM read_parquet(\n"
        f"  's3://{bucket}/{base_prefix}/{table}/dt=' || partition_date || '/**/*.parquet',\n"
        f"  hive_partitioning=true\n"
        f");"
    )


def build_schema_sql(schema: str) -> str:
    """Create schema statement."""

    return f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)};"

