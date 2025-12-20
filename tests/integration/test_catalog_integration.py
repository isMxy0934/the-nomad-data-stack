"""Integration test for DuckDB analysis catalog (metadata-only)."""

from __future__ import annotations

from pathlib import Path

import duckdb

from lakehouse_core.api import prepare_paths
from lakehouse_core.catalog import (
    LayerSpec,
    apply_migrations,
    build_layer_dt_macro_sql,
    build_layer_view_sql,
)
from lakehouse_core.compute import (
    configure_s3_access,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
)
from lakehouse_core.domain.manifest import build_manifest
from lakehouse_core.io.sql import load_and_render_sql
from lakehouse_core.io.uri import parse_s3_uri

ROOT_DIR = Path(__file__).resolve().parents[2]


def _create_tmp_raw_view(
    *, conn: duckdb.DuckDBPyConnection, table_name: str, csv_path: str
) -> None:
    execute_sql(
        conn,
        f"CREATE OR REPLACE VIEW tmp_{table_name} AS SELECT * FROM read_csv_auto('{csv_path}', hive_partitioning=false);",
    )


def _materialize_ods_partition(
    *,
    minio_client,
    test_bucket_name: str,
    test_s3_config,
    integration_prefix: str,
    table_name: str,
    src_path: str,
    csv_fixture: str,
    partition_date: str,
    run_id: str,
    load_test_csv,
    s3_publish_partition,
) -> None:
    csv_rel = src_path.strip("/")
    if csv_rel.startswith("lake/"):
        csv_rel = csv_rel[len("lake/") :]

    csv_key = f"{integration_prefix}/{csv_rel}/dt={partition_date}/data.csv"
    csv_content = load_test_csv(csv_fixture, partition_date)
    minio_client.put_object(Bucket=test_bucket_name, Key=csv_key, Body=csv_content)

    base_prefix = f"{integration_prefix}/ods/{table_name}"
    paths = prepare_paths(
        base_prefix=base_prefix,
        run_id=run_id,
        partition_date=partition_date,
        is_partitioned=True,
        store_namespace=test_bucket_name,
    )

    conn = create_temporary_connection()
    configure_s3_access(conn, test_s3_config)

    csv_path = f"s3://{test_bucket_name}/{csv_key}"
    _create_tmp_raw_view(conn=conn, table_name=table_name, csv_path=csv_path)

    sql_template_path = ROOT_DIR / "dags" / "ods" / f"{table_name}.sql"
    rendered_sql = load_and_render_sql(sql_template_path, {"PARTITION_DATE": partition_date})

    copy_partitioned_parquet(
        conn,
        query=rendered_sql,
        destination=paths.tmp_prefix,
        partition_column="dt",
        filename_pattern="file_{uuid}",
        use_tmp_file=True,
    )

    row_count = int(conn.execute(f"SELECT COUNT(*) FROM ({rendered_sql})").fetchone()[0])
    tmp_bucket, tmp_prefix_key = parse_s3_uri(paths.tmp_prefix)
    tmp_prefix_key = tmp_prefix_key.rstrip("/") + "/"
    tmp_objects = minio_client.list_objects_v2(Bucket=tmp_bucket, Prefix=tmp_prefix_key)
    parquet_files = [
        obj["Key"] for obj in tmp_objects.get("Contents", []) if obj["Key"].endswith(".parquet")
    ]

    manifest = build_manifest(
        dest=table_name,
        partition_date=partition_date,
        run_id=run_id,
        file_count=len(parquet_files),
        row_count=row_count,
        source_prefix=paths.tmp_partition_prefix,
        target_prefix=paths.canonical_prefix,
    )
    s3_publish_partition(paths, dict(manifest))


def test_duckdb_catalog_can_query_views_and_macros(
    tmp_path: Path,
    minio_client,
    test_bucket_name,
    test_s3_config,
    test_date,
    integration_prefix,
    load_test_csv,
    s3_publish_partition,
    ods_table_case,
):
    table_name = ods_table_case.dest

    _materialize_ods_partition(
        minio_client=minio_client,
        test_bucket_name=test_bucket_name,
        test_s3_config=test_s3_config,
        integration_prefix=integration_prefix,
        table_name=table_name,
        src_path=ods_table_case.src_path,
        csv_fixture=ods_table_case.csv_fixture,
        partition_date=test_date,
        run_id="catalog-test-run",
        load_test_csv=load_test_csv,
        s3_publish_partition=s3_publish_partition,
    )

    catalog_db = tmp_path / "catalog.duckdb"
    migrations_dir = ROOT_DIR / "catalog" / "migrations"

    conn = duckdb.connect(str(catalog_db))
    configure_s3_access(conn, test_s3_config)
    apply_migrations(conn, migrations_dir=migrations_dir)

    configure_s3_access(conn, test_s3_config)
    layer = LayerSpec(schema="ods", base_prefix=f"{integration_prefix}/ods", partitioned_by_dt=True)
    conn.execute(build_layer_view_sql(bucket=test_bucket_name, layer=layer, table=table_name))
    conn.execute(build_layer_dt_macro_sql(bucket=test_bucket_name, layer=layer, table=table_name))
    conn.close()

    query_conn = create_temporary_connection(database=catalog_db)
    configure_s3_access(query_conn, test_s3_config)

    view_count = query_conn.execute(f"SELECT COUNT(*) FROM ods.{table_name};").fetchone()[0]
    macro_count = query_conn.execute(
        f"SELECT COUNT(*) FROM ods.{table_name}_dt('{test_date}');"
    ).fetchone()[0]

    assert int(view_count) > 0
    assert int(macro_count) == int(view_count)
