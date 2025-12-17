"""Integration test for DuckDB analysis catalog (metadata-only)."""

from __future__ import annotations

from pathlib import Path

import duckdb

from dags.utils.catalog_migrations import apply_migrations
from dags.utils.catalog_utils import LayerSpec, build_layer_dt_macro_sql, build_layer_view_sql
from dags.utils.duckdb_utils import configure_s3_access, create_temporary_connection, execute_sql
from dags.utils.partition_utils import build_manifest, build_partition_paths, parse_s3_uri
from dags.utils.sql_utils import load_and_render_sql

ROOT_DIR = Path(__file__).resolve().parents[2]


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
    paths = build_partition_paths(
        base_prefix=base_prefix,
        partition_date=partition_date,
        run_id=run_id,
        bucket_name=test_bucket_name,
    )

    conn = create_temporary_connection()
    configure_s3_access(conn, test_s3_config)

    csv_path = f"s3://{test_bucket_name}/{csv_key}"
    execute_sql(
        conn,
        f"CREATE OR REPLACE VIEW tmp_{table_name} AS SELECT * FROM read_csv_auto('{csv_path}');",
    )

    sql_template_path = ROOT_DIR / "dags" / "ods" / f"{table_name}.sql"
    rendered_sql = load_and_render_sql(sql_template_path, {"PARTITION_DATE": partition_date})

    tmp_parquet_uri = f"{paths.tmp_prefix}/data.parquet"
    execute_sql(conn, f"COPY ({rendered_sql}) TO '{tmp_parquet_uri}' (FORMAT PARQUET);")

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
        source_prefix=paths.tmp_prefix,
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

    # 1) Apply migrations into the catalog DB (metadata-only).
    conn = duckdb.connect(str(catalog_db))
    apply_migrations(conn, migrations_dir=migrations_dir)

    # 2) Register view + macro for this table, pointing at integration prefix.
    configure_s3_access(conn, test_s3_config)
    layer = LayerSpec(schema="ods", base_prefix=f"{integration_prefix}/ods", partitioned_by_dt=True)
    conn.execute(build_layer_view_sql(bucket=test_bucket_name, layer=layer, table=table_name))
    conn.execute(build_layer_dt_macro_sql(bucket=test_bucket_name, layer=layer, table=table_name))
    conn.close()

    # 3) Query through the catalog with S3 settings on the query connection.
    query_conn = create_temporary_connection(database=catalog_db)
    configure_s3_access(query_conn, test_s3_config)

    view_count = query_conn.execute(f"SELECT COUNT(*) FROM ods.{table_name};").fetchone()[0]
    macro_count = query_conn.execute(
        f"SELECT COUNT(*) FROM ods.{table_name}_dt('{test_date}');"
    ).fetchone()[0]

    assert int(view_count) > 0
    assert int(macro_count) == int(view_count)
