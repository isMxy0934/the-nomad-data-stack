"""Test end-to-end ETL pipeline integration - single table integration test."""

import json
from pathlib import Path

from dags.utils.duckdb_utils import (
    configure_s3_access,
    copy_partitioned_parquet,
    create_temporary_connection,
    execute_sql,
)
from lakehouse_core import build_manifest, parse_s3_uri, prepare_paths
from dags.utils.sql_utils import load_and_render_sql

ROOT_DIR = Path(__file__).resolve().parents[2]


def _create_tmp_raw_view(*, conn, table_name: str, csv_path: str) -> None:  # noqa: ANN001
    execute_sql(
        conn,
        f"CREATE OR REPLACE VIEW tmp_{table_name} AS SELECT * FROM read_csv_auto('{csv_path}', hive_partitioning=false);",
    )


def test_single_table_etl_pipeline(
    minio_client,
    test_bucket_name,
    test_s3_config,
    test_date,
    integration_prefix,
    load_test_csv,
    s3_publish_partition,
    ods_table_case,
):
    """Test complete ETL flow for a single table: raw data -> ODS -> validation"""

    table_name = ods_table_case.dest
    csv_rel = ods_table_case.src_path.strip("/")
    if csv_rel.startswith("lake/"):
        csv_rel = csv_rel[len("lake/") :]
    raw_key = f"{integration_prefix}/{csv_rel}/dt={test_date}/data.csv"

    csv_content = load_test_csv(ods_table_case.csv_fixture, test_date)
    minio_client.put_object(Bucket=test_bucket_name, Key=raw_key, Body=csv_content)

    run_id = f"test-run-{table_name}"
    base_prefix = f"{integration_prefix}/ods/{table_name}"
    paths = prepare_paths(
        base_prefix=base_prefix,
        run_id=run_id,
        partition_date=test_date,
        is_partitioned=True,
        store_namespace=test_bucket_name,
    )

    conn = create_temporary_connection()
    configure_s3_access(conn, test_s3_config)

    csv_path = f"s3://{test_bucket_name}/{raw_key}"
    _create_tmp_raw_view(conn=conn, table_name=table_name, csv_path=csv_path)

    sql_template_path = ROOT_DIR / "dags" / "ods" / f"{table_name}.sql"
    rendered_sql = load_and_render_sql(sql_template_path, {"PARTITION_DATE": test_date})

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
        partition_date=test_date,
        run_id=run_id,
        file_count=len(parquet_files),
        row_count=row_count,
        source_prefix=paths.tmp_partition_prefix,
        target_prefix=paths.canonical_prefix,
    )
    s3_publish_partition(paths, dict(manifest))

    verify_table_processed(minio_client, test_bucket_name, table_name, test_date)


def verify_table_processed(minio_client, bucket, table, test_date):
    """Helper to verify table processing"""

    objects = minio_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"lake/_integration/ods/{table}/dt={test_date}/",
    )

    assert objects.get("Contents"), f"No files found for table {table}"

    parquet_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".parquet")]
    assert len(parquet_files) > 0, f"No parquet files found for table {table}"

    manifest_response = minio_client.get_object(
        Bucket=bucket,
        Key=f"lake/_integration/ods/{table}/dt={test_date}/manifest.json",
    )
    manifest = json.loads(manifest_response["Body"].read())
    assert manifest["file_count"] == len(parquet_files)
    assert manifest["status"] == "success"
    assert manifest["row_count"] > 0
