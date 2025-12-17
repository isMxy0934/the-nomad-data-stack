"""Test ODS processing pipeline - M1 core functionality."""

import json
from pathlib import Path

from dags.utils.duckdb_utils import (
    configure_s3_access,
    create_temporary_connection,
    execute_sql,
)
from dags.utils.partition_utils import build_manifest, build_partition_paths, parse_s3_uri
from dags.utils.sql_utils import load_and_render_sql

ROOT_DIR = Path(__file__).resolve().parents[2]


def _delete_prefix(minio_client, *, bucket: str, prefix: str) -> None:
    paginator = minio_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue
        minio_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
        )


def _publish_partition(minio_client, *, paths, manifest: dict) -> None:
    canonical_bucket, canonical_prefix_key = parse_s3_uri(paths.canonical_prefix)
    tmp_bucket, tmp_prefix_key = parse_s3_uri(paths.tmp_prefix)
    if canonical_bucket != tmp_bucket:
        raise AssertionError("tmp/canonical buckets must match in integration tests")

    canonical_prefix_key = canonical_prefix_key.rstrip("/") + "/"
    tmp_prefix_key = tmp_prefix_key.rstrip("/") + "/"

    _delete_prefix(minio_client, bucket=canonical_bucket, prefix=canonical_prefix_key)

    paginator = minio_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=tmp_bucket, Prefix=tmp_prefix_key):
        for obj in page.get("Contents", []):
            src_key = obj["Key"]
            rel = src_key[len(tmp_prefix_key) :]
            dest_key = canonical_prefix_key + rel
            minio_client.copy_object(
                Bucket=canonical_bucket,
                Key=dest_key,
                CopySource={"Bucket": tmp_bucket, "Key": src_key},
            )

    manifest_bucket, manifest_key = parse_s3_uri(paths.manifest_path)
    minio_client.put_object(Bucket=manifest_bucket, Key=manifest_key, Body=json.dumps(manifest))

    success_bucket, success_key = parse_s3_uri(paths.success_flag_path)
    minio_client.put_object(Bucket=success_bucket, Key=success_key, Body=b"")


def _run_single_partition(
    *,
    minio_client,
    test_bucket_name: str,
    test_s3_config,
    table_name: str,
    partition_date: str,
    run_id: str,
) -> dict:
    csv_key = f"raw/daily/stock_price/akshare/dt={partition_date}/data.csv"
    csv_path = f"s3://{test_bucket_name}/{csv_key}"

    base_prefix = f"dw/ods/{table_name}"
    paths = build_partition_paths(
        base_prefix=base_prefix,
        partition_date=partition_date,
        run_id=run_id,
        bucket_name=test_bucket_name,
    )

    conn = create_temporary_connection()
    configure_s3_access(conn, test_s3_config)

    execute_sql(
        conn,
        f"CREATE OR REPLACE VIEW tmp_{table_name} AS "
        f"SELECT * FROM read_csv_auto('{csv_path}');",
    )

    sql_template_path = ROOT_DIR / "dags" / "ods" / f"{table_name}.sql"
    rendered_sql = load_and_render_sql(sql_template_path, {"PARTITION_DATE": partition_date})

    tmp_parquet_uri = f"{paths.tmp_prefix}/data.parquet"
    execute_sql(conn, f"COPY ({rendered_sql}) TO '{tmp_parquet_uri}' (FORMAT PARQUET);")

    manifest = build_manifest(
        dest=table_name,
        partition_date=partition_date,
        run_id=run_id,
        file_count=1,
        row_count=2,
        source_prefix=paths.tmp_prefix,
        target_prefix=paths.canonical_prefix,
    )
    _publish_partition(minio_client, paths=paths, manifest=dict(manifest))
    return dict(manifest)


def test_ods_loader_basic_flow(minio_client, test_bucket_name, test_s3_config, test_date):
    """Test basic ODS loading: CSV -> DuckDB -> tmp parquet -> publish -> manifest/_SUCCESS."""

    csv_content = """date,code,open,close,volume
2024-01-15,000001,10.5,11.2,1000000
2024-01-15,000002,20.1,19.8,500000
"""

    minio_client.put_object(
        Bucket=test_bucket_name,
        Key=f"raw/daily/stock_price/akshare/dt={test_date}/data.csv",
        Body=csv_content,
    )

    table_name = "ods_daily_stock_price_akshare"
    _run_single_partition(
        minio_client=minio_client,
        test_bucket_name=test_bucket_name,
        test_s3_config=test_s3_config,
        table_name=table_name,
        partition_date=test_date,
        run_id="test-run-1",
    )

    verify_ods_output(minio_client, test_bucket_name, table_name, test_date)


def verify_ods_output(minio_client, bucket, table, date):
    """Helper to verify ODS output files"""
    # 检查 Parquet 文件
    objects = minio_client.list_objects_v2(
        Bucket=bucket,
        Prefix=f"dw/ods/{table}/dt={date}/",
    )
    parquet_files = [
        obj["Key"] for obj in objects.get("Contents", []) if obj["Key"].endswith(".parquet")
    ]
    assert len(parquet_files) > 0

    keys = {obj["Key"] for obj in objects.get("Contents", [])}
    assert f"dw/ods/{table}/dt={date}/_SUCCESS" in keys

    # 检查 manifest.json
    manifest_response = minio_client.get_object(
        Bucket=bucket,
        Key=f"dw/ods/{table}/dt={date}/manifest.json",
    )
    manifest = json.loads(manifest_response["Body"].read())
    assert manifest["file_count"] == len(parquet_files)
    assert manifest["status"] == "success"


def test_ods_loader_idempotency(minio_client, test_bucket_name, test_s3_config, test_date):
    """Test that re-running publish clears old partition contents."""

    csv_content = """date,code,open,close,volume
2024-01-15,000001,10.5,11.2,1000000
2024-01-15,000002,20.1,19.8,500000
"""

    minio_client.put_object(
        Bucket=test_bucket_name,
        Key=f"raw/daily/stock_price/akshare/dt={test_date}/data.csv",
        Body=csv_content,
    )

    table_name = "ods_daily_stock_price_akshare"

    manifest_1 = _run_single_partition(
        minio_client=minio_client,
        test_bucket_name=test_bucket_name,
        test_s3_config=test_s3_config,
        table_name=table_name,
        partition_date=test_date,
        run_id="test-run-1",
    )

    sentinel_key = f"dw/ods/{table_name}/dt={test_date}/SENTINEL"
    minio_client.put_object(Bucket=test_bucket_name, Key=sentinel_key, Body=b"sentinel")

    manifest_2 = _run_single_partition(
        minio_client=minio_client,
        test_bucket_name=test_bucket_name,
        test_s3_config=test_s3_config,
        table_name=table_name,
        partition_date=test_date,
        run_id="test-run-2",
    )

    objects = minio_client.list_objects_v2(
        Bucket=test_bucket_name,
        Prefix=f"dw/ods/{table_name}/dt={test_date}/",
    )
    keys = {obj["Key"] for obj in objects.get("Contents", [])}
    assert sentinel_key not in keys
    assert f"dw/ods/{table_name}/dt={test_date}/manifest.json" in keys
    assert f"dw/ods/{table_name}/dt={test_date}/_SUCCESS" in keys
    assert any(key.endswith(".parquet") for key in keys)
    assert manifest_1["run_id"] != manifest_2["run_id"]

    manifest_response = minio_client.get_object(
        Bucket=test_bucket_name,
        Key=f"dw/ods/{table_name}/dt={test_date}/manifest.json",
    )
    persisted_manifest = json.loads(manifest_response["Body"].read())
    assert persisted_manifest["run_id"] == manifest_2["run_id"]
