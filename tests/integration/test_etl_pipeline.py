"""Test end-to-end ETL pipeline integration - single table smoke test."""

import json
from pathlib import Path

from dags.utils.duckdb_utils import configure_s3_access, create_temporary_connection, execute_sql
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


def test_single_table_etl_pipeline(minio_client, test_bucket_name, test_s3_config, test_date):
    """Test complete ETL flow for a single table: raw data -> ODS -> validation"""

    table_name = "ods_daily_stock_price_akshare"

    # 1. 准备测试数据（只准备一个表的数据）
    prepare_single_table_data(minio_client, test_bucket_name, test_date)

    # 2. 执行ODS处理流程（直接使用核心工具，不依赖Airflow）
    run_id = f"test-run-{table_name}"
    base_prefix = f"dw/ods/{table_name}"
    paths = build_partition_paths(
        base_prefix=base_prefix,
        partition_date=test_date,
        run_id=run_id,
        bucket_name=test_bucket_name,
    )

    conn = create_temporary_connection()
    configure_s3_access(conn, test_s3_config)

    csv_path = f"s3://{test_bucket_name}/raw/daily/stock_price/akshare/dt={test_date}/data.csv"
    execute_sql(
        conn,
        f"CREATE OR REPLACE VIEW tmp_{table_name} AS SELECT * FROM read_csv_auto('{csv_path}');",
    )

    sql_template_path = ROOT_DIR / "dags" / "ods" / f"{table_name}.sql"
    rendered_sql = load_and_render_sql(sql_template_path, {"PARTITION_DATE": test_date})

    tmp_parquet_uri = f"{paths.tmp_prefix}/data.parquet"
    execute_sql(conn, f"COPY ({rendered_sql}) TO '{tmp_parquet_uri}' (FORMAT PARQUET);")

    manifest = build_manifest(
        dest=table_name,
        partition_date=test_date,
        run_id=run_id,
        file_count=1,
        row_count=2,
        source_prefix=paths.tmp_prefix,
        target_prefix=paths.canonical_prefix,
    )
    _publish_partition(minio_client, paths=paths, manifest=dict(manifest))

    # 3. 验证处理结果
    verify_table_processed(minio_client, test_bucket_name, table_name, test_date)


def prepare_single_table_data(minio_client, bucket, test_date):
    """Helper to prepare test data for single table"""

    # 测试数据内容 - 只测试股票价格表
    csv_content = """date,code,open,close,volume
2024-01-15,000001,10.5,11.2,1000000
2024-01-15,000002,20.1,19.8,500000"""

    # 上传测试数据
    key = f"raw/daily/stock_price/akshare/dt={test_date}/data.csv"
    minio_client.put_object(Bucket=bucket, Key=key, Body=csv_content)


def verify_table_processed(minio_client, bucket, table, test_date):
    """Helper to verify table processing"""

    # 检查 Parquet 文件
    objects = minio_client.list_objects_v2(Bucket=bucket, Prefix=f"dw/ods/{table}/dt={test_date}/")

    assert objects.get("Contents"), f"No files found for table {table}"

    parquet_files = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".parquet")]
    assert len(parquet_files) > 0, f"No parquet files found for table {table}"

    # 检查 manifest.json
    manifest_response = minio_client.get_object(
        Bucket=bucket, Key=f"dw/ods/{table}/dt={test_date}/manifest.json"
    )
    manifest = json.loads(manifest_response["Body"].read())
    assert manifest["file_count"] == len(parquet_files)
    assert manifest["status"] == "success"
    assert manifest["row_count"] > 0
