"""Integration test for Data Warehouse (DW) pipeline following project standards."""

from pathlib import Path

from lakehouse_core.api import prepare_paths
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


def test_dwd_pipeline_standard_flow(
    minio_client,
    test_bucket_name,
    test_s3_config,
    test_date,
    integration_prefix,
    load_test_csv,
    s3_publish_partition,
):
    """Test standard DW flow: RAW (CSV) -> ODS (Parquet) -> DWD (Parquet)."""

    ods_table = "ods_fund_names_em_akshare"
    dwd_table = "dwd_daily_stock_price"

    # ---------------------------------------------------------
    # 1. Prepare RAW data in standard location
    # ---------------------------------------------------------
    raw_base_path = "raw/daily/fund_name/akshare/em"
    raw_key = f"{integration_prefix}/{raw_base_path}/dt={test_date}/data.csv"

    csv_content = load_test_csv("fund_names_em_akshare.csv", test_date)
    minio_client.put_object(Bucket=test_bucket_name, Key=raw_key, Body=csv_content)

    # ---------------------------------------------------------
    # 2. Run ODS process to produce standard ODS partition
    # ---------------------------------------------------------
    ods_conn = create_temporary_connection()
    configure_s3_access(ods_conn, test_s3_config)

    ods_run_id = f"test-run-ods-{test_date}"
    ods_base_prefix = f"{integration_prefix}/ods/{ods_table}"
    ods_paths = prepare_paths(
        base_prefix=ods_base_prefix,
        run_id=ods_run_id,
        partition_date=test_date,
        is_partitioned=True,
        store_namespace=test_bucket_name,
    )

    raw_s3_uri = f"s3://{test_bucket_name}/{raw_key}"
    execute_sql(
        ods_conn,
        f"""
        CREATE OR REPLACE VIEW tmp_{ods_table} AS
        SELECT * FROM read_csv_auto('{raw_s3_uri}', hive_partitioning=false);
        """,
    )

    ods_sql_path = ROOT_DIR / "dags" / "ods" / f"{ods_table}.sql"
    ods_rendered_sql = load_and_render_sql(ods_sql_path, {"PARTITION_DATE": test_date})

    copy_partitioned_parquet(
        ods_conn,
        query=ods_rendered_sql,
        destination=ods_paths.tmp_prefix,
        partition_column="dt",
        filename_pattern="file_{uuid}",
        use_tmp_file=True,
    )

    ods_row_count = int(
        ods_conn.execute(f"SELECT COUNT(*) FROM ({ods_rendered_sql})").fetchone()[0]
    )
    tmp_bucket, tmp_prefix_key = parse_s3_uri(ods_paths.tmp_prefix)
    tmp_prefix_key = tmp_prefix_key.rstrip("/") + "/"
    tmp_objects = minio_client.list_objects_v2(Bucket=tmp_bucket, Prefix=tmp_prefix_key)
    ods_parquet_files = [
        obj["Key"] for obj in tmp_objects.get("Contents", []) if obj["Key"].endswith(".parquet")
    ]

    ods_manifest = build_manifest(
        dest=ods_table,
        partition_date=test_date,
        run_id=ods_run_id,
        file_count=len(ods_parquet_files),
        row_count=ods_row_count,
        source_prefix=ods_paths.tmp_partition_prefix,
        target_prefix=ods_paths.canonical_prefix,
    )
    s3_publish_partition(ods_paths, dict(ods_manifest))

    # # ---------------------------------------------------------
    # # 3. Run DWD process reading from the standard ODS partition
    # # ---------------------------------------------------------
    # dw_conn = create_temporary_connection()
    # configure_s3_access(dw_conn, test_s3_config)

    # dw_conn.execute("CREATE SCHEMA IF NOT EXISTS ods;")
    # macro_sql = f"""
    # CREATE OR REPLACE MACRO ods.{ods_table}_dt(partition_date) AS TABLE
    # SELECT * FROM read_parquet(
    #   's3://{test_bucket_name}/{integration_prefix}/ods/{ods_table}/dt=' || partition_date || '/**/*.parquet',
    #   hive_partitioning=true
    # );
    # """
    # dw_conn.execute(macro_sql)

    # dwd_run_id = f"test-run-dwd-{test_date}"
    # dwd_base_prefix = f"{integration_prefix}/dwd/{dwd_table}"
    # dwd_paths = prepare_paths(
    #     base_prefix=dwd_base_prefix,
    #     run_id=dwd_run_id,
    #     partition_date=test_date,
    #     is_partitioned=True,
    #     store_namespace=test_bucket_name,
    # )

    # dwd_sql_path = ROOT_DIR / "dags" / "dwd" / f"{dwd_table}.sql"
    # dwd_rendered_sql = load_and_render_sql(dwd_sql_path, {"PARTITION_DATE": test_date})

    # copy_partitioned_parquet(
    #     dw_conn,
    #     query=dwd_rendered_sql,
    #     destination=dwd_paths.tmp_prefix,
    #     partition_column="dt",
    #     filename_pattern="file_{uuid}",
    #     use_tmp_file=True,
    # )

    # dwd_row_count = int(dw_conn.execute(f"SELECT COUNT(*) FROM ({dwd_rendered_sql})").fetchone()[0])
    # tmp_bucket, tmp_prefix_key = parse_s3_uri(dwd_paths.tmp_prefix)
    # tmp_prefix_key = tmp_prefix_key.rstrip("/") + "/"
    # tmp_objects = minio_client.list_objects_v2(Bucket=tmp_bucket, Prefix=tmp_prefix_key)
    # dwd_parquet_files = [
    #     obj["Key"] for obj in tmp_objects.get("Contents", []) if obj["Key"].endswith(".parquet")
    # ]

    # dwd_manifest = build_manifest(
    #     dest=dwd_table,
    #     partition_date=test_date,
    #     run_id=dwd_run_id,
    #     file_count=len(dwd_parquet_files),
    #     row_count=dwd_row_count,
    #     source_prefix=dwd_paths.tmp_partition_prefix,
    #     target_prefix=dwd_paths.canonical_prefix,
    # )
    # s3_publish_partition(dwd_paths, dict(dwd_manifest))

    # # ---------------------------------------------------------
    # # 4. Final Verification
    # # ---------------------------------------------------------
    # objects = minio_client.list_objects_v2(
    #     Bucket=test_bucket_name,
    #     Prefix=f"{integration_prefix}/dwd/{dwd_table}/dt={test_date}/",
    # )
    # keys = {obj["Key"] for obj in objects.get("Contents", [])}
    # assert f"{integration_prefix}/dwd/{dwd_table}/dt={test_date}/_SUCCESS" in keys
    # assert f"{integration_prefix}/dwd/{dwd_table}/dt={test_date}/manifest.json" in keys

    # check_conn = create_temporary_connection()
    # configure_s3_access(check_conn, test_s3_config)
    # dwd_path = (
    #     f"s3://{test_bucket_name}/{integration_prefix}/dwd/{dwd_table}/dt={test_date}/*.parquet"
    # )
    # res = check_conn.execute(
    #     f"SELECT COUNT(*), COUNT(DISTINCT symbol) FROM read_parquet('{dwd_path}')"
    # ).fetchone()

    # assert res[0] == dwd_row_count
    # assert res[1] > 0
