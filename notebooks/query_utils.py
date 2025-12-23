import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv


def connect_catalog(catalog_path=None):
    """
    分析人员专用：安全连接湖仓 Catalog。
    特点：
    1. 使用内存库作为主连接，不占用物理文件锁。
    2. 以 READ_ONLY 模式挂载物理 catalog.duckdb。
    3. 自动配置 S3/MinIO 访问环境。
    """
    # 加载项目根目录的 .env
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)

    if catalog_path is None:
        # 自动定位项目根目录下的 .duckdb/catalog.duckdb
        catalog_path = Path(__file__).parent.parent / ".duckdb" / "catalog.duckdb"

    # 1. 创建内存连接
    con = duckdb.connect()

    # 2. 配置 S3 访问
    endpoint = os.getenv("S3_ENDPOINT", "127.0.0.1:9000")
    access_key = os.getenv("S3_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("S3_SECRET_ACCESS_KEY", "minioadmin")

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_region='us-east-1';")

    # 3. 挂载物理库 (强制 READ_ONLY)
    if os.path.exists(catalog_path):
        # 使用绝对路径以防万一
        abs_path = os.path.abspath(catalog_path)
        con.execute(f"ATTACH '{abs_path}' AS catalog (READ_ONLY);")

        # 切换到 catalog 数据库，这样可以直接使用 ods.xxx, dwd.xxx 前缀
        con.execute("USE catalog;")

        print(f"✅ 已成功连接并切换至 Catalog: {abs_path}")
    else:
        print(f"⚠️ 未找到 Catalog 文件: {catalog_path}，已进入纯 S3 查询模式。")

    return con
