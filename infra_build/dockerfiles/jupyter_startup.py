import json
import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb

# Try to import project modules (if available)
try:
    import sys
    sys.path.insert(0, '/home/jovyan/lakehouse_core')
    from lakehouse_core.compute import S3ConnectionConfig, configure_s3_access, create_temporary_connection
    from lakehouse_core.catalog import attach_catalog_if_available
    USE_PROJECT_MODULES = True
except ImportError:
    USE_PROJECT_MODULES = False
    print("Project modules not found, using basic DuckDB setup")

def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_s3_from_airflow_conn() -> dict[str, str]:
    raw = os.getenv("AIRFLOW_CONN_MINIO_S3", "")
    if not raw:
        return {}
    try:
        payload = json.loads(_strip_quotes(raw))
    except json.JSONDecodeError:
        return {}
    extra = payload.get("extra") or {}
    return {
        "endpoint_url": extra.get("endpoint_url", ""),
        "access_key": payload.get("login", ""),
        "secret_key": payload.get("password", ""),
    }


airflow_s3 = _load_s3_from_airflow_conn()

# Get configuration from environment variables (fallback to AIRFLOW_CONN_MINIO_S3)
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL") or airflow_s3.get("endpoint_url") or "http://minio:9000"
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY_ID") or airflow_s3.get("access_key") or "minioadmin"
S3_SECRET_KEY = os.getenv("S3_SECRET_ACCESS_KEY") or airflow_s3.get("secret_key") or "minioadmin"
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "stock-data")
CATALOG_PATH = Path(os.getenv("DUCKDB_CATALOG_PATH", "/home/jovyan/.duckdb/catalog.duckdb"))


def _duckdb_endpoint(endpoint_url: str) -> str:
    value = (endpoint_url or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc:
        return parsed.netloc
    return value

# Create DuckDB connection
print("🔧 Configuring DuckDB connection...")
conn = duckdb.connect()

if USE_PROJECT_MODULES:
    # Use project's configuration utilities
    s3_config = S3ConnectionConfig(
        endpoint_url=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        region=S3_REGION,
        use_ssl=False,
        url_style='path',
    )
    configure_s3_access(conn, s3_config)
    print(f"S3 configured: {S3_ENDPOINT} (bucket: {S3_BUCKET})")
    
    # Attach catalog if available
    if attach_catalog_if_available(conn, catalog_path=CATALOG_PATH):
        print(f"Catalog attached: {CATALOG_PATH}")
    else:
        print(f"Catalog not found: {CATALOG_PATH}")
        print("   Run: scripts/duckdb_catalog_refresh.py to create it")
else:
    # Fallback: manual configuration
    try:
        conn.execute("LOAD httpfs;")
    except Exception:  # noqa: BLE001
        print("httpfs extension not available. Rebuild the image to preinstall it.")
        raise
    
    # Parse endpoint (remove http:// prefix if present)
    endpoint = S3_ENDPOINT.replace('http://', '').replace('https://', '')
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute(f"SET s3_region='{S3_REGION}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    print(f"S3 configured: {S3_ENDPOINT}")
    
    # Try to attach catalog manually
    if CATALOG_PATH.exists():
        conn.execute(f"ATTACH '{CATALOG_PATH}' AS catalog (READ_ONLY);")
        conn.execute("SET search_path='temp, catalog, main';")
        print(f"Catalog attached: {CATALOG_PATH}")
    else:
        print(f"Catalog not found: {CATALOG_PATH}")

# Make connection available globally
print("\nReady! Use `conn` to query data.\n")
print("   Example: conn.execute('SELECT * FROM ods.fund_names_em_akshare LIMIT 10').df()")

# Optional: enable %%sql magic with a preconfigured DuckDB connection.
try:
    from IPython import get_ipython

    ip = get_ipython()
    if ip is not None:
        ip.run_line_magic("load_ext", "sql")
        ip.run_line_magic("config", "SqlMagic.style = 'PLAIN_COLUMNS'")
        ip.run_line_magic("sql", "duckdb:///:memory:")

        endpoint = _duckdb_endpoint(S3_ENDPOINT)
        ip.run_cell_magic(
            "sql",
            "",
            f"""
            LOAD httpfs;
            SET s3_region='{S3_REGION}';
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{S3_ACCESS_KEY}';
            SET s3_secret_access_key='{S3_SECRET_KEY}';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
            """,
        )

        if CATALOG_PATH.exists():
            ip.run_cell_magic(
                "sql",
                "",
                f"""
                ATTACH '{CATALOG_PATH}' AS catalog (READ_ONLY);
                SET search_path='temp, catalog, main';
                """,
            )
        print("SQL magic ready. Use `%%sql` in cells.")
except Exception as exc:  # noqa: BLE001
    print(f"SQL magic not available: {exc}")
