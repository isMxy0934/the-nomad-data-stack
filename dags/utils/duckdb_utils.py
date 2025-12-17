"""DuckDB helpers for temporary compute and S3/MinIO access."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import duckdb


@dataclass(frozen=True)
class S3ConnectionConfig:
    """Configuration for enabling DuckDB HTTPFS with S3-compatible storage."""

    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    use_ssl: bool = False
    url_style: str = "path"
    session_token: str | None = None


def create_temporary_connection(database: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    """Create an isolated DuckDB connection.

    Args:
        database: Optional database path. Defaults to an in-memory connection.

    Returns:
        A connected ``DuckDBPyConnection`` instance.
    """

    db_path = ":memory:" if database is None else str(database)
    return duckdb.connect(database=db_path, read_only=False)


def configure_s3_access(connection: duckdb.DuckDBPyConnection, config: S3ConnectionConfig) -> None:
    """Enable HTTPFS in DuckDB and configure S3/MinIO access settings."""

    connection.execute("INSTALL httpfs;")
    connection.execute("LOAD httpfs;")

    connection.execute(f"SET s3_region='{config.region}';")

    endpoint = config.endpoint_url.strip()
    parsed = urlparse(endpoint)
    if parsed.scheme and parsed.netloc:
        endpoint = parsed.netloc
    connection.execute(f"SET s3_endpoint='{endpoint}';")

    connection.execute(f"SET s3_access_key_id='{config.access_key}';")
    connection.execute(f"SET s3_secret_access_key='{config.secret_key}';")
    if config.session_token:
        connection.execute(f"SET s3_session_token='{config.session_token}';")
    connection.execute(f"SET s3_url_style='{config.url_style}';")
    ssl_flag = "true" if config.use_ssl else "false"
    connection.execute(f"SET s3_use_ssl={ssl_flag};")


def execute_sql(connection: duckdb.DuckDBPyConnection, sql: str) -> duckdb.DuckDBPyRelation:
    """Execute a SQL statement and return the resulting relation."""

    if not sql.strip():
        raise ValueError("SQL must not be empty")
    return connection.execute(sql)


def copy_partitioned_parquet(
    connection: duckdb.DuckDBPyConnection,
    *,
    query: str,
    destination: str,
    partition_column: str = "dt",
    filename_pattern: str = "file_{uuid}",
    use_tmp_file: bool = False,
) -> None:
    """Run a query and copy results to partitioned Parquet files."""

    if not query.strip():
        raise ValueError("query must not be empty")
    if not destination:
        raise ValueError("destination is required")
    if not partition_column:
        raise ValueError("partition_column is required")

    options: list[str] = [
        "FORMAT parquet",
        f"PARTITION_BY ({partition_column})",
        f"FILENAME_PATTERN '{filename_pattern}'",
        "WRITE_PARTITION_COLUMNS false",
    ]
    if use_tmp_file:
        options.append("USE_TMP_FILE true")

    copy_sql = "COPY (" + query + f") TO '{destination}' (" + ", ".join(options) + ");"

    connection.execute(copy_sql)
