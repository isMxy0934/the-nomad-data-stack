"""DuckDB execution helpers (orchestrator-agnostic).

Phase 2 goal: move execution semantics (connect/configure/copy) into core so
orchestrators only assemble inputs and call a stable API.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

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


@contextmanager
def temporary_connection(
    database: str | Path | None = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Context manager for an isolated DuckDB connection that always closes."""

    connection = create_temporary_connection(database=database)
    try:
        yield connection
    finally:
        connection.close()


def configure_s3_access(connection, config: S3ConnectionConfig) -> None:  # noqa: ANN001
    """Enable HTTPFS in DuckDB and configure S3/MinIO access settings."""

    # Prefer loading an already-installed extension to avoid network access in
    # restricted environments. Fall back to INSTALL only when LOAD fails.
    try:
        connection.execute("LOAD httpfs;")
    except Exception:  # noqa: BLE001
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


def normalize_duckdb_path(path_or_uri: str) -> str:
    """Normalize a destination path/URI for DuckDB.

    DuckDB accepts S3 URIs (via httpfs) but local paths are generally best passed
    as filesystem paths rather than ``file://`` URIs. This helper converts
    ``file://`` URIs into OS paths and leaves other schemes untouched.
    """

    value = (path_or_uri or "").strip()
    if not value:
        raise ValueError("path_or_uri is required")
    if value.startswith("s3://"):
        return value
    if value.startswith("file://"):
        parsed = urlparse(value)
        raw_path = unquote(parsed.path or "")
        local_path = url2pathname(raw_path)
        if local_path.startswith("/") and len(local_path) >= 3 and local_path[2] == ":":
            # Windows drive letter: /C:/path -> C:/path
            local_path = local_path[1:]
        return local_path
    return value


def ensure_local_destination_dir(destination: str) -> None:
    """Ensure local COPY destinations have a parent directory."""

    dest = (destination or "").strip()
    if not dest or dest.startswith("s3://") or "://" in dest:
        return
    Path(dest).mkdir(parents=True, exist_ok=True)


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
    dest = normalize_duckdb_path(destination)
    # For tmp S3 prefixes, allow overwriting to keep retries idempotent across
    # DuckDB versions that may partially write before raising.
    if dest.startswith("s3://") and "/_tmp/" in dest:
        options.append("OVERWRITE_OR_IGNORE true")
    if use_tmp_file:
        options.append("USE_TMP_FILE true")

    ensure_local_destination_dir(dest)
    copy_sql = "COPY (" + query + f") TO '{dest}' (" + ", ".join(options) + ");"
    try:
        connection.execute(copy_sql)
    except duckdb.NotImplementedException:
        # DuckDB compatibility: some versions don't support combining USE_TMP_FILE
        # with PARTITION_BY. Fall back to a normal partitioned COPY.
        if not use_tmp_file:
            raise
        fallback_options = [opt for opt in options if opt != "USE_TMP_FILE true"]
        fallback_sql = "COPY (" + query + f") TO '{dest}' (" + ", ".join(fallback_options) + ");"
        connection.execute(fallback_sql)


def copy_parquet(
    connection: duckdb.DuckDBPyConnection,
    *,
    query: str,
    destination: str,
    filename_pattern: str = "file_{uuid}",
    use_tmp_file: bool = False,
) -> None:
    """Run a query and copy results to Parquet files without partitioning."""

    if not query.strip():
        raise ValueError("query must not be empty")
    if not destination:
        raise ValueError("destination is required")

    options: list[str] = ["FORMAT parquet", f"FILENAME_PATTERN '{filename_pattern}'"]
    if use_tmp_file:
        options.append("USE_TMP_FILE true")

    dest = normalize_duckdb_path(destination)
    ensure_local_destination_dir(dest)
    copy_sql = "COPY (" + query + f") TO '{dest}' (" + ", ".join(options) + ");"
    connection.execute(copy_sql)


class Executor(Protocol):
    """Execution interface for running SQL to Parquet outputs."""

    def run_query_to_parquet(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        query: str,
        destination: str,
        filename_pattern: str = "file_{uuid}",
        use_tmp_file: bool = False,
    ) -> None: ...

    def run_query_to_partitioned_parquet(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        query: str,
        destination: str,
        partition_column: str = "dt",
        filename_pattern: str = "file_{uuid}",
        use_tmp_file: bool = False,
    ) -> None: ...


def run_query_to_parquet(
    connection: duckdb.DuckDBPyConnection,
    *,
    query: str,
    destination: str,
    filename_pattern: str = "file_{uuid}",
    use_tmp_file: bool = False,
) -> None:
    """Stable alias for writing non-partitioned Parquet outputs."""

    copy_parquet(
        connection,
        query=query,
        destination=destination,
        filename_pattern=filename_pattern,
        use_tmp_file=use_tmp_file,
    )


def run_query_to_partitioned_parquet(
    connection: duckdb.DuckDBPyConnection,
    *,
    query: str,
    destination: str,
    partition_column: str = "dt",
    filename_pattern: str = "file_{uuid}",
    use_tmp_file: bool = False,
) -> None:
    """Stable alias for writing partitioned Parquet outputs."""

    copy_partitioned_parquet(
        connection,
        query=query,
        destination=destination,
        partition_column=partition_column,
        filename_pattern=filename_pattern,
        use_tmp_file=use_tmp_file,
    )
