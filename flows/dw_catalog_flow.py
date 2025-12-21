"""Prefect flow to maintain the DuckDB analysis catalog (metadata-only)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from prefect import flow, task

from flows.dw_layer_flow import dw_layer_flow
from flows.utils.etl_utils import build_s3_connection_config
from lakehouse_core.catalog import apply_migrations
from lakehouse_core.compute import configure_s3_access, create_temporary_connection

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = Path(
    os.getenv("DUCKDB_CATALOG_PATH", str(REPO_ROOT / ".duckdb" / "catalog.duckdb"))
)
DEFAULT_MIGRATIONS_DIR = Path(
    os.getenv("DUCKDB_CATALOG_MIGRATIONS_DIR", str(REPO_ROOT / "catalog" / "migrations"))
)


@task
def migrate_catalog(*, catalog_path: str, migrations_dir: str, **kwargs: Any) -> list[str]:
    catalog = Path(catalog_path)
    catalog.parent.mkdir(parents=True, exist_ok=True)

    s3_config = build_s3_connection_config()

    conn = create_temporary_connection(database=catalog)
    try:
        configure_s3_access(conn, s3_config)
        return apply_migrations(conn, migrations_dir=Path(migrations_dir))
    finally:
        conn.close()


@flow(name="dw_catalog_flow")
def dw_catalog_flow(run_conf: dict[str, Any] | None = None) -> None:
    migrate_catalog.submit(
        catalog_path=str(DEFAULT_CATALOG_PATH),
        migrations_dir=str(DEFAULT_MIGRATIONS_DIR),
    ).result()
    dw_layer_flow(layer="ods", run_conf=run_conf or {})


if __name__ == "__main__":
    dw_catalog_flow()
