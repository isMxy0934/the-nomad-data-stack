#!/usr/bin/env python3
"""Validation script for catalog migrations (no MinIO required)."""

from __future__ import annotations

from pathlib import Path

import duckdb

from dags.utils.catalog_migrations import (
    CATALOG_META_SCHEMA,
    CATALOG_MIGRATIONS_TABLE,
    apply_migrations,
)


def main() -> int:
    migrations_dir = Path("catalog/migrations")
    conn = duckdb.connect()
    apply_migrations(conn, migrations_dir=migrations_dir)

    conn.execute(f"SELECT COUNT(*) FROM {CATALOG_META_SCHEMA}.{CATALOG_MIGRATIONS_TABLE};")
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
