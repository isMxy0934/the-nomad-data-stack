from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from lakehouse_core.catalog_migrations import (
    CATALOG_META_SCHEMA,
    CATALOG_MIGRATIONS_TABLE,
    apply_migrations,
)


def test_apply_migrations_records_history(tmp_path: Path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001.sql").write_text("CREATE SCHEMA IF NOT EXISTS ods;", encoding="utf-8")
    (migrations / "0002.sql").write_text("CREATE SCHEMA IF NOT EXISTS dwd;", encoding="utf-8")

    conn = duckdb.connect()
    applied = apply_migrations(conn, migrations_dir=migrations)
    assert applied == ["0001.sql", "0002.sql"]

    # Second run should be idempotent.
    applied_again = apply_migrations(conn, migrations_dir=migrations)
    assert applied_again == []

    count = conn.execute(
        f"SELECT COUNT(*) FROM {CATALOG_META_SCHEMA}.{CATALOG_MIGRATIONS_TABLE};"
    ).fetchone()[0]
    assert count == 2


def test_checksum_mismatch_fails_by_default(tmp_path: Path):
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    file_path = migrations / "0001.sql"
    file_path.write_text("CREATE SCHEMA IF NOT EXISTS ods;", encoding="utf-8")

    conn = duckdb.connect()
    apply_migrations(conn, migrations_dir=migrations)

    file_path.write_text("CREATE SCHEMA IF NOT EXISTS ods; -- changed", encoding="utf-8")
    with pytest.raises(ValueError, match="Checksum mismatch"):
        apply_migrations(conn, migrations_dir=migrations)
