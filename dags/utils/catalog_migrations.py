"""DuckDB catalog migration utilities.

Migrations are SQL files stored in ``catalog/migrations`` and applied into a
persistent DuckDB database (the analysis catalog). This is metadata-only:
no data is materialized into DuckDB; views/macros point to S3/MinIO paths.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import duckdb

CATALOG_META_SCHEMA = "catalog_meta"
CATALOG_MIGRATIONS_TABLE = "schema_migrations"


@dataclass(frozen=True)
class AppliedMigration:
    filename: str
    checksum: str


def compute_checksum(sql: str) -> str:
    if not sql.strip():
        raise ValueError("Migration SQL must not be empty")
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def list_migration_files(migrations_dir: Path) -> list[Path]:
    if not migrations_dir.exists():
        return []
    if not migrations_dir.is_dir():
        raise ValueError(f"migrations_dir is not a directory: {migrations_dir}")

    return sorted([p for p in migrations_dir.iterdir() if p.is_file() and p.suffix == ".sql"])


def ensure_migrations_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_META_SCHEMA};")
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG_META_SCHEMA}.{CATALOG_MIGRATIONS_TABLE} (
          filename VARCHAR PRIMARY KEY,
          checksum VARCHAR NOT NULL,
          applied_at TIMESTAMP NOT NULL
        );
        """
    )


def load_applied_migrations(connection: duckdb.DuckDBPyConnection) -> dict[str, AppliedMigration]:
    ensure_migrations_table(connection)
    rows = connection.execute(
        f"SELECT filename, checksum FROM {CATALOG_META_SCHEMA}.{CATALOG_MIGRATIONS_TABLE};"
    ).fetchall()
    return {
        filename: AppliedMigration(filename=filename, checksum=checksum)
        for filename, checksum in rows
    }


def apply_migrations(
    connection: duckdb.DuckDBPyConnection,
    *,
    migrations_dir: Path,
    allow_checksum_mismatch: bool = False,
) -> list[str]:
    """Apply pending migrations and return the list of applied filenames."""

    ensure_migrations_table(connection)
    applied = load_applied_migrations(connection)
    migration_files = list_migration_files(migrations_dir)

    applied_now: list[str] = []
    for path in migration_files:
        filename = path.name
        sql = path.read_text(encoding="utf-8")
        if not sql.strip():
            raise ValueError(f"Empty migration file: {path}")
        checksum = compute_checksum(sql)

        existing = applied.get(filename)
        if existing:
            if existing.checksum != checksum and not allow_checksum_mismatch:
                raise ValueError(
                    f"Checksum mismatch for applied migration {filename}: "
                    f"{existing.checksum} != {checksum}"
                )
            continue

        connection.execute("BEGIN;")
        try:
            connection.execute(sql)
            connection.execute(
                f"""
                INSERT INTO {CATALOG_META_SCHEMA}.{CATALOG_MIGRATIONS_TABLE}
                (filename, checksum, applied_at)
                VALUES (?, ?, now());
                """,
                [filename, checksum],
            )
            connection.execute("COMMIT;")
        except Exception:
            connection.execute("ROLLBACK;")
            raise

        applied_now.append(filename)

    return applied_now
