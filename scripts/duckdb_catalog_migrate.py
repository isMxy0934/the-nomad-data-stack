#!/usr/bin/env python3
"""Apply catalog migrations into the persistent DuckDB analysis catalog."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakehouse_core.catalog_migrations import apply_migrations
from lakehouse_core.execution import create_temporary_connection

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply DuckDB catalog migrations (metadata-only).")
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=Path(".duckdb/catalog.duckdb"),
        help="Path to the persistent DuckDB catalog database file.",
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=Path("catalog/migrations"),
        help="Directory containing catalog migration SQL files.",
    )
    parser.add_argument(
        "--allow-checksum-mismatch",
        action="store_true",
        help="Do not fail if an already-applied migration file content changed.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG/INFO/WARNING/ERROR).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s"
    )

    args.catalog_path.parent.mkdir(parents=True, exist_ok=True)

    conn = create_temporary_connection(database=args.catalog_path)
    applied = apply_migrations(
        conn,
        migrations_dir=args.migrations_dir,
        allow_checksum_mismatch=bool(args.allow_checksum_mismatch),
    )

    if applied:
        logger.info("Applied %d migrations: %s", len(applied), ", ".join(applied))
    else:
        logger.info("No pending migrations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
