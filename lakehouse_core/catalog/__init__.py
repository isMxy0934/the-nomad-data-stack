"""DuckDB analysis catalog helpers (metadata-only)."""

from lakehouse_core.catalog.attach import attach_catalog_if_available
from lakehouse_core.catalog.migrations import (
    CATALOG_META_SCHEMA,
    CATALOG_MIGRATIONS_TABLE,
    AppliedMigration,
    apply_migrations,
    compute_checksum,
    ensure_migrations_table,
    list_migration_files,
    load_applied_migrations,
)
from lakehouse_core.catalog.sql import (
    LayerSpec,
    build_layer_dt_macro_sql,
    build_layer_view_sql,
    build_schema_sql,
    normalize_prefix,
    quote_ident,
)

__all__ = [
    "AppliedMigration",
    "CATALOG_META_SCHEMA",
    "CATALOG_MIGRATIONS_TABLE",
    "LayerSpec",
    "apply_migrations",
    "attach_catalog_if_available",
    "build_layer_dt_macro_sql",
    "build_layer_view_sql",
    "build_schema_sql",
    "compute_checksum",
    "ensure_migrations_table",
    "list_migration_files",
    "load_applied_migrations",
    "normalize_prefix",
    "quote_ident",
]
