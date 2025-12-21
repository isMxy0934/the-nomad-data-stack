from __future__ import annotations

from lakehouse_core.store.object_store import ObjectStore


def has_csv_under_prefix(store: ObjectStore, *, prefix_uri: str) -> bool:
    """Return True when there is at least one CSV object under prefix_uri."""

    uris = store.list_keys(prefix_uri)
    return any(uri.endswith(".csv") for uri in uris)


def register_csv_glob_as_temp_view(
    connection,
    *,
    view_name: str,
    csv_glob_uri: str,  # noqa: ANN001
) -> None:
    """Create or replace a DuckDB TEMP VIEW from a CSV glob URI."""

    if not view_name:
        raise ValueError("view_name is required")
    if not csv_glob_uri:
        raise ValueError("csv_glob_uri is required")

    connection.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT * FROM read_csv_auto('{csv_glob_uri}', hive_partitioning=false);
        """
    )
