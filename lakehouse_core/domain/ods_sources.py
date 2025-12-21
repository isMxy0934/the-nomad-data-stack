from __future__ import annotations

from lakehouse_core.domain.input_views import has_csv_under_prefix, register_csv_glob_as_temp_view
from lakehouse_core.io.uri import join_uri, strip_slashes
from lakehouse_core.store.object_store import ObjectStore


def register_ods_csv_source_view(
    *,
    connection,  # noqa: ANN001
    store: ObjectStore,
    base_uri: str,
    source_path: str,
    partition_date: str,
    view_name: str,
) -> bool:
    """Register an ODS source CSV glob as a DuckDB temp view.

    Returns:
        True when a CSV exists and the view was created; False otherwise.
    """

    source_prefix = strip_slashes(source_path)
    prefix_uri = join_uri(base_uri, f"{source_prefix}/dt={partition_date}/")
    if not has_csv_under_prefix(store, prefix_uri=prefix_uri):
        return False

    glob_uri = join_uri(base_uri, f"{source_prefix}/dt={partition_date}/*.csv")
    register_csv_glob_as_temp_view(connection, view_name=view_name, csv_glob_uri=glob_uri)
    return True
