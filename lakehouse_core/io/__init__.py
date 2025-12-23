"""Orchestrator-agnostic IO helpers (URIs, paths, SQL templating, time)."""

from lakehouse_core.io.paths import (
    NonPartitionPaths,
    PartitionPaths,
    build_non_partition_paths,
    build_partition_paths,
)
from lakehouse_core.io.sql import (
    MissingTemplateVariableError,
    load_and_render_sql,
    load_sql,
    render_sql,
)
from lakehouse_core.io.time import (
    get_date_str,
    get_partition_date_str,
    normalize_to_partition_format,
)
from lakehouse_core.io.uri import (
    ParsedUri,
    join_uri,
    normalize_s3_key_prefix,
    parse_s3_uri,
    parse_uri,
    strip_slashes,
)

__all__ = [
    "MissingTemplateVariableError",
    "NonPartitionPaths",
    "ParsedUri",
    "PartitionPaths",
    "build_non_partition_paths",
    "build_partition_paths",
    "get_date_str",
    "get_partition_date_str",
    "normalize_to_partition_format",
    "join_uri",
    "load_and_render_sql",
    "load_sql",
    "normalize_s3_key_prefix",
    "parse_s3_uri",
    "parse_uri",
    "render_sql",
    "strip_slashes",
]
