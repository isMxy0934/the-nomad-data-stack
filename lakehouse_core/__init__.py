"""Stable core layer for the-nomad-data-stack.

This package must not depend on any specific orchestrator (Airflow/Prefect) or
storage implementation (S3/local). Integrations live in adapters.
"""

from lakehouse_core.api import cleanup_tmp, prepare_paths, publish_output, validate_output
from lakehouse_core.manifest import build_manifest
from lakehouse_core.paths import NonPartitionPaths, PartitionPaths
from lakehouse_core.uri import parse_s3_uri

__all__ = [
    "NonPartitionPaths",
    "PartitionPaths",
    "build_manifest",
    "cleanup_tmp",
    "parse_s3_uri",
    "prepare_paths",
    "publish_output",
    "validate_output",
]
