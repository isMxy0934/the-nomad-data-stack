"""Airflow-specific configuration adapters.

This module isolates Airflow connection format conversions from core business logic.
"""

from __future__ import annotations

from typing import Any

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from lakehouse_core.compute import S3ConnectionConfig


def _parse_bool(value: Any, default: bool = True) -> bool:
    """Parse boolean value from string, bool, or other types.

    Handles string representations of boolean values that Airflow
    connections might store in extra fields.

    Args:
        value: Value to parse (can be bool, str, int, or None)
        default: Default value if value is None

    Returns:
        Parsed boolean value
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() not in ("false", "0", "no", "off", "")
    return bool(value) if value is not None else default


def build_s3_connection_config(s3_hook: S3Hook) -> S3ConnectionConfig:
    """Construct DuckDB S3 settings from an Airflow connection.

    This adapter converts Airflow's S3Hook connection configuration to
    lakehouse_core's S3ConnectionConfig format, isolating Airflow-specific
    logic from the core library.

    Args:
        s3_hook: Airflow S3Hook with connection configuration

    Returns:
        S3ConnectionConfig for DuckDB S3 access

    Raises:
        ValueError: If S3 connection is missing endpoint_url
    """
    connection: Connection = s3_hook.get_connection(s3_hook.aws_conn_id)
    extras = connection.extra_dejson or {}

    endpoint = extras.get("endpoint_url") or extras.get("host")
    if not endpoint:
        raise ValueError("S3 connection must define endpoint_url")

    url_style = extras.get("s3_url_style", "path")
    region = extras.get("region_name", "us-east-1")
    use_ssl = _parse_bool(extras.get("use_ssl"), default=endpoint.startswith("https"))

    return S3ConnectionConfig(
        endpoint_url=endpoint,
        access_key=connection.login or "",
        secret_key=connection.password or "",
        region=region,
        use_ssl=use_ssl,
        url_style=url_style,
        session_token=extras.get("session_token"),
    )

