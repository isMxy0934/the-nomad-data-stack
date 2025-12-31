from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from lakehouse_core.compute import S3ConnectionConfig


@dataclass(frozen=True)
class S3Settings:
    bucket: str | None
    endpoint_url: str | None
    access_key: str | None
    secret_key: str | None
    region: str
    url_style: str
    use_ssl: bool
    session_token: str | None = None


def _parse_airflow_conn(raw: str) -> dict[str, Any]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def resolve_s3_settings() -> S3Settings:
    bucket = os.getenv("S3_BUCKET_NAME")
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("S3_REGION", "us-east-1")
    url_style = os.getenv("S3_URL_STYLE", "path")
    session_token = os.getenv("AWS_SESSION_TOKEN")

    airflow_conn = os.getenv("AIRFLOW_CONN_MINIO_S3")
    if airflow_conn:
        conn = _parse_airflow_conn(airflow_conn)
        extra = conn.get("extra") or {}
        endpoint_url = endpoint_url or extra.get("endpoint_url")
        access_key = access_key or conn.get("login")
        secret_key = secret_key or conn.get("password")

    use_ssl_env = os.getenv("S3_USE_SSL")
    use_ssl = False
    if use_ssl_env is not None:
        use_ssl = use_ssl_env.strip().lower() in {"1", "true", "yes"}
    elif endpoint_url:
        use_ssl = endpoint_url.strip().lower().startswith("https://")

    return S3Settings(
        bucket=bucket,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        url_style=url_style,
        use_ssl=use_ssl,
        session_token=session_token,
    )


def build_s3_connection_config(settings: S3Settings) -> S3ConnectionConfig | None:
    if not settings.endpoint_url or not settings.access_key or not settings.secret_key:
        return None
    return S3ConnectionConfig(
        endpoint_url=settings.endpoint_url,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        region=settings.region,
        use_ssl=settings.use_ssl,
        url_style=settings.url_style,
        session_token=settings.session_token,
    )
