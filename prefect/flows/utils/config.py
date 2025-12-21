"""Prefect-side configuration helpers (env-first, Airflow-compat fallback)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from lakehouse_core.compute import S3ConnectionConfig


@dataclass(frozen=True)
class S3EnvConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    use_ssl: bool
    url_style: str
    session_token: str | None


def _parse_bool(value: str | None, *, default: bool | None = None) -> bool | None:
    if value is None:
        return default
    text = value.strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return default


def _load_airflow_s3_conn(env: dict[str, str]) -> S3EnvConfig | None:
    raw = env.get("AIRFLOW_CONN_MINIO_S3")
    if not raw:
        return None

    raw = raw.strip()
    if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1]

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:  # noqa: PERF203
        raise ValueError("AIRFLOW_CONN_MINIO_S3 must be valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValueError("AIRFLOW_CONN_MINIO_S3 must decode to a JSON object")

    login = str(payload.get("login") or "")
    password = str(payload.get("password") or "")
    extras = payload.get("extra") or {}
    if not isinstance(extras, dict):
        raise ValueError("AIRFLOW_CONN_MINIO_S3.extra must be a JSON object")

    endpoint = str(extras.get("endpoint_url") or extras.get("host") or "").strip()
    if not endpoint:
        raise ValueError("AIRFLOW_CONN_MINIO_S3.extra must include endpoint_url")

    use_ssl = extras.get("use_ssl")
    if use_ssl is None:
        use_ssl = endpoint.startswith("https")
    else:
        use_ssl = bool(use_ssl)

    return S3EnvConfig(
        endpoint_url=endpoint,
        access_key=login,
        secret_key=password,
        region=str(extras.get("region_name") or "us-east-1"),
        use_ssl=use_ssl,
        url_style=str(extras.get("s3_url_style") or "path"),
        session_token=str(extras.get("session_token") or "") or None,
    )


def _load_explicit_s3_env(env: dict[str, str]) -> S3EnvConfig | None:
    endpoint = env.get("S3_ENDPOINT_URL")
    access_key = env.get("S3_ACCESS_KEY_ID")
    secret_key = env.get("S3_SECRET_ACCESS_KEY")

    if not any([endpoint, access_key, secret_key]):
        return None

    if not endpoint or not access_key or not secret_key:
        raise ValueError(
            "S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, and S3_SECRET_ACCESS_KEY must all be set"
        )

    use_ssl = _parse_bool(env.get("S3_USE_SSL"))
    if use_ssl is None:
        use_ssl = endpoint.strip().startswith("https")

    return S3EnvConfig(
        endpoint_url=str(endpoint),
        access_key=str(access_key),
        secret_key=str(secret_key),
        region=str(env.get("S3_REGION") or "us-east-1"),
        use_ssl=bool(use_ssl),
        url_style=str(env.get("S3_URL_STYLE") or "path"),
        session_token=str(env.get("S3_SESSION_TOKEN") or "") or None,
    )


def build_s3_connection_config_from_env(
    env: dict[str, str] | None = None,
) -> S3ConnectionConfig:
    """Resolve S3 connection config for Prefect flows.

    Priority: explicit S3_* env vars, then AIRFLOW_CONN_MINIO_S3 fallback.
    """

    env = env or dict(os.environ)
    explicit = _load_explicit_s3_env(env)
    if explicit is not None:
        return S3ConnectionConfig(**explicit.__dict__)

    airflow_conn = _load_airflow_s3_conn(env)
    if airflow_conn is not None:
        return S3ConnectionConfig(**airflow_conn.__dict__)

    raise ValueError(
        "Missing S3 configuration: set S3_ENDPOINT_URL/S3_ACCESS_KEY_ID/"
        "S3_SECRET_ACCESS_KEY or AIRFLOW_CONN_MINIO_S3"
    )


def get_s3_bucket_name(env: dict[str, str] | None = None) -> str:
    env = env or dict(os.environ)
    return str(env.get("S3_BUCKET_NAME") or "stock-data")


def ensure_mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a dict")
    return value
