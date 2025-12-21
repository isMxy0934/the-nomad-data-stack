import json

import pytest

from flows.utils.config import build_s3_connection_config_from_env


def test_build_s3_connection_config_from_explicit_env() -> None:
    env = {
        "S3_ENDPOINT_URL": "https://minio.example.local:9000",
        "S3_ACCESS_KEY_ID": "access",
        "S3_SECRET_ACCESS_KEY": "secret",
        "S3_REGION": "us-west-2",
        "S3_USE_SSL": "true",
        "S3_URL_STYLE": "path",
        "S3_SESSION_TOKEN": "token",
    }

    config = build_s3_connection_config_from_env(env)

    assert config.endpoint_url == "https://minio.example.local:9000"
    assert config.access_key == "access"
    assert config.secret_key == "secret"
    assert config.region == "us-west-2"
    assert config.use_ssl is True
    assert config.url_style == "path"
    assert config.session_token == "token"


def test_build_s3_connection_config_from_airflow_conn() -> None:
    env = {
        "AIRFLOW_CONN_MINIO_S3": "'" + json.dumps(
            {
                "conn_type": "aws",
                "login": "minio",
                "password": "secret",
                "extra": {
                    "endpoint_url": "http://minio:9000",
                    "s3_url_style": "path",
                },
            }
        ) + "'"
    }

    config = build_s3_connection_config_from_env(env)

    assert config.endpoint_url == "http://minio:9000"
    assert config.access_key == "minio"
    assert config.secret_key == "secret"
    assert config.use_ssl is False
    assert config.url_style == "path"


def test_build_s3_connection_config_missing_env() -> None:
    env = {
        "S3_ENDPOINT_URL": "http://minio:9000",
        "S3_ACCESS_KEY_ID": "minio",
    }

    with pytest.raises(ValueError, match="S3_ENDPOINT_URL"):
        build_s3_connection_config_from_env(env)
