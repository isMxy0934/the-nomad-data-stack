from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class ParsedUri:
    scheme: str
    authority: str
    path: str


def parse_uri(uri: str) -> ParsedUri:
    if not uri:
        raise ValueError("uri is required")
    parsed = urlparse(uri)
    if not parsed.scheme:
        raise ValueError(f"URI missing scheme: {uri}")
    authority = parsed.netloc
    path = parsed.path.lstrip("/")
    return ParsedUri(scheme=parsed.scheme, authority=authority, path=path)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://`` URI into bucket and key components."""

    parsed = parse_uri(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {uri}")
    if not parsed.authority or not parsed.path:
        raise ValueError(f"S3 URI missing bucket or key: {uri}")
    return parsed.authority, parsed.path


def normalize_s3_key_prefix(key: str) -> str:
    """Normalize S3 key prefix for string-prefix semantics.

    - No directory inference: prefix is a raw string prefix.
    - Trailing slashes are ignored: ``a`` and ``a/`` behave the same.
    - Leading slashes are ignored: callers should not rely on ``/``-rooted keys.
    """

    value = (key or "").strip()
    value = value.lstrip("/").rstrip("/")
    if not value:
        raise ValueError("S3 key prefix is required")
    return value


def strip_slashes(value: str) -> str:
    return value.strip("/")


def join_uri(base_uri: str, key: str) -> str:
    base = base_uri.rstrip("/")
    k = key.lstrip("/")
    return f"{base}/{k}"
