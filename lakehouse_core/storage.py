from __future__ import annotations

from typing import Protocol


class ObjectStore(Protocol):
    """Object store abstraction using URI identifiers.

    Phase 1 convention: all methods accept and return URIs (e.g. ``s3://bucket/key``).
    Adapters are responsible for parsing/normalizing URIs for their backend.
    """

    def list_keys(self, prefix_uri: str) -> list[str]:
        """Return object keys (as URIs) under prefix_uri."""

    def exists(self, uri: str) -> bool:
        """Return True when object exists."""

    def read_bytes(self, uri: str) -> bytes:
        """Read object content as bytes."""

    def write_text(self, uri: str, text: str) -> None:
        """Write UTF-8 text to the target URI (overwrite)."""

    def write_bytes(self, uri: str, data: bytes) -> None:
        """Write bytes to the target URI (overwrite)."""

    def delete_objects(self, uris: list[str]) -> None:
        """Delete the listed object URIs (best-effort)."""

    def delete_prefix(self, prefix_uri: str) -> None:
        """Delete all objects whose URI starts with prefix_uri (prefix match)."""

    def copy_prefix(self, source_prefix_uri: str, dest_prefix_uri: str) -> None:
        """Copy all objects under source_prefix_uri to dest_prefix_uri (server-side when possible)."""
