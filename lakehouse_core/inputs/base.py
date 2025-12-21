from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from lakehouse_core.domain.models import RunSpec
from lakehouse_core.store.object_store import ObjectStore


@dataclass(frozen=True)
class InputRegistration:
    has_data: bool
    details: dict[str, Any] = field(default_factory=dict)


class InputRegistrar(Protocol):
    def register(
        self,
        *,
        spec: RunSpec,
        connection,
        store: ObjectStore,
        base_uri: str,
        partition_date: str,
    ) -> InputRegistration:
        """Register inputs for a RunSpec, returning whether source data exists."""
