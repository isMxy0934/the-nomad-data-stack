from __future__ import annotations

from typing import Protocol

from lakehouse_core.models import RunContext, RunSpec


class Planner(Protocol):
    """Planner protocol: translate user table definitions into executable RunSpecs."""

    def build(self, context: RunContext) -> list[RunSpec]:
        ...

