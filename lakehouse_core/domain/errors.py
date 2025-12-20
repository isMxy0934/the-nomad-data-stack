from __future__ import annotations


class LakehouseCoreError(Exception):
    """Base error for lakehouse_core."""


class ValidationError(LakehouseCoreError):
    """Raised when dataset validation fails."""


class PlanningError(LakehouseCoreError):
    """Raised when planning (RunSpec generation) fails."""

