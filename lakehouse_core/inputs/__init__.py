from __future__ import annotations

from lakehouse_core.inputs.base import InputRegistrar, InputRegistration
from lakehouse_core.inputs.ods_csv import OdsCsvRegistrar

__all__ = ["InputRegistrar", "InputRegistration", "OdsCsvRegistrar"]

