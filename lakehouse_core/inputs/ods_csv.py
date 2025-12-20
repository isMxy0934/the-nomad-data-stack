from __future__ import annotations

import logging

from lakehouse_core.inputs.base import InputRegistrar, InputRegistration
from lakehouse_core.models import RunSpec
from lakehouse_core.observability import log_event
from lakehouse_core.ods_sources import register_ods_csv_source_view
from lakehouse_core.storage import ObjectStore

logger = logging.getLogger(__name__)


class OdsCsvRegistrar(InputRegistrar):
    """Register ODS CSV source as a DuckDB temp view (tmp_{table})."""

    def register(
        self,
        *,
        spec: RunSpec,
        connection,
        store: ObjectStore,
        base_uri: str,
        partition_date: str,
    ) -> InputRegistration:
        if spec.layer != "ods":
            return InputRegistration(has_data=True)

        source = (spec.inputs or {}).get("source") or {}
        source_path = str(source.get("path") or "")
        fmt = str(source.get("format") or "").lower()
        if not source_path:
            raise ValueError(f"Missing ODS source path in RunSpec.inputs for {spec.layer}.{spec.table}")
        if fmt and fmt != "csv":
            raise ValueError(
                f"Unsupported ODS source format for {spec.layer}.{spec.table}: '{fmt}'"
            )
        if not partition_date:
            raise ValueError(f"partition_date is required for ODS table {spec.layer}.{spec.table}")

        view_name = f"tmp_{spec.table}"
        has_source_data = register_ods_csv_source_view(
            connection=connection,
            store=store,
            base_uri=base_uri,
            source_path=source_path,
            partition_date=partition_date,
            view_name=view_name,
        )
        log_event(
            logger,
            "inputs.ods_csv",
            layer=spec.layer,
            table=spec.table,
            dt=partition_date,
            status="ok" if has_source_data else "no_data",
            view=view_name,
            source_path=source_path,
        )
        return InputRegistration(
            has_data=bool(has_source_data),
            details={"view_name": view_name, "source_path": source_path, "format": "csv"},
        )

