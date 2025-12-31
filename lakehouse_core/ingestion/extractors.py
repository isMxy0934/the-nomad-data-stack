from __future__ import annotations

import importlib
import inspect
import logging
from collections.abc import Callable

import pandas as pd

from lakehouse_core.domain.observability import log_event
from lakehouse_core.ingestion.interfaces import BaseExtractor, IngestionJob

logger = logging.getLogger(__name__)


class SimpleFunctionExtractor(BaseExtractor):
    """Wrap a plain function that accepts **kwargs from job params."""

    def __init__(self, function_ref: str) -> None:
        self.function = self._resolve_callable(function_ref)

    def _resolve_callable(self, ref: str) -> Callable:
        if ":" not in ref:
            raise ValueError(f"Invalid reference: {ref}. Expected module:function")
        mod_name, func_name = ref.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, func_name)

    def extract(self, job: IngestionJob) -> pd.DataFrame | None:
        try:
            sig = inspect.signature(self.function)
            accepts_kwargs = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            )

            if accepts_kwargs:
                call_kwargs = job.params
            else:
                call_kwargs = {k: v for k, v in job.params.items() if k in sig.parameters}

            result = self.function(**call_kwargs)
            if hasattr(result, "csv_bytes"):
                from io import BytesIO

                return pd.read_csv(BytesIO(result.csv_bytes))
            return result
        except Exception as exc:  # noqa: BLE001
            log_event(logger, "ingestion.extract_failed", params=str(job.params), error=str(exc))
            raise


class AkShareExtractor(BaseExtractor):
    """Specialized extractor for AkShare with rename and date filtering."""

    def __init__(
        self,
        api_ref: str,
        rename_map: dict[str, str],
        date_column: str = "trade_date",
        cols: list[str] | None = None,
    ) -> None:
        self.api_ref = api_ref
        self.rename_map = rename_map
        self.date_column = date_column
        self.cols = cols

    def extract(self, job: IngestionJob) -> pd.DataFrame | None:
        import akshare as ak

        try:
            func_name = self.api_ref.split(".")[-1]
            api_func = getattr(ak, func_name)
            df = api_func(**job.params)
            if df is None or df.empty:
                log_event(logger, "ingestion.akshare_empty", api=func_name)
                return None

            df = df.rename(columns=self.rename_map)
            if self.cols:
                existing_cols = [c for c in self.cols if c in df.columns]
                df = df[existing_cols]

            start = job.params.get("start_date")
            end = job.params.get("end_date")
            if self.date_column in df.columns:
                df[self.date_column] = pd.to_datetime(df[self.date_column], errors="coerce")
                if start:
                    df = df[df[self.date_column] >= pd.Timestamp(start)]
                if end:
                    df = df[df[self.date_column] <= pd.Timestamp(end)]

            log_event(logger, "ingestion.akshare_ok", api=func_name, row_count=len(df))
            return df
        except Exception as exc:  # noqa: BLE001
            log_event(logger, "ingestion.akshare_failed", api=self.api_ref, error=str(exc))
            raise
