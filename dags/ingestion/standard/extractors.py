import importlib
import inspect
import logging
from collections.abc import Callable

import pandas as pd

from dags.ingestion.core.interfaces import BaseExtractor, IngestionJob
from lakehouse_core.domain.observability import log_event

logger = logging.getLogger(__name__)

class SimpleFunctionExtractor(BaseExtractor):
    """
    A generic extractor that wraps a simple Python function.
    Useful for migrating existing functional code.
    The function should accept **kwargs matching the job params.
    """
    def __init__(self, function_ref: str):
        self.function = self._resolve_callable(function_ref)

    def _resolve_callable(self, ref: str) -> Callable:
        if ":" not in ref:
            raise ValueError(f"Invalid reference: {ref}. Expected module:function")
        mod_name, func_name = ref.split(":", 1)
        mod = importlib.import_module(mod_name)
        return getattr(mod, func_name)

    def extract(self, job: IngestionJob) -> pd.DataFrame | None:
        # Call the function with unpacked params
        # We smartly filter params to only what the function accepts
        try:
            sig = inspect.signature(self.function)

            # Check if function accepts **kwargs
            accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

            call_kwargs = {}
            if accepts_kwargs:
                call_kwargs = job.params
            else:
                # Only pass args that exist in signature
                for k, v in job.params.items():
                    if k in sig.parameters:
                        call_kwargs[k] = v

            result = self.function(**call_kwargs)

            # Adapt legacy return types if necessary
            if hasattr(result, "csv_bytes"): # CsvPayload support for legacy
                 from io import BytesIO
                 return pd.read_csv(BytesIO(result.csv_bytes))

            return result
        except Exception as e:
            log_event(logger, "Extraction failed", params=str(job.params), error=str(e))
            raise

class AkShareExtractor(BaseExtractor):
    """
    A specialized extractor for AkShare that handles common patterns:
    - Column renaming (CN -> EN)
    - Date filtering (start_date/end_date)
    - Standardizing trade_date column
    """
    def __init__(
        self,
        api_ref: str,
        rename_map: dict[str, str],
        date_column: str = "trade_date",
        cols: list[str] | None = None
    ):
        """
        Args:
            api_ref: "akshare.fund_etf_hist_em"
            rename_map: {"日期": "trade_date", "收盘": "close"}
            date_column: The name of the date column *after* renaming.
            cols: List of columns to keep.
        """
        self.api_ref = api_ref
        self.rename_map = rename_map
        self.date_column = date_column
        self.cols = cols

    def extract(self, job: IngestionJob) -> pd.DataFrame | None:
        import akshare as ak

        try:
            # 1. Resolve API function
            func_name = self.api_ref.split(".")[-1]
            api_func = getattr(ak, func_name)

            # 2. Call API
            call_kwargs = {}
            call_kwargs.update(job.params)

            # log_event(logger, "Calling AkShare API", api=func_name, params=str(call_kwargs))

            df = api_func(**call_kwargs)

            if df is None or df.empty:
                log_event(logger, "AkShare returned empty", api=func_name)
                return None

            # 3. Rename
            df = df.rename(columns=self.rename_map)

            # 4. Filter Columns
            if self.cols:
                existing_cols = [c for c in self.cols if c in df.columns]
                df = df[existing_cols]

            # 5. Filter Date Range
            start = job.params.get("start_date")
            end = job.params.get("end_date")

            if self.date_column in df.columns:
                df[self.date_column] = pd.to_datetime(df[self.date_column], errors="coerce")

                if start:
                    df = df[df[self.date_column] >= pd.Timestamp(start)]
                if end:
                    df = df[df[self.date_column] <= pd.Timestamp(end)]

            log_event(logger, "AkShare extraction success", api=func_name, row_count=len(df))
            return df

        except Exception as e:
            log_event(logger, "AkShare extraction failed", api=self.api_ref, error=str(e))
            raise
