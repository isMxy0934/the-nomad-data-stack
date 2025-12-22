import importlib
from typing import Any, Dict, Optional, Callable
import pandas as pd
from datetime import date

from dags.ingestion.core.interfaces import BaseExtractor, IngestionJob

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

    def extract(self, job: IngestionJob) -> Optional[pd.DataFrame]:
        # Call the function with unpacked params
        # Note: We filter params to only what the function accepts? 
        # For simplicity, we assume the function accepts **kwargs or the specific keys provided by partitioner.
        try:
            result = self.function(**job.params)
            
            # Adapt legacy return types if necessary
            # Some legacy functions return CsvPayload, some return DataFrame.
            # This extractor assumes the function has been refactored to return DataFrame or None.
            if hasattr(result, "csv_bytes"): # CsvPayload support for legacy
                 from io import BytesIO
                 return pd.read_csv(BytesIO(result.csv_bytes))
            
            return result
        except Exception as e:
            print(f"Extraction failed for {job.params}: {e}")
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
        rename_map: Dict[str, str],
        date_column: str = "trade_date",
        cols: Optional[List[str]] = None
    ):
        """
        Args:
            api_ref: "akshare.fund_etf_hist_em" (can be string ref or we import akshare inside)
            rename_map: {"日期": "trade_date", "收盘": "close"}
            date_column: The name of the date column *after* renaming.
            cols: List of columns to keep.
        """
        self.api_ref = api_ref
        self.rename_map = rename_map
        self.date_column = date_column
        self.cols = cols

    def extract(self, job: IngestionJob) -> Optional[pd.DataFrame]:
        import akshare as ak
        
        # 1. Resolve API function
        # Simple lookup in akshare module
        func_name = self.api_ref.split(".")[-1]
        api_func = getattr(ak, func_name)
        
        # 2. Call API
        # We need to map standard job params (start_date, end_date, symbol) to AkShare specific args
        # This is the tricky part: AkShare args vary (symbol vs code, start_date vs period).
        # We can pass job.params directly, but might need mapping.
        # For now, assume params match or are handled by **kwargs in API (mostly true for symbol).
        # Dates are usually required strings.
        
        call_kwargs = {}
        call_kwargs.update(job.params)
        
        # AkShare usually wants 'start_date' as 'YYYYMMDD' or 'YYYY-MM-DD'
        # We assume params are already strings.
        
        df = api_func(**call_kwargs)
        
        if df is None or df.empty:
            return None
            
        # 3. Rename
        df = df.rename(columns=self.rename_map)
        
        # 4. Filter Columns
        if self.cols:
            existing_cols = [c for c in self.cols if c in df.columns]
            df = df[existing_cols]
            
        # 5. Filter Date Range (Client-side filtering)
        # Because many AkShare APIs ignore start/end and return full history
        start = job.params.get("start_date")
        end = job.params.get("end_date")
        
        if self.date_column in df.columns:
            # Ensure it's datetime
            df[self.date_column] = pd.to_datetime(df[self.date_column], errors="coerce")
            
            if start:
                df = df[df[self.date_column] >= pd.Timestamp(start)]
            if end:
                df = df[df[self.date_column] <= pd.Timestamp(end)]
        
        return df
