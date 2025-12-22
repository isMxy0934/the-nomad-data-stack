import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.ingestion.core.interfaces import BaseCompactor
from lakehouse_core.domain.observability import log_event

logger = logging.getLogger(__name__)

class StandardS3Compactor(BaseCompactor):
    """
    Standard compactor that writes DataFrames to S3 in CSV/Parquet format.
    Handles partitioning (by DAG date or data column) and atomic writes.
    """
    def __init__(
        self,
        bucket: str,
        prefix_template: str,
        file_format: str = "csv",
        dedup_cols: list[str] | None = None,
        partition_column: str | None = None
    ):
        """
        Args:
            bucket: S3 Bucket name.
            prefix_template: e.g. "lake/raw/daily/{target}/dt={date}"
                             Must contain '{date}' placeholder.
            file_format: "csv" or "parquet".
            dedup_cols: Columns to use for drop_duplicates.
            partition_column: If set, data will be split by this column value.
                              The '{date}' in prefix_template will be replaced by the value in this column.
                              If None, '{date}' is replaced by the DAG execution date.
        """
        self.bucket = bucket
        self.prefix_template = prefix_template
        self.file_format = file_format
        self.dedup_cols = dedup_cols
        self.partition_column = partition_column
        self.s3_hook = S3Hook(aws_conn_id="MINIO_S3")

    def _write_single_partition(self, df: pd.DataFrame, target: str, partition_val: str):
        """Helper to write one dataframe to one partition path."""
        prefix = self.prefix_template.format(target=target, date=partition_val)
        prefix = prefix.strip("/")

        data_key = f"{prefix}/data.{self.file_format}"
        success_key = f"{prefix}/_SUCCESS"
        manifest_key = f"{prefix}/manifest.json"

        if self.file_format == "csv":
            out_bytes = df.to_csv(index=False).encode("utf-8")
        elif self.file_format == "parquet":
            out_bytes = df.to_parquet(index=False)
        else:
            raise ValueError(f"Unsupported format: {self.file_format}")

        self.s3_hook.load_bytes(out_bytes, key=data_key, bucket_name=self.bucket, replace=True)

        manifest = {
            "target": target,
            "partition_date": partition_val,
            "row_count": len(df),
            "generated_at": datetime.now().isoformat(),
            "columns": list(df.columns)
        }
        self.s3_hook.load_string(json.dumps(manifest), key=manifest_key, bucket_name=self.bucket, replace=True)
        self.s3_hook.load_string("", key=success_key, bucket_name=self.bucket, replace=True)

        return f"s3://{self.bucket}/{data_key}"

    def compact(
        self,
        results: list[pd.DataFrame],
        target: str,
        partition_date: str,
        **kwargs
    ) -> dict[str, Any]:

        # 1. Merge
        valid_frames = [df for df in results if df is not None and not df.empty]
        if not valid_frames:
            log_event(logger, "No data to compact", target=target)
            return {"row_count": 0, "status": "skipped"}

        merged_df = pd.concat(valid_frames, ignore_index=True)

        # 2. Dedupe
        if self.dedup_cols:
            # Drop dedup logic is same
            before = len(merged_df)
            available_cols = [c for c in self.dedup_cols if c in merged_df.columns]
            if available_cols:
                merged_df = merged_df.drop_duplicates(subset=available_cols, keep="last")

            dropped_count = before - len(merged_df)
            log_event(logger, "Deduplication finished", before=before, after=len(merged_df), dropped=dropped_count)

        # 3. Partition & Write
        paths = []

        if self.partition_column:
            if self.partition_column not in merged_df.columns:
                 # Fallback or error? Let's error to be safe.
                 # Or warn and dump to default partition?
                 # Error is better to prevent data pollution.
                 raise ValueError(f"Partition column '{self.partition_column}' not found in data.")

            # Ensure it's string-like for grouping
            # If it's date/datetime, convert to YYYY-MM-DD string
            if pd.api.types.is_datetime64_any_dtype(merged_df[self.partition_column]):
                 merged_df[self.partition_column] = merged_df[self.partition_column].dt.strftime("%Y-%m-%d")

            # GroupBy
            for val, group in merged_df.groupby(self.partition_column):
                val_str = str(val)
                # Skip NaNs
                if not val_str or val_str.lower() == "nan" or val_str.lower() == "nat":
                    continue
                path = self._write_single_partition(group, target, val_str)
                paths.append(path)

        else:
            # Default Mode: Use the DAG provided partition_date
            path = self._write_single_partition(merged_df, target, partition_date)
            paths.append(path)

        return {"row_count": len(merged_df), "partition_count": len(paths), "paths": paths}
