from typing import Any, Dict, List, Optional
import pandas as pd
import json
import os
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dags.ingestion.core.interfaces import BaseCompactor

class StandardS3Compactor(BaseCompactor):
    """
    Standard compactor that writes DataFrames to S3 in CSV/Parquet format.
    Handles partitioning and SUCCESS flags.
    """
    def __init__(
        self, 
        bucket: str, 
        prefix_template: str, 
        file_format: str = "csv",
        dedup_cols: Optional[List[str]] = None
    ):
        """
        Args:
            bucket: S3 Bucket name.
            prefix_template: e.g. "lake/raw/daily/{target}/dt={date}"
            file_format: "csv" or "parquet".
            dedup_cols: Columns to use for drop_duplicates.
        """
        self.bucket = bucket
        self.prefix_template = prefix_template
        self.file_format = file_format
        self.dedup_cols = dedup_cols
        self.s3_hook = S3Hook(aws_conn_id="MINIO_S3") # Default connection

    def compact(
        self, 
        results: List[pd.DataFrame], 
        target: str, 
        partition_date: str, 
        **kwargs
    ) -> Dict[str, Any]:
        
        # 1. Merge
        valid_frames = [df for df in results if df is not None and not df.empty]
        if not valid_frames:
            print(f"No data to compact for {target} on {partition_date}")
            return {"row_count": 0, "status": "skipped"}
            
        merged_df = pd.concat(valid_frames, ignore_index=True)
        
        # 2. Dedupe
        if self.dedup_cols:
            before = len(merged_df)
            # If dedup columns are missing, skip or warn? 
            # We assume they exist for now.
            available_cols = [c for c in self.dedup_cols if c in merged_df.columns]
            if available_cols:
                merged_df = merged_df.drop_duplicates(subset=available_cols, keep="last")
            print(f"Deduped {before - len(merged_df)} rows.")

        # 3. Write
        # Construct path
        prefix = self.prefix_template.format(target=target, date=partition_date)
        # Ensure no double slashes
        prefix = prefix.strip("/")
        
        data_key = f"{prefix}/data.{self.file_format}"
        success_key = f"{prefix}/_SUCCESS"
        manifest_key = f"{prefix}/manifest.json"
        
        # To Byte Stream
        if self.file_format == "csv":
            out_bytes = merged_df.to_csv(index=False).encode("utf-8")
        elif self.file_format == "parquet":
            out_bytes = merged_df.to_parquet(index=False)
        else:
            raise ValueError(f"Unsupported format: {self.file_format}")
            
        # Atomic Write (Simulated by S3 overwrite)
        self.s3_hook.load_bytes(out_bytes, key=data_key, bucket_name=self.bucket, replace=True)
        
        # Write Meta
        manifest = {
            "target": target,
            "partition_date": partition_date,
            "row_count": len(merged_df),
            "generated_at": datetime.now().isoformat(),
            "columns": list(merged_df.columns)
        }
        self.s3_hook.load_string(json.dumps(manifest), key=manifest_key, bucket_name=self.bucket, replace=True)
        self.s3_hook.load_string("", key=success_key, bucket_name=self.bucket, replace=True)
        
        return {"row_count": len(merged_df), "path": f"s3://{self.bucket}/{data_key}"}
