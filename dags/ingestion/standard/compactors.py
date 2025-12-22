from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.ingestion.core.interfaces import BaseCompactor
from lakehouse_core.api import prepare_paths
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.uri import join_uri
from lakehouse_core.pipeline import cleanup, commit

logger = logging.getLogger(__name__)


class StandardS3Compactor(BaseCompactor):
    """
    Standard compactor that utilizes lakehouse_core.pipeline for atomic commits.
    """

    def __init__(
        self,
        bucket: str,
        prefix_template: str,
        file_format: str = "csv",
        dedup_cols: list[str] | None = None,
        partition_column: str | None = None,
    ):
        """
        Args:
            bucket: S3 Bucket name (store namespace).
            prefix_template: e.g. "lake/raw/daily/{target}" (Note: dt={date} is handled by pipeline)
            file_format: "csv" or "parquet".
            dedup_cols: Columns to use for drop_duplicates.
            partition_column: If set, splits data and commits multiple partitions.
        """
        self.bucket = bucket
        self.prefix_template = prefix_template
        self.file_format = file_format
        self.dedup_cols = dedup_cols
        self.partition_column = partition_column

        # Initialize AirflowS3Store via S3Hook
        s3_hook = S3Hook(aws_conn_id="MINIO_S3")
        self.store = AirflowS3Store(s3_hook)

    def _commit_single_partition(
        self, df: pd.DataFrame, target: str, partition_val: str, run_id: str
    ) -> dict[str, Any]:
        """Runs the standard prepare -> load(write) -> commit -> cleanup pipeline."""

        # 1. Prepare Paths (Directly via API to avoid RunSpec validation)
        base_prefix = self.prefix_template.format(target=target)

        # We bypass the 'prepare' pipeline function because it requires a RunSpec
        # and RunSpec has strict validation on base_prefix.
        paths = prepare_paths(
            base_prefix=base_prefix,
            run_id=run_id,
            partition_date=partition_val,
            is_partitioned=True,
            store_namespace=self.bucket,
        )

        # 2. Write to TMP
        self.store.delete_prefix(paths.tmp_prefix)

        filename = f"data.{self.file_format}"
        tmp_uri = join_uri(paths.tmp_partition_prefix, filename)

        if self.file_format == "csv":
            content = df.to_csv(index=False).encode("utf-8")
        else:
            content = df.to_parquet(index=False)

        self.store.write_bytes(tmp_uri, content)

        # 3. Commit
        metrics = {
            "row_count": len(df),
            "file_count": 1,
            "has_data": 1,
        }

        publish_result, _ = commit(
            store=self.store,
            paths=paths,
            dest=target,
            run_id=run_id,
            partition_date=partition_val,
            metrics=metrics,
        )

        # 4. Cleanup
        cleanup(store=self.store, paths=paths)

        return publish_result

    def compact(
        self, results: list[pd.DataFrame], target: str, partition_date: str, **kwargs
    ) -> dict[str, Any]:
        run_id = kwargs.get("run_id") or f"ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 1. Merge & Dedupe
        valid_frames = [df for df in results if df is not None and not df.empty]
        if not valid_frames:
            log_event(logger, "ingestion.compact.skip", target=target, reason="no_data")
            return {"row_count": 0, "status": "skipped"}

        merged_df = pd.concat(valid_frames, ignore_index=True)

        if self.dedup_cols:
            available_cols = [c for c in self.dedup_cols if c in merged_df.columns]
            if available_cols:
                merged_df = merged_df.drop_duplicates(subset=available_cols, keep="last")

        # 2. Partition & Pipeline Commit
        summary = []
        if self.partition_column and self.partition_column in merged_df.columns:
            # Data-driven partitioning
            if pd.api.types.is_datetime64_any_dtype(merged_df[self.partition_column]):
                merged_df[self.partition_column] = merged_df[self.partition_column].dt.strftime(
                    "%Y-%m-%d"
                )

            for val, group in merged_df.groupby(self.partition_column):
                val_str = str(val)
                if not val_str or val_str.lower() in ("nan", "nat"):
                    continue
                res = self._commit_single_partition(group, target, val_str, run_id)
                summary.append(res)
        else:
            # DAG-driven partitioning
            res = self._commit_single_partition(merged_df, target, partition_date, run_id)
            summary.append(res)

        return {
            "target": target,
            "row_count": len(merged_df),
            "partition_count": len(summary),
            "status": "success",
        }
