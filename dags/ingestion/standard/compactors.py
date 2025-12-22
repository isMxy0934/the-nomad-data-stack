from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.ingestion.core.interfaces import BaseCompactor
from dags.utils.etl_utils import build_s3_connection_config
from lakehouse_core.domain.models import RunSpec
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.uri import join_uri
from lakehouse_core.pipeline import cleanup, commit, prepare
from lakehouse_core.store.stores import S3ObjectStore

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

        # Initialize ObjectStore via standard factory/hooks
        s3_hook = S3Hook(aws_conn_id="MINIO_S3")
        s3_config = build_s3_connection_config(s3_hook)
        self.store = S3ObjectStore(s3_config)

    def _commit_single_partition(
        self, df: pd.DataFrame, target: str, partition_val: str, run_id: str
    ) -> dict[str, Any]:
        """Runs the standard prepare -> load(write) -> commit -> cleanup pipeline."""

        # 1. Prepare Paths
        # We wrap the target in a virtual 'RunSpec'
        # prefix_template here is the base_prefix (e.g. lake/raw/daily/fund_etf)
        base_prefix = self.prefix_template.format(target=target)
        spec = RunSpec(
            layer="raw",
            table=target,
            base_prefix=base_prefix,
            is_partitioned=True,
        )

        paths_payload = prepare(
            spec=spec,
            run_id=run_id,
            store_namespace=self.bucket,
            partition_date=partition_val,
        )

        # We need the actual path objects for pipeline functions
        from lakehouse_core.io.paths import PartitionPaths

        paths = PartitionPaths(
            canonical_prefix=paths_payload["canonical_prefix"],
            tmp_prefix=paths_payload["tmp_prefix"],
            tmp_partition_prefix=paths_payload["tmp_partition_prefix"],
            manifest_path=paths_payload["manifest_path"],
            success_flag_path=paths_payload["success_flag_path"],
            partition_date=paths_payload["partition_date"],
        )

        # 2. Write to TMP (The 'Load' equivalent for in-memory DataFrames)
        # Ensure tmp is clean for idempotency
        self.store.delete_prefix(paths.tmp_prefix)

        filename = f"data.{self.file_format}"
        tmp_uri = join_uri(paths.tmp_partition_prefix, filename)

        if self.file_format == "csv":
            content = df.to_csv(index=False).encode("utf-8")
        else:
            content = df.to_parquet(index=False)

        self.store.put_object(tmp_uri, content)

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