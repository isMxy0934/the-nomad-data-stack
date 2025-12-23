import logging
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.ingestion.core.interfaces import BaseCompactor
from dags.utils.etl_utils import non_partition_paths_from_xcom, partition_paths_from_xcom, validate_dataset
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.paths import PartitionPaths, NonPartitionPaths
from lakehouse_core.io.uri import join_uri
from lakehouse_core.pipeline import cleanup, commit

logger = logging.getLogger(__name__)


class StandardS3Compactor(BaseCompactor):
    """
    Standard compactor that utilizes lakehouse_core.pipeline for atomic commits.
    Now supports structured task results from mapped extractors.
    """

    def __init__(
        self,
        bucket: str,
        prefix_template: str,
        file_format: str = "csv",
        dedup_cols: list[str] | None = None,
        partition_column: str | None = None,
    ):
        self.bucket = bucket
        self.prefix_template = prefix_template
        self.file_format = file_format
        self.dedup_cols = dedup_cols
        self.partition_column = partition_column
        
        # Initialize AirflowS3Store via S3Hook
        self.s3_hook = S3Hook(aws_conn_id="MINIO_S3")
        self.store = AirflowS3Store(self.s3_hook)

    def _get_paths_obj(self, paths_dict: dict) -> PartitionPaths | NonPartitionPaths:
        """Reconstruct paths object from serialized dict (using etl_utils for consistency)."""
        if bool(paths_dict.get("partitioned")):
            return partition_paths_from_xcom(paths_dict)
        return non_partition_paths_from_xcom(paths_dict)

    def compact(
        self, results: list[dict], target: str, partition_date: str, **kwargs
    ) -> dict[str, Any]:
        run_id = kwargs.get("run_id")
        paths_dict = kwargs.get("paths_dict")
        if not paths_dict:
            raise ValueError("paths_dict is required for StandardS3Compactor.compact")

        # 1. Load DataFrames from S3 URIs
        frames = []
        for res in results:
            uri = res.get("uri")
            if not uri:
                continue
            # Note: We assume Parquet for intermediate results
            from io import BytesIO
            content = self.store.read_bytes(uri)
            frames.append(pd.read_parquet(BytesIO(content)))

        if not frames:
            log_event(logger, "ingestion.compact.skip", target=target, reason="no_data")
            return {"row_count": 0, "status": "skipped"}

        merged_df = pd.concat(frames, ignore_index=True)

        if self.dedup_cols:
            available_cols = [c for c in self.dedup_cols if c in merged_df.columns]
            if available_cols:
                merged_df = merged_df.drop_duplicates(subset=available_cols, keep="last")

        # 2. Reconstruct Paths
        paths = self._get_paths_obj(paths_dict)

        # 3. Write Final Merged Data to TMP Partition
        # Use filename as data.{format}
        filename = f"data.{self.file_format}"
        # For ingestion, if it's partitioned, we write to tmp_partition_prefix
        # If not, we write to tmp_prefix
        target_prefix = getattr(paths, "tmp_partition_prefix", paths.tmp_prefix)
        final_uri = join_uri(target_prefix, filename)

        if self.file_format == "csv":
            content = merged_df.to_csv(index=False).encode("utf-8")
        else:
            content = merged_df.to_parquet(index=False)

        self.store.write_bytes(final_uri, content)

        # 4. Prepare load metrics (before commit)
        load_metrics = {
            "row_count": len(merged_df),
            "file_count": 1,
            "has_data": 1,
        }

        # 5. Validate load metrics (before commit, following standard pipeline)
        validated_metrics = validate_dataset(
            paths_dict=kwargs.get("paths_dict", {}),
            metrics=load_metrics,
            s3_hook=self.s3_hook,
        )
        logger.info(f"Validation passed: {validated_metrics}")

        # 6. Standard Commit via lakehouse_core
        publish_result, _ = commit(
            store=self.store,
            paths=paths,
            dest=target,
            run_id=run_id,
            partition_date=partition_date,
            metrics=validated_metrics,
        )

        # 7. Cleanup (core handled)
        cleanup(store=self.store, paths=paths)

        return publish_result

