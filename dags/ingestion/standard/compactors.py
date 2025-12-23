import logging
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.ingestion.core.interfaces import BaseCompactor
from dags.utils.etl_utils import (
    non_partition_paths_from_xcom,
    partition_paths_from_xcom,
    validate_dataset,
)
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths, build_partition_paths
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

    def _extract_base_prefix(self, paths_dict: dict) -> str:
        """Extract base prefix from paths_dict by removing bucket and dt= suffix."""
        canonical_prefix = paths_dict.get("canonical_prefix", "")
        # canonical_prefix format: s3://bucket/lake/raw/daily/target/dt=2024-01-01
        # We need: lake/raw/daily/target
        # Remove s3://bucket/
        parts = canonical_prefix.split("/", 3)  # ["s3:", "", "bucket", "rest..."]
        if len(parts) >= 4:
            rest = parts[3]  # "lake/raw/daily/target/dt=2024-01-01"
            # Remove /dt=XXXXX if present
            if "/dt=" in rest:
                return rest.rsplit("/dt=", 1)[0]
            return rest
        return canonical_prefix

    def _compact_single_partition(
        self,
        df: pd.DataFrame,
        paths: PartitionPaths | NonPartitionPaths,
        target: str,
        partition_date: str,
        run_id: str,
    ) -> dict[str, Any]:
        """Compact a single partition (helper method)."""
        # Write data to tmp partition
        filename = f"data.{self.file_format}"
        target_prefix = getattr(paths, "tmp_partition_prefix", paths.tmp_prefix)
        final_uri = join_uri(target_prefix, filename)

        if self.file_format == "csv":
            content = df.to_csv(index=False).encode("utf-8")
        else:
            content = df.to_parquet(index=False)

        self.store.write_bytes(final_uri, content)

        # Prepare load metrics
        load_metrics = {
            "row_count": len(df),
            "file_count": 1,
            "has_data": 1,
        }

        # Convert paths to dict for validate_dataset
        if isinstance(paths, PartitionPaths):
            paths_dict = {
                "partitioned": True,
                "partition_date": paths.partition_date,
                "canonical_prefix": paths.canonical_prefix,
                "tmp_prefix": paths.tmp_prefix,
                "tmp_partition_prefix": paths.tmp_partition_prefix,
                "manifest_path": paths.manifest_path,
                "success_flag_path": paths.success_flag_path,
            }
        else:
            paths_dict = {
                "partitioned": False,
                "canonical_prefix": paths.canonical_prefix,
                "tmp_prefix": paths.tmp_prefix,
                "manifest_path": paths.manifest_path,
                "success_flag_path": paths.success_flag_path,
            }

        # Validate load metrics
        validated_metrics = validate_dataset(
            paths_dict=paths_dict,
            metrics=load_metrics,
            s3_hook=self.s3_hook,
            file_format=self.file_format,
        )
        logger.info(f"Validation passed for partition {partition_date}: {validated_metrics}")

        # Commit
        publish_result, _ = commit(
            store=self.store,
            paths=paths,
            dest=target,
            run_id=run_id,
            partition_date=partition_date,
            metrics=validated_metrics,
        )

        # Cleanup
        cleanup(store=self.store, paths=paths)

        return publish_result

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

        # 2. Check if we need to partition by partition_column
        if (
            self.partition_column
            and self.partition_column in merged_df.columns
            and bool(paths_dict.get("partitioned"))
        ):
            # Group by partition_column and commit each partition separately
            logger.info(
                f"Partitioning data by column '{self.partition_column}' "
                f"into {merged_df[self.partition_column].nunique()} unique partitions"
            )

            base_prefix = self._extract_base_prefix(paths_dict)
            publish_results = []
            total_row_count = 0

            for partition_value, partition_df in merged_df.groupby(self.partition_column):
                # Convert partition value to string (handle date/datetime)
                if pd.api.types.is_datetime64_any_dtype(partition_df[self.partition_column]):
                    partition_date_str = partition_value.strftime("%Y-%m-%d")
                else:
                    partition_date_str = str(partition_value)

                logger.info(
                    f"Processing partition: {self.partition_column}={partition_date_str} "
                    f"({len(partition_df)} rows)"
                )

                # Build new PartitionPaths for this specific partition
                partition_paths = build_partition_paths(
                    base_uri=f"s3://{self.bucket}",
                    base_prefix=base_prefix,
                    partition_date=partition_date_str,
                    run_id=run_id,
                )

                # Compact this partition
                result = self._compact_single_partition(
                    df=partition_df,
                    paths=partition_paths,
                    target=target,
                    partition_date=partition_date_str,
                    run_id=run_id,
                )
                publish_results.append(result)
                total_row_count += len(partition_df)

            # Return combined result
            return {
                "row_count": total_row_count,
                "partition_count": len(publish_results),
                "status": "partitioned",
                "partitions": publish_results,
            }

        # 3. Reconstruct Paths (single partition mode)
        paths = self._get_paths_obj(paths_dict)

        # 4. Write Final Merged Data to TMP Partition
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

        # 5. Prepare load metrics (before commit)
        load_metrics = {
            "row_count": len(merged_df),
            "file_count": 1,
            "has_data": 1,
        }

        # 6. Validate load metrics (before commit, following standard pipeline)
        validated_metrics = validate_dataset(
            paths_dict=kwargs.get("paths_dict", {}),
            metrics=load_metrics,
            s3_hook=self.s3_hook,
            file_format=self.file_format,  # Pass file format for validation
        )
        logger.info(f"Validation passed: {validated_metrics}")

        # 7. Standard Commit via lakehouse_core
        publish_result, _ = commit(
            store=self.store,
            paths=paths,
            dest=target,
            run_id=run_id,
            partition_date=partition_date,
            metrics=validated_metrics,
        )

        # 8. Cleanup (core handled)
        cleanup(store=self.store, paths=paths)

        return publish_result
