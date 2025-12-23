import logging
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.adapters.airflow_s3_store import AirflowS3Store
from dags.ingestion.core.interfaces import BaseCompactor
from dags.utils.etl_utils import (
    DEFAULT_AWS_CONN_ID,
    non_partition_paths_from_xcom,
    partition_paths_from_xcom,
)
from lakehouse_core.domain.observability import log_event
from lakehouse_core.io.paths import NonPartitionPaths, PartitionPaths, build_partition_paths
from lakehouse_core.io.time import normalize_to_partition_format
from lakehouse_core.io.uri import join_uri
from lakehouse_core.pipeline import cleanup, commit, validate

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
        aws_conn_id: str = DEFAULT_AWS_CONN_ID,
    ):
        self.bucket = bucket
        self.prefix_template = prefix_template
        self.file_format = file_format
        self.dedup_cols = dedup_cols
        self.partition_column = partition_column

        # Initialize AirflowS3Store via S3Hook
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.store = AirflowS3Store(self.s3_hook)

    def _get_paths_obj(self, paths_dict: dict) -> PartitionPaths | NonPartitionPaths:
        """Reconstruct paths object from serialized dict (using etl_utils for consistency)."""
        if bool(paths_dict.get("partitioned")):
            return partition_paths_from_xcom(paths_dict)
        return non_partition_paths_from_xcom(paths_dict)

    def _compact_single_partition(
        self,
        df: pd.DataFrame,
        paths: PartitionPaths | NonPartitionPaths,
        target: str,
        partition_date: str,
        run_id: str,
    ) -> dict[str, Any]:
        """Compact a single partition (helper method)."""
        # Ensure idempotency: clear tmp prefix before writing
        self.store.delete_prefix(paths.tmp_prefix)

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

        # Validate load metrics using lakehouse_core directly
        validated_metrics = validate(
            store=self.store,
            paths=paths,
            metrics=load_metrics,
            file_format=self.file_format,
        )
        logger.info(f"Validation passed for partition {partition_date}: {validated_metrics}")

        # Commit via lakehouse_core
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
            from io import BytesIO

            content = self.store.read_bytes(uri)
            # Note: We assume Parquet for intermediate results
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
        is_partitioned = bool(paths_dict.get("partitioned"))
        if self.partition_column and self.partition_column in merged_df.columns and is_partitioned:
            logger.info(
                f"Partitioning data by column '{self.partition_column}' "
                f"into {merged_df[self.partition_column].nunique()} unique partitions"
            )

            publish_results = []
            total_row_count = 0

            for partition_value, partition_df in merged_df.groupby(self.partition_column):
                # Filter out NaN/NaT/None/empty partition values
                if pd.isna(partition_value) or str(partition_value).strip() in {
                    "",
                    "None",
                    "nan",
                    "NaT",
                }:
                    logger.warning(f"Skipping empty/NaN partition value: {partition_value}")
                    continue

                # Normalize partition value to YYYY-MM-DD format with validation
                try:
                    p_date_str = normalize_to_partition_format(partition_value)
                except (ValueError, TypeError) as e:
                    logger.error(
                        f"Invalid partition value for '{self.partition_column}': {partition_value} - {e}"
                    )
                    raise

                logger.info(
                    f"Processing partition: {self.partition_column}={p_date_str} "
                    f"({len(partition_df)} rows)"
                )

                # Build new PartitionPaths for this specific partition
                partition_paths = build_partition_paths(
                    base_uri=f"s3://{self.bucket}",
                    base_prefix=self.prefix_template,
                    partition_date=p_date_str,
                    run_id=run_id,
                )

                result = self._compact_single_partition(
                    df=partition_df,
                    paths=partition_paths,
                    target=target,
                    partition_date=p_date_str,
                    run_id=run_id,
                )
                publish_results.append(result)
                total_row_count += len(partition_df)

            return {
                "row_count": total_row_count,
                "partition_count": len(publish_results),
                "status": "partitioned",
                "partitions": publish_results,
            }

        # 3. Single partition mode (default)
        paths = self._get_paths_obj(paths_dict)
        return self._compact_single_partition(
            df=merged_df,
            paths=paths,
            target=target,
            partition_date=partition_date,
            run_id=run_id,
        )
