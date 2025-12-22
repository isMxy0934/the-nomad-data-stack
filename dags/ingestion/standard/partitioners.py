import itertools
import logging
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.ingestion.core.interfaces import BasePartitioner, IngestionJob
from dags.utils.etl_utils import build_s3_connection_config
from lakehouse_core.compute import configure_s3_access
from lakehouse_core.domain.observability import log_event

logger = logging.getLogger(__name__)


class SqlPartitioner(BasePartitioner):
    """
    Partitions jobs based on a SQL query against the local DuckDB catalog.
    Useful for getting a list of symbols (whitelist).
    """

    def __init__(
        self,
        query: str,
        item_key: str = "symbol",
        catalog_path: str = ".duckdb/catalog.duckdb",
        aws_conn_id: str = "MINIO_S3",
    ):
        self.query = query
        self.item_key = item_key
        self.catalog_path = catalog_path
        self.aws_conn_id = aws_conn_id

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        db_path = Path(self.catalog_path)
        if not db_path.exists():
            # Fallback or error? For now, empty list to avoid crashing during initial setup
            print(f"Warning: Catalog not found at {db_path}")
            return iter([])

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            # Configure S3 Access for DuckDB to read remote tables
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_config = build_s3_connection_config(s3_hook)
            configure_s3_access(conn, s3_config)

            df = conn.sql(self.query).df()
        except Exception as e:
            # If the ODS table doesn't exist yet, we don't want to crash the whole DAG.
            # Just log the error and return empty, which will skip downstream tasks.
            log_event(logger, "Partitioning failed (graceful skip)", error=str(e), query=self.query)
            return iter([])
        finally:
            conn.close()

        if df.empty:
            log_event(logger, "Partitioning query returned empty result", query=self.query)
            return iter([])

        # Assume the query returns the item in the first column if not specified,
        # or we look for item_key if it exists in columns.
        target_col = df.columns[0]
        if self.item_key in df.columns:
            target_col = self.item_key

        items = df[target_col].dropna().unique().tolist()

        log_event(
            logger, "Partitioning via SQL success", item_count=len(items), strategy="SqlPartitioner"
        )

        for item in items:
            yield IngestionJob(params={self.item_key: str(item)})


class TimePartitioner(BasePartitioner):
    """
    Partitions jobs based on time range.
    Supports two modes:
    - 'range': Passes the full start/end date as a single job (passthrough).
    - 'chunk': Splits the range into smaller chunks (e.g., monthly).
    """

    def __init__(self, method: str = "range", freq: str = "1M"):
        self.method = method
        self.freq = freq

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        if not start_date:
            # If no start date provided (e.g. ad-hoc run without params),
            # we might rely on default behavior or return empty.
            # Assuming 'today' if not provided is dangerous in library code,
            # let's assume the caller handles defaults or we pass None.
            # But for jobs requiring dates, we yield one empty-date job if mode is range?
            # Better: if missing, just return a job with None to let Extractor handle defaults.
            yield IngestionJob(params={"start_date": None, "end_date": None})
            return

        if not end_date:
            end_date = start_date

        if self.method == "range":
            yield IngestionJob(params={"start_date": start_date, "end_date": end_date})
            return

        if self.method == "chunk":
            # Generate chunks
            dates = pd.date_range(start=start_date, end=end_date, freq=self.freq)
            # Handle edge case: date_range might be empty if start=end and freq is large
            if len(dates) == 0:
                yield IngestionJob(params={"start_date": start_date, "end_date": end_date})
                return

            # This simple logic needs refinement for precise day-to-day chunking
            # But for now, basic implementation:
            # We create periods.
            # Actually, pd.period_range or simply iterating is safer.

            # Simplified chunking logic:
            cursor = pd.Timestamp(start_date)
            final_end = pd.Timestamp(end_date)

            while cursor <= final_end:
                next_cursor = cursor + pd.tseries.frequencies.to_offset(self.freq)
                # chunk end is next_cursor - 1 day, or final_end
                chunk_end = min(next_cursor - pd.Timedelta(days=1), final_end)

                yield IngestionJob(
                    params={
                        "start_date": cursor.strftime("%Y-%m-%d"),
                        "end_date": chunk_end.strftime("%Y-%m-%d"),
                    },
                    meta={"shard_id": cursor.strftime("%Y%m")},
                )
                cursor = next_cursor


class CompositePartitioner(BasePartitioner):
    """
    Combines multiple partitioners using Cartesian Product.
    """

    def __init__(self, strategies: list[BasePartitioner]):
        self.strategies = strategies

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        # 1. Generate lists of jobs from each strategy
        job_lists = []
        for strategy in self.strategies:
            # We convert iterator to list to allow cartesian product
            job_lists.append(list(strategy.generate_jobs(start_date, end_date, **kwargs)))

        # 2. Cartesian Product
        for combination in itertools.product(*job_lists):
            merged_params = {}
            merged_meta = {}

            for job in combination:
                merged_params.update(job.params)
                merged_meta.update(job.meta)

            yield IngestionJob(params=merged_params, meta=merged_meta)


class SingleJobPartitioner(BasePartitioner):
    """
    A minimal partitioner that yields exactly one job with no parameters.
    Useful for:
    - Scripts that fetch a full snapshot (e.g. local files).
    - APIs that don't accept parameters.
    - Tasks where partitioning is handled internally by the extractor.
    """

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        # Yield one empty job. The Extractor will receive empty params.
        # The storage partition date will be handled by the DAG/Compactor context,
        # not by the job params.
        yield IngestionJob(params={})
