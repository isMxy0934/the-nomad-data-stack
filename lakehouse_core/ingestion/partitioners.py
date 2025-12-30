from __future__ import annotations

import itertools
import logging
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pandas as pd

from lakehouse_core.compute import configure_s3_access
from lakehouse_core.domain.observability import log_event
from lakehouse_core.ingestion.interfaces import BasePartitioner, IngestionJob
from lakehouse_core.ingestion.settings import build_s3_connection_config, resolve_s3_settings

logger = logging.getLogger(__name__)


class SqlPartitioner(BasePartitioner):
    """Partition jobs based on a SQL query against the DuckDB catalog."""

    def __init__(
        self,
        query: str,
        item_key: str = "symbol",
        catalog_path: str = ".duckdb/catalog.duckdb",
    ) -> None:
        self.query = query
        self.item_key = item_key
        self.catalog_path = catalog_path

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        db_path = Path(self.catalog_path)
        if not db_path.exists():
            logger.warning("Catalog not found at %s", db_path)
            return iter([])

        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            s3_settings = resolve_s3_settings()
            s3_config = build_s3_connection_config(s3_settings)
            if s3_config:
                configure_s3_access(conn, s3_config)

            df = conn.sql(self.query).df()
        except Exception as exc:  # noqa: BLE001
            log_event(logger, "partitioning.sql_failed", error=str(exc), query=self.query)
            return iter([])
        finally:
            conn.close()

        if df.empty:
            log_event(logger, "partitioning.sql_empty", query=self.query)
            return iter([])

        target_col = df.columns[0]
        if self.item_key in df.columns:
            target_col = self.item_key

        items = df[target_col].dropna().unique().tolist()
        log_event(logger, "partitioning.sql_ok", item_count=len(items))
        for item in items:
            yield IngestionJob(params={self.item_key: str(item)})


class TimePartitioner(BasePartitioner):
    """Partition jobs based on time ranges (range or chunk)."""

    def __init__(self, method: str = "range", freq: str = "1M") -> None:
        self.method = method
        self.freq = freq

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        if not start_date:
            yield IngestionJob(params={"start_date": None, "end_date": None})
            return

        if not end_date:
            end_date = start_date

        if self.method == "range":
            yield IngestionJob(params={"start_date": start_date, "end_date": end_date})
            return

        if self.method == "chunk":
            dates = pd.date_range(start=start_date, end=end_date, freq=self.freq)
            if len(dates) == 0:
                yield IngestionJob(params={"start_date": start_date, "end_date": end_date})
                return

            cursor = pd.Timestamp(start_date)
            final_end = pd.Timestamp(end_date)
            while cursor <= final_end:
                next_cursor = cursor + pd.tseries.frequencies.to_offset(self.freq)
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
    """Combine multiple partitioners using Cartesian product."""

    def __init__(self, strategies: list[BasePartitioner]) -> None:
        self.strategies = strategies

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        job_lists = [
            list(strategy.generate_jobs(start_date, end_date, **kwargs))
            for strategy in self.strategies
        ]
        for combination in itertools.product(*job_lists):
            merged_params: dict[str, str] = {}
            merged_meta: dict[str, str] = {}
            for job in combination:
                merged_params.update(job.params)
                merged_meta.update(job.meta)
            yield IngestionJob(params=merged_params, meta=merged_meta)


class SingleJobPartitioner(BasePartitioner):
    """Yield a single job without parameters."""

    def generate_jobs(
        self, start_date: str | None = None, end_date: str | None = None, **kwargs
    ) -> Iterator[IngestionJob]:
        yield IngestionJob(params={})
