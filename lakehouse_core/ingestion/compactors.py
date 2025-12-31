from __future__ import annotations

import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any

import duckdb

from lakehouse_core.api import prepare_paths
from lakehouse_core.compute import normalize_duckdb_path
from lakehouse_core.domain.observability import log_event
from lakehouse_core.ingestion.interfaces import BaseCompactor
from lakehouse_core.io.uri import join_uri
from lakehouse_core.pipeline import cleanup, commit, validate
from lakehouse_core.store.object_store import ObjectStore

logger = logging.getLogger(__name__)


class DuckDBCompactor(BaseCompactor):
    """Compact local parquet results using DuckDB, then commit to S3."""

    def __init__(
        self,
        bucket: str,
        prefix_template: str,
        file_format: str = "csv",
        dedup_cols: list[str] | None = None,
        partition_column: str | None = None,
    ) -> None:
        self.bucket = bucket
        self.prefix_template = prefix_template
        self.file_format = file_format.lower()
        self.dedup_cols = dedup_cols or []
        self.partition_column = partition_column

    def compact(
        self, results: list[dict], target: str, partition_date: str, **kwargs
    ) -> dict[str, Any]:
        store: ObjectStore | None = kwargs.get("store")
        base_uri: str | None = kwargs.get("base_uri")
        run_id: str | None = kwargs.get("run_id")
        local_dir: Path | None = kwargs.get("local_dir")
        write_mode: str = str(kwargs.get("write_mode") or "overwrite").lower()
        max_workers = int(kwargs.get("max_workers") or 1)

        if not store:
            raise ValueError("store is required for DuckDBCompactor")
        if not run_id:
            raise ValueError("run_id is required for DuckDBCompactor")
        if not local_dir:
            raise ValueError("local_dir is required for DuckDBCompactor")

        if base_uri is None:
            base_uri = f"s3://{self.bucket}"

        if self.file_format not in {"csv", "parquet"}:
            raise ValueError(f"Unsupported file_format: {self.file_format}")

        base_prefix = _format_prefix(self.prefix_template, target)
        parquet_files = [Path(r["local_path"]) for r in results if r.get("local_path")]
        if not parquet_files:
            log_event(logger, "ingestion.compact.skip", target=target, reason="no_data")
            return {"row_count": 0, "status": "skipped"}

        connection = duckdb.connect(database=":memory:")
        try:
            _register_base_view(connection, parquet_files)
            view_name = _apply_dedup(connection, self.dedup_cols)

            if self.partition_column and _column_exists(
                connection, view_name, self.partition_column
            ):
                return self._compact_partitioned(
                    connection=connection,
                    view_name=view_name,
                    target=target,
                    base_prefix=base_prefix,
                    run_id=run_id,
                    base_uri=base_uri,
                    store=store,
                    local_dir=local_dir,
                    partition_column=self.partition_column,
                    write_mode=write_mode,
                    max_workers=max_workers,
                )

            if not partition_date:
                raise ValueError("partition_date is required for non-partitioned compaction")

            return self._compact_single_partition(
                connection=connection,
                view_name=view_name,
                target=target,
                base_prefix=base_prefix,
                run_id=run_id,
                base_uri=base_uri,
                store=store,
                local_dir=local_dir,
                partition_date=partition_date,
                write_mode=write_mode,
            )
        finally:
            connection.close()

    def _compact_single_partition(
        self,
        *,
        connection,
        view_name: str,
        target: str,
        base_prefix: str,
        run_id: str,
        base_uri: str,
        store: ObjectStore,
        local_dir: Path,
        partition_date: str,
        write_mode: str,
    ) -> dict[str, Any]:
        unique_run_id = _unique_run_id(run_id, partition_date)
        paths = prepare_paths(
            base_prefix=base_prefix,
            run_id=unique_run_id,
            partition_date=partition_date,
            is_partitioned=True,
            store_namespace=base_uri,
        )

        if _should_skip_partition(write_mode, store, paths.success_flag_path, partition_date):
            return {"status": "skipped", "row_count": 0}

        row_count = _count_rows(connection, view_name)
        if row_count == 0:
            return {"status": "skipped", "row_count": 0}

        write_start = perf_counter()
        local_path = _write_partition_file(
            connection=connection,
            view_name=view_name,
            output_dir=local_dir / f"dt={partition_date}",
            file_format=self.file_format,
        )
        write_ms = (perf_counter() - write_start) * 1000.0
        log_event(
            logger,
            "ingestion.partition.write",
            dt=partition_date,
            row_count=row_count,
            file_format=self.file_format,
            duration_ms=round(write_ms, 2),
            local_path=str(local_path),
        )

        publish_start = perf_counter()
        dest_uri = join_uri(paths.tmp_partition_prefix, local_path.name)
        store.write_bytes(dest_uri, local_path.read_bytes())

        metrics = {"row_count": row_count, "file_count": 1, "has_data": 1}
        validated = validate(
            store=store, paths=paths, metrics=metrics, file_format=self.file_format
        )
        publish_result, _ = commit(
            store=store,
            paths=paths,
            dest=target,
            run_id=unique_run_id,
            partition_date=partition_date,
            metrics=validated,
        )
        cleanup(store=store, paths=paths)
        publish_ms = (perf_counter() - publish_start) * 1000.0
        log_event(
            logger,
            "ingestion.partition.publish",
            dt=partition_date,
            row_count=row_count,
            file_count=1,
            duration_ms=round(publish_ms, 2),
        )
        return dict(publish_result)

    def _compact_partitioned(
        self,
        *,
        connection,
        view_name: str,
        target: str,
        base_prefix: str,
        run_id: str,
        base_uri: str,
        store: ObjectStore,
        local_dir: Path,
        partition_column: str,
        write_mode: str,
        max_workers: int,
    ) -> dict[str, Any]:
        prepared_view = _build_partition_view(connection, view_name, partition_column)
        partition_counts = _partition_counts(connection, prepared_view)

        if not partition_counts:
            log_event(logger, "ingestion.compact.skip", target=target, reason="no_partitions")
            return {"row_count": 0, "status": "skipped"}

        total_rows = 0
        publish_results: list[dict[str, Any]] = []
        skipped: list[str] = []
        work_items: list[_PartitionWork] = []
        total_write_ms = 0.0

        partitions = list(partition_counts.keys())
        paths_by_dt: dict[str, tuple[str, Any]] = {}
        allowed_partitions: list[str] = []

        for partition_date in partitions:
            unique_run_id = _unique_run_id(run_id, partition_date)
            paths = prepare_paths(
                base_prefix=base_prefix,
                run_id=unique_run_id,
                partition_date=partition_date,
                is_partitioned=True,
                store_namespace=base_uri,
            )
            paths_by_dt[partition_date] = (unique_run_id, paths)

            if _should_skip_partition(write_mode, store, paths.success_flag_path, partition_date):
                skipped.append(partition_date)
                continue

            allowed_partitions.append(partition_date)

        if not allowed_partitions:
            log_event(
                logger,
                "ingestion.compact.partitioned",
                target=target,
                partition_count=0,
                skipped_count=len(skipped),
                row_count=0,
                write_ms=0.0,
                max_workers=max_workers,
            )
            return {
                "row_count": 0,
                "partition_count": 0,
                "status": "skipped",
                "partitions": [],
                "skipped_partitions": skipped,
            }

        write_start = perf_counter()
        output_root = local_dir / "partitioned"
        if output_root.exists():
            shutil.rmtree(output_root)
        output_root.mkdir(parents=True, exist_ok=True)
        filter_partitions = (
            allowed_partitions if len(allowed_partitions) < len(partitions) else None
        )
        _write_partitioned_files(
            connection=connection,
            view_name=prepared_view,
            output_dir=output_root,
            file_format=self.file_format,
            allowed_partitions=filter_partitions,
        )
        write_ms = (perf_counter() - write_start) * 1000.0
        total_write_ms += write_ms
        log_event(
            logger,
            "ingestion.partition.write_batch",
            partition_count=len(allowed_partitions),
            file_format=self.file_format,
            duration_ms=round(write_ms, 2),
            output_dir=str(output_root),
        )

        for partition_date in allowed_partitions:
            unique_run_id, paths = paths_by_dt[partition_date]
            row_count = int(partition_counts.get(partition_date, 0))
            local_files = _collect_partition_files(
                output_root=output_root,
                partition_date=partition_date,
                file_format=self.file_format,
            )
            if not local_files or row_count == 0:
                skipped.append(partition_date)
                continue

            work_items.append(
                _PartitionWork(
                    partition_date=partition_date,
                    unique_run_id=unique_run_id,
                    paths=paths,
                    local_paths=local_files,
                    row_count=row_count,
                )
            )

        if work_items:
            total_rows = sum(item.row_count for item in work_items)
            results_by_dt: dict[str, dict[str, Any]] = {}
            publish_start = perf_counter()
            if max_workers <= 1 or len(work_items) == 1:
                for item in work_items:
                    result = _upload_and_commit_partition(
                        item=item,
                        store=store,
                        target=target,
                        file_format=self.file_format,
                    )
                    results_by_dt[item.partition_date] = result
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            _upload_and_commit_partition,
                            item=item,
                            store=store,
                            target=target,
                            file_format=self.file_format,
                        ): item.partition_date
                        for item in work_items
                    }
                    for future in as_completed(futures):
                        partition_date = futures[future]
                        try:
                            results_by_dt[partition_date] = future.result()
                        except Exception as exc:  # noqa: BLE001
                            raise RuntimeError(
                                f"Failed to publish partition dt={partition_date}"
                            ) from exc
            publish_ms = (perf_counter() - publish_start) * 1000.0
            log_event(
                logger,
                "ingestion.partition.publish_batch",
                target=target,
                partition_count=len(work_items),
                max_workers=max_workers,
                duration_ms=round(publish_ms, 2),
            )

            publish_results = [
                results_by_dt[item.partition_date]
                for item in work_items
                if item.partition_date in results_by_dt
            ]

        if work_items or skipped:
            log_event(
                logger,
                "ingestion.compact.partitioned",
                target=target,
                partition_count=len(work_items),
                skipped_count=len(skipped),
                row_count=total_rows,
                write_ms=round(total_write_ms, 2),
                max_workers=max_workers,
            )

        return {
            "row_count": total_rows,
            "partition_count": len(publish_results),
            "status": "partitioned",
            "partitions": publish_results,
            "skipped_partitions": skipped,
        }


@dataclass(frozen=True)
class _PartitionWork:
    partition_date: str
    unique_run_id: str
    paths: Any
    local_paths: list[Path]
    row_count: int


def _upload_and_commit_partition(
    *,
    item: _PartitionWork,
    store: ObjectStore,
    target: str,
    file_format: str,
) -> dict[str, Any]:
    publish_start = perf_counter()
    local_size = 0
    for local_path in item.local_paths:
        local_size += local_path.stat().st_size
        dest_uri = join_uri(item.paths.tmp_partition_prefix, local_path.name)
        store.write_bytes(dest_uri, local_path.read_bytes())

    metrics = {
        "row_count": item.row_count,
        "file_count": len(item.local_paths),
        "has_data": 1,
    }
    validated = validate(store=store, paths=item.paths, metrics=metrics, file_format=file_format)
    publish_result, _ = commit(
        store=store,
        paths=item.paths,
        dest=target,
        run_id=item.unique_run_id,
        partition_date=item.partition_date,
        metrics=validated,
    )
    cleanup(store=store, paths=item.paths)
    publish_ms = (perf_counter() - publish_start) * 1000.0
    log_event(
        logger,
        "ingestion.partition.publish",
        dt=item.partition_date,
        row_count=item.row_count,
        file_count=len(item.local_paths),
        file_size=local_size,
        duration_ms=round(publish_ms, 2),
    )
    return dict(publish_result)


def _register_base_view(connection, parquet_files: list[Path]) -> None:
    file_expr = _format_read_parquet(parquet_files)
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW raw_results AS "
        f"SELECT *, filename AS _ingest_file FROM {file_expr};"
    )

    connection.execute(
        "CREATE OR REPLACE TEMP VIEW base AS "
        "SELECT *, "
        "CASE "
        "WHEN regexp_matches(_ingest_file, 'part_[0-9]+\\.parquet$') "
        "THEN CAST(regexp_extract(_ingest_file, 'part_([0-9]+)\\.parquet$', 1) AS BIGINT) "
        "ELSE 0 END AS _ingest_order "
        "FROM raw_results;"
    )


def _apply_dedup(connection, dedup_cols: list[str]) -> str:
    if not dedup_cols:
        return "base"
    cols = _get_columns(connection, "base")
    available = [col for col in dedup_cols if col in cols]
    if not available:
        return "base"

    cols_expr = ", ".join(available)
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW deduped AS "
        "SELECT * EXCLUDE (_row_num) FROM ("
        f"SELECT *, row_number() OVER (PARTITION BY {cols_expr} ORDER BY _ingest_order DESC) "
        "AS _row_num "
        "FROM base"
        ") WHERE _row_num = 1;"
    )
    return "deduped"


def _build_partition_view(connection, view_name: str, partition_column: str) -> str:
    if partition_column == "__partition_dt":
        raise ValueError("partition_column conflicts with internal column name")
    dt_expr = _build_partition_expression(partition_column)
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW prepared AS "
        f"SELECT *, {dt_expr} AS __partition_dt FROM {view_name};"
    )
    return "prepared"


def _partition_counts(connection, view_name: str) -> dict[str, int]:
    rows = connection.execute(
        f"SELECT __partition_dt, COUNT(*) FROM {view_name} "
        "WHERE __partition_dt IS NOT NULL GROUP BY __partition_dt ORDER BY __partition_dt;"
    ).fetchall()
    return {str(row[0]): int(row[1]) for row in rows if row[0]}


def _write_partitioned_files(
    *,
    connection,
    view_name: str,
    output_dir: Path,
    file_format: str,
    allowed_partitions: list[str] | None = None,
) -> None:
    copy_view = view_name
    if allowed_partitions is not None:
        connection.execute("CREATE OR REPLACE TEMP TABLE allowed_partitions (dt VARCHAR);")
        connection.executemany(
            "INSERT INTO allowed_partitions VALUES (?)", [(dt,) for dt in allowed_partitions]
        )
        connection.execute(
            "CREATE OR REPLACE TEMP VIEW prepared_filtered AS "
            f"SELECT * FROM {view_name} "
            "WHERE __partition_dt IN (SELECT dt FROM allowed_partitions);"
        )
        copy_view = "prepared_filtered"

    dest = normalize_duckdb_path(str(output_dir))
    if file_format == "csv":
        options = (
            "FORMAT CSV, HEADER TRUE, PARTITION_BY (__partition_dt), "
            "WRITE_PARTITION_COLUMNS false, FILENAME_PATTERN 'data_{i}'"
        )
    else:
        options = (
            "FORMAT PARQUET, PARTITION_BY (__partition_dt), "
            "WRITE_PARTITION_COLUMNS false, FILENAME_PATTERN 'data_{i}'"
        )

    connection.execute(
        f"COPY (SELECT * EXCLUDE (__partition_dt) FROM {copy_view}) "
        f"TO '{dest}' ({options});"
    )


def _collect_partition_files(
    *,
    output_root: Path,
    partition_date: str,
    file_format: str,
    partition_column: str = "__partition_dt",
) -> list[Path]:
    partition_dir = output_root / f"{partition_column}={partition_date}"
    if not partition_dir.exists():
        return []
    suffix = ".csv" if file_format == "csv" else ".parquet"
    return sorted(
        [path for path in partition_dir.iterdir() if path.is_file() and path.suffix == suffix]
    )


def _write_partition_file(
    *,
    connection,
    view_name: str,
    output_dir: Path,
    file_format: str,
    partition_date: str | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_ext = "csv" if file_format == "csv" else "parquet"
    output_path = output_dir / f"data.{file_ext}"

    if partition_date:
        where_clause = f"WHERE __partition_dt = '{partition_date}'"
        select_clause = f"SELECT * EXCLUDE (__partition_dt) FROM {view_name} {where_clause}"
    else:
        select_clause = f"SELECT * FROM {view_name}"

    if file_format == "csv":
        copy_sql = (
            f"COPY ({select_clause}) TO '{normalize_duckdb_path(str(output_path))}' "
            "(FORMAT CSV, HEADER TRUE);"
        )
    else:
        copy_sql = (
            f"COPY ({select_clause}) TO '{normalize_duckdb_path(str(output_path))}' "
            "(FORMAT PARQUET);"
        )

    connection.execute(copy_sql)
    return output_path


def _count_rows(connection, view_name: str, partition_date: str | None = None) -> int:
    if partition_date:
        where_clause = f"WHERE __partition_dt = '{partition_date}'"
    else:
        where_clause = ""
    return int(
        connection.execute(f"SELECT COUNT(*) FROM {view_name} {where_clause};").fetchone()[0]
    )


def _column_exists(connection, view_name: str, column_name: str) -> bool:
    return column_name in _get_columns(connection, view_name)


def _get_columns(connection, view_name: str) -> list[str]:
    rows = connection.execute(f"PRAGMA table_info('{view_name}')").fetchall()
    return [str(row[1]) for row in rows]


def _format_read_parquet(files: list[Path]) -> str:
    if len(files) == 1:
        return f"read_parquet('{normalize_duckdb_path(str(files[0]))}', filename=true)"
    normalized = ", ".join([f"'{normalize_duckdb_path(str(path))}'" for path in files])
    return f"read_parquet([{normalized}], filename=true)"


def _build_partition_expression(column_name: str) -> str:
    value = f"TRIM(CAST({column_name} AS VARCHAR))"
    safe_date = f"try_cast({column_name} AS DATE)"
    return (
        "CASE "
        f"WHEN {column_name} IS NULL THEN NULL "
        f"WHEN {safe_date} IS NOT NULL THEN strftime({safe_date}, '%Y-%m-%d') "
        f"WHEN regexp_matches({value}, '^[0-9]{{8}}$') "
        f"THEN strftime(strptime({value}, '%Y%m%d'), '%Y-%m-%d') "
        f"WHEN regexp_matches({value}, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$') "
        f"THEN strftime(strptime({value}, '%Y-%m-%d'), '%Y-%m-%d') "
        "ELSE NULL END"
    )


def _format_prefix(prefix_template: str, target: str) -> str:
    if "{target}" in prefix_template:
        return prefix_template.format(target=target)
    return prefix_template


def _unique_run_id(run_id: str, partition_date: str) -> str:
    safe_dt = partition_date.replace("-", "")
    return f"{run_id}_{safe_dt}"


def _should_skip_partition(
    write_mode: str, store: ObjectStore, success_flag_path: str, partition_date: str
) -> bool:
    if write_mode not in {"overwrite", "skip_existing", "fail_if_exists"}:
        raise ValueError(f"Invalid write_mode: {write_mode}")
    if not store.exists(success_flag_path):
        return False
    if write_mode == "overwrite":
        return False
    if write_mode == "skip_existing":
        logger.info("Skipping dt=%s (already exists).", partition_date)
        return True
    raise RuntimeError(f"Partition already exists: dt={partition_date}")
