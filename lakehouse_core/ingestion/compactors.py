from __future__ import annotations

import logging
from pathlib import Path
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

        local_path = _write_partition_file(
            connection=connection,
            view_name=view_name,
            output_dir=local_dir / f"dt={partition_date}",
            file_format=self.file_format,
        )

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
    ) -> dict[str, Any]:
        prepared_view = _build_partition_view(connection, view_name, partition_column)
        partitions = _list_partitions(connection, prepared_view)

        if not partitions:
            log_event(logger, "ingestion.compact.skip", target=target, reason="no_partitions")
            return {"row_count": 0, "status": "skipped"}

        total_rows = 0
        publish_results: list[dict[str, Any]] = []
        skipped: list[str] = []

        for partition_date in partitions:
            unique_run_id = _unique_run_id(run_id, partition_date)
            paths = prepare_paths(
                base_prefix=base_prefix,
                run_id=unique_run_id,
                partition_date=partition_date,
                is_partitioned=True,
                store_namespace=base_uri,
            )

            if _should_skip_partition(write_mode, store, paths.success_flag_path, partition_date):
                skipped.append(partition_date)
                continue

            row_count = _count_rows(connection, prepared_view, partition_date)
            if row_count == 0:
                skipped.append(partition_date)
                continue

            local_path = _write_partition_file(
                connection=connection,
                view_name=prepared_view,
                output_dir=local_dir / f"dt={partition_date}",
                file_format=self.file_format,
                partition_date=partition_date,
            )

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
            publish_results.append(dict(publish_result))
            total_rows += row_count

        return {
            "row_count": total_rows,
            "partition_count": len(publish_results),
            "status": "partitioned",
            "partitions": publish_results,
            "skipped_partitions": skipped,
        }


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


def _list_partitions(connection, view_name: str) -> list[str]:
    rows = connection.execute(
        f"SELECT DISTINCT __partition_dt FROM {view_name} "
        "WHERE __partition_dt IS NOT NULL ORDER BY __partition_dt;"
    ).fetchall()
    return [str(row[0]) for row in rows if row[0]]


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
