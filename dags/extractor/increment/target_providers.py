"""Built-in target providers for `dw_extractor_dag`.

Providers are optional Python callables configured in
`dags/extractor/increment/config.yaml`.

They can be configured either:
- globally at top-level `provider:` (advanced), or
- per extractor entry under `extractors[].provider:` (recommended).

They must return either:
- `None` (meaning: fall back to running all configured extractors), or
- a list of job mappings, each containing at least `target`.

Job mapping shape:
  {"target": "<extractor_target>", "fetcher_kwargs": {...}}
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def provide_jobs_from_catalog(
    *,
    sql: str,
    catalog_path: str | None = None,
    aws_conn_id: str | None = None,
    target_column: str | None = None,
    fetcher_kwargs: Mapping[str, object] | None = None,
    specs: Sequence[object] | None = None,  # accepted for uniform provider signature
    conf: Mapping[str, Any] | None = None,  # accepted for uniform provider signature
) -> list[dict[str, object]]:
    """Query `catalog.duckdb` and return extractor jobs.

    Args:
      sql: SQL query executed against the catalog database.
      catalog_path: Defaults to `./.duckdb/catalog.duckdb` when absent.
      aws_conn_id: Airflow connection id for MinIO/S3 (defaults to `MINIO_S3`).
      target_column: When provided, uses this column name; otherwise infers when
        query returns a single column.
      fetcher_kwargs: Optional static kwargs to attach to every job.

    Returns:
      List[{"target": "...", "fetcher_kwargs": {...}}]
    """

    import duckdb  # local import to keep DAG parse light
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from dags.utils.etl_utils import DEFAULT_AWS_CONN_ID, build_s3_connection_config
    from lakehouse_core.compute import configure_s3_access

    catalog = Path(catalog_path) if catalog_path else Path(".duckdb/catalog.duckdb")
    if not catalog.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog}")

    conn = duckdb.connect(database=str(catalog), read_only=True)
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id or DEFAULT_AWS_CONN_ID)
        s3_config = build_s3_connection_config(s3_hook)
        configure_s3_access(conn, s3_config)

        rel = conn.sql(sql)
        df = rel.df()
    finally:
        conn.close()

    if df.empty:
        return []

    if target_column:
        if target_column not in df.columns:
            raise ValueError(f"target_column={target_column} not found in query result columns")
        targets = [str(v).strip() for v in df[target_column].tolist()]
    else:
        if len(df.columns) != 1:
            raise ValueError(
                "Query must return exactly 1 column when target_column is not provided"
            )
        targets = [str(v).strip() for v in df[df.columns[0]].tolist()]

    jobs: list[dict[str, object]] = []
    for target in targets:
        if not target:
            continue
        job: dict[str, object] = {"target": target}
        if fetcher_kwargs:
            job["fetcher_kwargs"] = dict(fetcher_kwargs)
        jobs.append(job)
    return jobs


def provide_jobs_from_catalog_values(
    *,
    sql: str,
    kwarg_name: str,
    catalog_path: str | None = None,
    aws_conn_id: str | None = None,
    value_column: str | None = None,
    extra_fetcher_kwargs: Mapping[str, object] | None = None,
    specs: Sequence[object] | None = None,  # accepted for uniform provider signature
    conf: Mapping[str, Any] | None = None,  # accepted for uniform provider signature
    target: str | None = None,
    spec: object | None = None,
) -> list[dict[str, object]]:
    """Query `catalog.duckdb` and return per-value jobs for a single extractor target.

    This is the recommended provider for "target-level" dynamic expansion, where
    the provider returns e.g. a list of symbols, not a list of extractor targets.

    Args:
      sql: Query returning a list of values (e.g. symbols).
      kwarg_name: The fetcher kwarg name to populate (e.g. "symbol").
      value_column: Optional column name in the query result. When omitted, the
        query must return exactly 1 column.
      extra_fetcher_kwargs: Optional extra kwargs merged into every job.

    Returns:
      List[{"target": "<current target>", "fetcher_kwargs": {kwarg_name: value, ...}}]
    """

    if not (target or "").strip():
        raise ValueError("provide_jobs_from_catalog_values requires target to be provided")
    if not kwarg_name.strip():
        raise ValueError("kwarg_name is required")

    import duckdb  # local import to keep DAG parse light
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from dags.utils.etl_utils import DEFAULT_AWS_CONN_ID, build_s3_connection_config
    from lakehouse_core.compute import configure_s3_access

    catalog = Path(catalog_path) if catalog_path else Path(".duckdb/catalog.duckdb")
    if not catalog.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog}")

    conn = duckdb.connect(database=str(catalog), read_only=True)
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id or DEFAULT_AWS_CONN_ID)
        s3_config = build_s3_connection_config(s3_hook)
        configure_s3_access(conn, s3_config)

        rel = conn.sql(sql)
        df = rel.df()
    finally:
        conn.close()

    if df.empty:
        return []

    if value_column:
        if value_column not in df.columns:
            raise ValueError(f"value_column={value_column} not found in query result columns")
        values = df[value_column].tolist()
    else:
        if len(df.columns) != 1:
            raise ValueError("Query must return exactly 1 column when value_column is not provided")
        values = df[df.columns[0]].tolist()

    jobs: list[dict[str, object]] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        kwargs: dict[str, object] = {kwarg_name: text}
        if extra_fetcher_kwargs:
            kwargs.update(dict(extra_fetcher_kwargs))
        jobs.append({"target": str(target), "fetcher_kwargs": kwargs})
    return jobs


def provide_fetcher_kwargs_from_catalog(
    *,
    sql: str,
    catalog_path: str | None = None,
    aws_conn_id: str | None = None,
    columns: Sequence[str] | None = None,
    rename: Mapping[str, str] | None = None,
    drop_nulls: bool = True,
    specs: Sequence[object] | None = None,  # accepted for uniform provider signature
    conf: Mapping[str, Any] | None = None,  # accepted for uniform provider signature
    target: str | None = None,
    spec: object | None = None,
) -> list[dict[str, object]]:
    """Query `catalog.duckdb` and return a list of kwargs dicts (per row).

    This is the most flexible provider for "target-level" expansion: you return
    row-shaped dicts like {"symbol": "...", "market": "..."}.

    `dw_extractor_dag` will treat each returned mapping (without `target`) as
    `fetcher_kwargs` for the current extractor target.
    """

    import duckdb  # local import to keep DAG parse light
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    from dags.utils.etl_utils import DEFAULT_AWS_CONN_ID, build_s3_connection_config
    from lakehouse_core.compute import configure_s3_access

    catalog = Path(catalog_path) if catalog_path else Path(".duckdb/catalog.duckdb")
    if not catalog.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog}")

    conn = duckdb.connect(database=str(catalog), read_only=True)
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id or DEFAULT_AWS_CONN_ID)
        s3_config = build_s3_connection_config(s3_hook)
        configure_s3_access(conn, s3_config)

        df = conn.sql(sql).df()
    finally:
        conn.close()

    if df.empty:
        return []

    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise ValueError(f"columns missing in query result: {missing}")
        df = df[list(columns)]

    if rename:
        df = df.rename(columns=dict(rename))

    records: list[dict[str, object]] = []
    for record in df.to_dict(orient="records"):
        cleaned: dict[str, object] = {}
        for k, v in record.items():
            key = str(k)
            if key in {"target", "fetcher_kwargs"}:
                raise ValueError(
                    f"Query result contains reserved column '{key}'. Please rename it in SQL or via rename=."
                )
            if drop_nulls and (v is None or str(v) == "nan"):
                continue
            cleaned[key] = v if not isinstance(v, str) else v.strip()
        if cleaned:
            records.append(cleaned)

    return records
