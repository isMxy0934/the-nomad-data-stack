"""Extractor backfill DAG (pieces-only, config-driven).

This DAG is for initializing historical data that must be fetched per-object (symbol)
and then compacted into per-day outputs.

Backfill writes *only* into the backfill area:
  lake/raw/backfill/<dataset>/<source>/dt=YYYY-MM-DD/symbol=<symbol>/data.csv
and writes completion markers next to each piece:
  .../manifest.json
  .../_SUCCESS

To support idempotent reruns without `run_id`, a shard-level marker is stored under:
  lake/raw/backfill/<dataset>/<source>/_meta/symbol=<symbol>/shard=<PART_ID>/_SUCCESS

dag_run.conf (suggested):
  {
    "targets": ["etf_price_akshare"],
    "start_date": "2020-01-01",
    "end_date": "2025-12-18",
    "universe_dt": "latest",
    "max_symbols": 0,
    "force": false
  }
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from io import BytesIO

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.utils.time_utils import get_partition_date_str
from dags.extractor.backfill_specs import (
    backfill_spec_from_mapping,
    load_backfill_specs,
)
from dags.extractor.dw_extractor_dag import load_extractor_specs

logger = logging.getLogger(__name__)

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")


def _resolve_callable(ref: str) -> Callable[..., object]:
    if ":" not in ref:
        raise ValueError(f"Invalid callable reference (expected module:function): {ref}")
    module_name, function_name = ref.split(":", 1)
    module = importlib.import_module(module_name)
    fn = getattr(module, function_name, None)
    if fn is None or not callable(fn):
        raise ValueError(f"Callable not found or not callable: {ref}")
    return fn


def _iter_unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _parse_dt_from_key(key: str) -> str | None:
    match = re.search(r"/dt=(\d{4}-\d{2}-\d{2})/", key)
    if not match:
        return None
    dt = match.group(1)
    try:
        date.fromisoformat(dt)
    except ValueError:
        return None
    return dt


def _s3_key_exists(s3_hook: S3Hook, *, bucket: str, key: str) -> bool:
    check = getattr(s3_hook, "check_for_key", None)
    if callable(check):
        return bool(check(key=key, bucket_name=bucket))
    obj = s3_hook.get_key(key=key, bucket_name=bucket)
    return obj is not None


def _list_latest_data_key(
    s3_hook: S3Hook,
    *,
    bucket: str,
    daily_prefix: str,
) -> str:
    prefix = daily_prefix.strip("/") + "/"
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    keys = [k for k in keys if k.endswith("/data.csv")]
    dts = [dt for dt in (_parse_dt_from_key(k) for k in keys) if dt]
    if not dts:
        raise FileNotFoundError(f"No data.csv partitions found under s3://{bucket}/{daily_prefix}")
    latest_dt = max(dts)
    return f"{daily_prefix.strip('/')}/dt={latest_dt}/data.csv"


def _daily_target_to_prefix() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for spec in load_extractor_specs():
        template = spec.destination_key_template
        prefix = template.split("/dt=", 1)[0].strip("/")
        mapping[spec.target] = prefix
    return mapping


def _conf_targets(conf: Mapping[str, object]) -> set[str] | None:
    raw = conf.get("targets")
    if not raw:
        return None
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("dag_run.conf.targets must be a list of strings")
    return {str(t) for t in raw}


def _month_shards(*, start_date: str, end_date: str) -> list[dict[str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    shards: list[dict[str, str]] = []
    current = date(start.year, start.month, 1)
    while current <= end:
        # next month
        if current.month == 12:
            next_month = date(current.year + 1, 1, 1)
        else:
            next_month = date(current.year, current.month + 1, 1)
        shard_start = max(start, current)
        shard_end = min(end, next_month - timedelta(days=1))
        shards.append(
            {
                "start_date": shard_start.isoformat(),
                "end_date": shard_end.isoformat(),
                "part_id": current.strftime("%Y%m"),
            }
        )
        current = next_month
    return shards


def _normalize_trade_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    return parsed.dt.date.astype("string")


def create_dw_extractor_backfill_dag() -> DAG:
    specs = load_backfill_specs()
    daily_target_to_prefix = _daily_target_to_prefix()

    dag = DAG(
        dag_id=DAG_ID,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["extractor", "backfill"],
        max_active_runs=1,
    )

    with dag:
        all_done = EmptyOperator(task_id="all_done")

        for spec in specs:
            spec_dict = asdict(spec)

            @task(task_id=f"{spec.target}.resolve_universe")
            def resolve_universe(spec_payload: Mapping[str, object]) -> list[str]:
                spec_obj = backfill_spec_from_mapping(spec_payload)
                ctx = get_current_context()
                conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
                targets = _conf_targets(conf)
                if targets is not None and spec_obj.target not in targets:
                    return []

                s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                daily_prefix = daily_target_to_prefix.get(spec_obj.universe.from_target)
                if not daily_prefix:
                    raise ValueError(
                        f"Unknown universe.from_target={spec_obj.universe.from_target}; "
                        "ensure it exists in dags/extractor/config.yaml"
                    )

                universe_dt = str(conf.get("universe_dt") or "latest")
                if universe_dt == "latest":
                    key = _list_latest_data_key(
                        s3_hook, bucket=DEFAULT_BUCKET_NAME, daily_prefix=daily_prefix
                    )
                else:
                    key = f"{daily_prefix.strip('/')}/dt={universe_dt}/data.csv"

                obj = s3_hook.get_key(key=key, bucket_name=DEFAULT_BUCKET_NAME)
                if obj is None:
                    raise FileNotFoundError(
                        f"Universe file not found: s3://{DEFAULT_BUCKET_NAME}/{key}"
                    )
                payload: bytes = obj.get()["Body"].read()

                df = pd.read_csv(BytesIO(payload))
                col = spec_obj.universe.symbol_column
                if col not in df.columns:
                    raise ValueError(
                        f"Universe missing symbol column '{col}' in s3://{DEFAULT_BUCKET_NAME}/{key}"
                    )

                symbols = [str(s).strip() for s in df[col].dropna().tolist()]
                symbols = [s for s in symbols if s]

                max_symbols = int(conf.get("max_symbols") or 0)
                if max_symbols > 0:
                    symbols = symbols[:max_symbols]

                return _iter_unique(symbols)

            @task(task_id=f"{spec.target}.make_shards")
            def make_shards(spec_payload: Mapping[str, object]) -> list[dict[str, str]]:
                spec_obj = backfill_spec_from_mapping(spec_payload)
                ctx = get_current_context()
                conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
                targets = _conf_targets(conf)
                if targets is not None and spec_obj.target not in targets:
                    return []

                start_date = str(conf.get("start_date") or "").strip()
                end_date = str(conf.get("end_date") or "").strip()

                if not start_date:
                    raise ValueError("dag_run.conf must provide start_date for backfill")
                
                if not end_date:
                    # Default to yesterday (partition date) if not specified
                    end_date = get_partition_date_str()
                    logger.info("No end_date provided, defaulting to %s", end_date)

                if spec_obj.shard_type == "none":
                    return [{"start_date": start_date, "end_date": end_date, "part_id": "full"}]
                return _month_shards(start_date=start_date, end_date=end_date)

            @task(task_id=f"{spec.target}.build_jobs")
            def build_jobs(
                symbols: list[str], shards: list[dict[str, str]], _: Mapping[str, object]
            ) -> list[dict[str, str]]:
                if not symbols or not shards:
                    return []
                jobs: list[dict[str, str]] = []
                for shard in shards:
                    for symbol in symbols:
                        jobs.append(
                            {
                                "symbol": symbol,
                                "start_date": shard["start_date"],
                                "end_date": shard["end_date"],
                                "part_id": shard["part_id"],
                            }
                        )
                return jobs

            @task(task_id=f"{spec.target}.fetch_to_pieces")
            def fetch_to_pieces(
                job: Mapping[str, str], spec_payload: Mapping[str, object]
            ) -> Mapping[str, object]:
                spec_obj = backfill_spec_from_mapping(spec_payload)
                ctx = get_current_context()
                conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
                force = bool(conf.get("force") or False)

                s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)

                symbol = job["symbol"]
                start_date = job["start_date"]
                end_date = job["end_date"]
                part_id = job["part_id"]

                meta_prefix = f"{spec_obj.pieces_base_prefix.strip('/')}/_meta/symbol={symbol}/shard={part_id}"
                shard_success_key = f"{meta_prefix}/_SUCCESS"
                if not force and _s3_key_exists(
                    s3_hook, bucket=DEFAULT_BUCKET_NAME, key=shard_success_key
                ):
                    return {"skipped": 1, "symbol": symbol, "part_id": part_id}

                fetcher = _resolve_callable(spec_obj.history_fetcher)
                result = fetcher(symbol=symbol, start_date=start_date, end_date=end_date)
                if result is None:
                    return {
                        "skipped": 0,
                        "symbol": symbol,
                        "part_id": part_id,
                        "row_count": 0,
                        "day_count": 0,
                    }
                if not isinstance(result, pd.DataFrame):
                    raise TypeError(
                        f"history_fetcher must return pandas.DataFrame, got {type(result)}"
                    )
                df = result.copy()
                if df.empty:
                    return {
                        "skipped": 0,
                        "symbol": symbol,
                        "part_id": part_id,
                        "row_count": 0,
                        "day_count": 0,
                    }

                trade_col = spec_obj.trade_date_column
                if trade_col not in df.columns:
                    raise ValueError(
                        f"history_fetcher output missing '{trade_col}' column for target={spec_obj.target}"
                    )

                if "symbol" not in df.columns:
                    df["symbol"] = symbol

                df[trade_col] = _normalize_trade_date(df[trade_col])
                df = df.dropna(subset=[trade_col])

                wrote_days = 0
                wrote_rows = 0
                base_prefix = spec_obj.pieces_base_prefix.strip("/")

                for trade_date in _iter_unique(df[trade_col].astype(str).tolist()):
                    if not trade_date or trade_date == "<NA>":
                        continue
                    try:
                        date.fromisoformat(trade_date)
                    except ValueError:
                        continue

                    day_prefix = f"{base_prefix}/dt={trade_date}/symbol={symbol}"
                    data_key = f"{day_prefix}/data.csv"
                    success_key = f"{day_prefix}/_SUCCESS"
                    manifest_key = f"{day_prefix}/manifest.json"

                    if not force and _s3_key_exists(
                        s3_hook, bucket=DEFAULT_BUCKET_NAME, key=success_key
                    ):
                        continue

                    day_df = df[df[trade_col].astype(str) == trade_date].copy()
                    if day_df.empty:
                        continue

                    csv_bytes = day_df.to_csv(index=False).encode("utf-8")
                    s3_hook.load_bytes(
                        bytes_data=csv_bytes,
                        key=data_key,
                        bucket_name=DEFAULT_BUCKET_NAME,
                        replace=True,
                    )

                    manifest = {
                        "target": spec_obj.target,
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "row_count": int(len(day_df)),
                        "history_range": {
                            "start_date": start_date,
                            "end_date": end_date,
                            "part_id": part_id,
                        },
                        "generated_at": datetime.now(UTC).isoformat(),
                    }
                    s3_hook.load_string(
                        string_data=json.dumps(manifest, sort_keys=True),
                        key=manifest_key,
                        bucket_name=DEFAULT_BUCKET_NAME,
                        replace=True,
                    )
                    s3_hook.load_string(
                        string_data="",
                        key=success_key,
                        bucket_name=DEFAULT_BUCKET_NAME,
                        replace=True,
                    )

                    wrote_days += 1
                    wrote_rows += int(len(day_df))

                s3_hook.load_string(
                    string_data=json.dumps(
                        {
                            "target": spec_obj.target,
                            "symbol": symbol,
                            "part_id": part_id,
                            "history_range": {"start_date": start_date, "end_date": end_date},
                            "row_count": wrote_rows,
                            "day_count": wrote_days,
                            "generated_at": datetime.now(UTC).isoformat(),
                        },
                        sort_keys=True,
                    ),
                    key=f"{meta_prefix}/manifest.json",
                    bucket_name=DEFAULT_BUCKET_NAME,
                    replace=True,
                )
                s3_hook.load_string(
                    string_data="",
                    key=shard_success_key,
                    bucket_name=DEFAULT_BUCKET_NAME,
                    replace=True,
                )

                return {
                    "skipped": 0,
                    "symbol": symbol,
                    "part_id": part_id,
                    "row_count": wrote_rows,
                    "day_count": wrote_days,
                }

            symbols = resolve_universe(spec_dict)
            shards = make_shards(spec_dict)
            jobs = build_jobs(symbols, shards, spec_dict)

            mapped = fetch_to_pieces.partial(spec_payload=spec_dict)
            if spec.pool:
                mapped = mapped.override(pool=spec.pool)
            mapped.expand(job=jobs) >> all_done

        @task.short_circuit(task_id="should_trigger_compact")
        def should_trigger_compact() -> bool:
            ctx = get_current_context()
            conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
            return bool(conf.get("trigger_compact") or False)

        trigger_compact = TriggerDagRunOperator(
            task_id="trigger_dw_extractor_compact_dag",
            trigger_dag_id="dw_extractor_compact_dag",
            wait_for_completion=False,
            reset_dag_run=True,
            conf={
                "targets": "{{ dag_run.conf.get('targets') if dag_run and dag_run.conf else None }}",
                "start_date": "{{ dag_run.conf.get('start_date') if dag_run and dag_run.conf else None }}",
            },
        )

        all_done >> should_trigger_compact() >> trigger_compact

    return dag


dag = create_dw_extractor_backfill_dag()
