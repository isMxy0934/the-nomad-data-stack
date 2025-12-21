"""Extractor backfill flow (pieces-only, config-driven)."""

from __future__ import annotations

import importlib
import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from typing import Any

import pandas as pd
from prefect import flow, get_run_logger, task

from dags.extractor.backfill.backfill_specs import backfill_spec_from_mapping, load_backfill_specs
from flows.dw_extractor_compact_flow import dw_extractor_compact_flow
from flows.extractor.increment.specs import load_extractor_specs
from flows.utils.config import get_s3_bucket_name
from flows.utils.s3_utils import get_boto3_client
from lakehouse_core.io.time import get_partition_date_str

logger = logging.getLogger(__name__)

DEFAULT_BUCKET_NAME = get_s3_bucket_name()


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


def _list_keys(client, *, bucket: str, prefix: str) -> list[str]:  # noqa: ANN001
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append(obj["Key"])
    return keys


def _s3_key_exists(client, *, bucket: str, key: str) -> bool:  # noqa: ANN001
    try:
        client.head_object(Bucket=bucket, Key=key)
    except Exception:
        return False
    return True


def _list_latest_data_key(*, client, bucket: str, daily_prefix: str) -> str:  # noqa: ANN001
    prefix = daily_prefix.strip("/") + "/"
    keys = _list_keys(client, bucket=bucket, prefix=prefix)
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
        raise ValueError("run_conf.targets must be a list of strings")
    return {str(t) for t in raw}


def _month_shards(*, start_date: str, end_date: str) -> list[dict[str, str]]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must be >= start_date")

    shards: list[dict[str, str]] = []
    current = date(start.year, start.month, 1)
    while current <= end:
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


@task
def resolve_universe(
    spec_payload: Mapping[str, object],
    run_conf: Mapping[str, object],
    daily_target_to_prefix: Mapping[str, str],
) -> list[str]:
    spec_obj = backfill_spec_from_mapping(spec_payload)
    targets = _conf_targets(run_conf)
    if targets is not None and spec_obj.target not in targets:
        return []

    allowlist = set(spec_obj.symbol_allowlist or [])
    runtime_allowlist = run_conf.get("symbol_allowlist")
    if runtime_allowlist:
        if not isinstance(runtime_allowlist, list):
            raise ValueError("run_conf.symbol_allowlist must be a list of strings")
        allowlist.update(str(s) for s in runtime_allowlist)

    max_symbols = int(run_conf.get("max_symbols") or 0)
    if allowlist:
        symbols = [str(s).strip() for s in allowlist if str(s).strip()]
        symbols = _iter_unique(symbols)
        if max_symbols > 0:
            symbols = symbols[:max_symbols]
        logger.info(
            "Resolved universe via allowlist: target=%s symbols=%d",
            spec_obj.target,
            len(symbols),
        )
        return symbols

    client = get_boto3_client()
    daily_prefix = daily_target_to_prefix.get(spec_obj.universe.from_target)
    if not daily_prefix:
        available = ", ".join(sorted(daily_target_to_prefix.keys())) or "<none>"
        raise ValueError(
            f"Unknown universe.from_target={spec_obj.universe.from_target}; "
            "ensure it exists in dags/extractor/increment/config.yaml "
            f"(available targets: {available})"
        )

    universe_dt = str(run_conf.get("universe_dt") or "latest")
    if universe_dt == "latest":
        key = _list_latest_data_key(client=client, bucket=DEFAULT_BUCKET_NAME, daily_prefix=daily_prefix)
    else:
        key = f"{daily_prefix.strip('/')}/dt={universe_dt}/data.csv"

    obj = client.get_object(Bucket=DEFAULT_BUCKET_NAME, Key=key)
    payload: bytes = obj["Body"].read()

    df = pd.read_csv(BytesIO(payload))
    col = spec_obj.universe.symbol_column
    if col not in df.columns:
        raise ValueError(
            f"Universe missing symbol column '{col}' in s3://{DEFAULT_BUCKET_NAME}/{key}"
        )

    symbols = [str(s).strip() for s in df[col].dropna().tolist()]
    symbols = [s for s in symbols if s]

    if allowlist:
        symbols = [s for s in symbols if s in allowlist]
        logger.info("Filtered universe to %d symbols based on allowlist", len(symbols))

    if max_symbols > 0:
        symbols = symbols[:max_symbols]

    return _iter_unique(symbols)


@task
def make_shards(spec_payload: Mapping[str, object], run_conf: Mapping[str, object]) -> list[dict[str, str]]:
    spec_obj = backfill_spec_from_mapping(spec_payload)
    targets = _conf_targets(run_conf)
    if targets is not None and spec_obj.target not in targets:
        return []

    start_date = str(spec_obj.start_date or "").strip()
    end_date = str(spec_obj.end_date or "").strip()

    if not start_date:
        raise ValueError("configs/backfill_config.yaml must provide start_date for backfill extractors")

    if not end_date:
        end_date = get_partition_date_str()
        logger.info("No end_date configured, defaulting to %s", end_date)

    if spec_obj.shard_type == "none":
        return [{"start_date": start_date, "end_date": end_date, "part_id": "full"}]
    return _month_shards(start_date=start_date, end_date=end_date)


@task
def build_jobs(
    symbols: list[str], shards: list[dict[str, str]], spec_payload: Mapping[str, object]
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


@task
def fetch_to_pieces(job: Mapping[str, str], spec_payload: Mapping[str, object], run_conf: Mapping[str, object]) -> Mapping[str, object]:
    spec_obj = backfill_spec_from_mapping(spec_payload)
    force = bool(run_conf.get("force") or False)

    client = get_boto3_client()

    symbol = job["symbol"]
    start_date = job["start_date"]
    end_date = job["end_date"]
    part_id = job["part_id"]

    logger.info(
        "Backfill fetch start: target=%s symbol=%s part_id=%s start_date=%s end_date=%s force=%s",
        spec_obj.target,
        symbol,
        part_id,
        start_date,
        end_date,
        force,
    )

    meta_prefix = f"{spec_obj.pieces_base_prefix.strip('/')}/_meta/symbol={symbol}/shard={part_id}"
    shard_success_key = f"{meta_prefix}/_SUCCESS"
    if not force and _s3_key_exists(client, bucket=DEFAULT_BUCKET_NAME, key=shard_success_key):
        logger.info(
            "Backfill shard already done, skipping: target=%s symbol=%s part_id=%s",
            spec_obj.target,
            symbol,
            part_id,
        )
        return {"skipped": 1, "symbol": symbol, "part_id": part_id}

    fetcher = _resolve_callable(spec_obj.history_fetcher)
    result = fetcher(symbol=symbol, start_date=start_date, end_date=end_date)
    if result is None:
        logger.info(
            "Backfill fetch returned None: target=%s symbol=%s part_id=%s",
            spec_obj.target,
            symbol,
            part_id,
        )
        return {"skipped": 0, "symbol": symbol, "part_id": part_id, "row_count": 0, "day_count": 0}
    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"history_fetcher must return pandas.DataFrame, got {type(result)}")
    df = result.copy()
    if df.empty:
        logger.info(
            "Backfill fetch returned empty: target=%s symbol=%s part_id=%s",
            spec_obj.target,
            symbol,
            part_id,
        )
        return {"skipped": 0, "symbol": symbol, "part_id": part_id, "row_count": 0, "day_count": 0}

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

        if not force and _s3_key_exists(client, bucket=DEFAULT_BUCKET_NAME, key=success_key):
            continue

        day_df = df[df[trade_col].astype(str) == trade_date].copy()
        if day_df.empty:
            continue

        tmp_key = f"{day_prefix}/_tmp/data.csv"
        csv_bytes = day_df.to_csv(index=False).encode("utf-8")
        client.put_object(Bucket=DEFAULT_BUCKET_NAME, Key=tmp_key, Body=csv_bytes)

        client.copy_object(
            Bucket=DEFAULT_BUCKET_NAME,
            Key=data_key,
            CopySource={"Bucket": DEFAULT_BUCKET_NAME, "Key": tmp_key},
        )
        client.delete_objects(
            Bucket=DEFAULT_BUCKET_NAME,
            Delete={"Objects": [{"Key": tmp_key}]},
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
        client.put_object(
            Bucket=DEFAULT_BUCKET_NAME,
            Key=manifest_key,
            Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
        )
        client.put_object(Bucket=DEFAULT_BUCKET_NAME, Key=success_key, Body=b"")

        wrote_days += 1
        wrote_rows += int(len(day_df))

    client.put_object(
        Bucket=DEFAULT_BUCKET_NAME,
        Key=f"{meta_prefix}/manifest.json",
        Body=json.dumps(
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
        ).encode("utf-8"),
    )

    try:
        shard_y = int(part_id[:4])
        shard_m = int(part_id[4:])
        shard_start_dt = date(shard_y, shard_m, 1)
        if shard_m == 12:
            shard_end_dt = date(shard_y + 1, 1, 1) - timedelta(days=1)
        else:
            shard_end_dt = date(shard_y, shard_m + 1, 1) - timedelta(days=1)

        job_start_dt = date.fromisoformat(start_date)
        job_end_dt = date.fromisoformat(end_date)

        if job_start_dt <= shard_start_dt and job_end_dt >= shard_end_dt:
            client.put_object(Bucket=DEFAULT_BUCKET_NAME, Key=shard_success_key, Body=b"")
        else:
            logger.info(
                "Skipping shard success mark for %s: job range %s~%s partial vs shard %s~%s",
                part_id,
                start_date,
                end_date,
                shard_start_dt,
                shard_end_dt,
            )
    except ValueError:
        client.put_object(Bucket=DEFAULT_BUCKET_NAME, Key=shard_success_key, Body=b"")

    logger.info(
        "Backfill fetch complete: target=%s symbol=%s part_id=%s wrote_rows=%d wrote_days=%d",
        spec_obj.target,
        symbol,
        part_id,
        wrote_rows,
        wrote_days,
    )
    return {"skipped": 0, "symbol": symbol, "part_id": part_id, "row_count": wrote_rows, "day_count": wrote_days}


@flow(name="dw_extractor_backfill_flow")
def dw_extractor_backfill_flow(run_conf: dict[str, Any] | None = None) -> None:
    run_conf = run_conf or {}
    specs = load_backfill_specs()
    daily_target_to_prefix = _daily_target_to_prefix()
    flow_logger = get_run_logger()

    for spec in specs:
        spec_dict = asdict(spec)
        symbols = resolve_universe.submit(spec_dict, run_conf, daily_target_to_prefix).result()
        shards = make_shards.submit(spec_dict, run_conf).result()
        jobs = build_jobs.submit(symbols, shards, spec_dict).result()

        for job in jobs:
            flow_logger.info("Backfill job target=%s symbol=%s part_id=%s", spec.target, job["symbol"], job["part_id"])
            fetch_to_pieces.submit(job, spec_dict, run_conf).result()

    if any(spec.trigger_compact for spec in specs):
        dw_extractor_compact_flow(run_conf=run_conf)


if __name__ == "__main__":
    dw_extractor_backfill_flow()
