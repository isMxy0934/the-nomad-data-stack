"""Extractor compact flow (config-driven)."""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
from collections.abc import Callable, Mapping
from datetime import date
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
from prefect import flow, get_run_logger, task

from dags.extractor.backfill.backfill_specs import backfill_spec_from_mapping, load_backfill_specs
from flows.utils.config import get_s3_bucket_name
from flows.utils.etl_utils import cleanup_dataset, commit_dataset
from flows.utils.s3_utils import get_boto3_client
from lakehouse_core.api import prepare_paths
from lakehouse_core.io.uri import parse_s3_uri

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


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip("/") + "/"


def _list_keys(client, *, bucket: str, prefix: str) -> list[str]:  # noqa: ANN001
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            keys.append(obj["Key"])
    return keys


def _sum_manifest_row_counts(
    *, client, bucket: str, manifest_keys: list[str]
) -> int:  # noqa: ANN001
    total = 0
    for key in manifest_keys:
        try:
            obj = client.get_object(Bucket=bucket, Key=key)
        except Exception:
            continue
        payload = obj["Body"].read()
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        value = data.get("row_count")
        if isinstance(value, int):
            total += value
    return total


def _stream_merge_csv_to_tmp(
    *, client, bucket: str, data_keys: list[str], tmp_key: str
) -> bool:  # noqa: ANN001
    wrote_header = False

    tmp_file = NamedTemporaryFile(mode="wb", delete=False)
    try:
        for key in data_keys:
            try:
                obj = client.get_object(Bucket=bucket, Key=key)
            except Exception:
                continue
            body = obj["Body"]
            first_line = True
            for line in body.iter_lines():
                if first_line:
                    first_line = False
                    if wrote_header:
                        continue
                    wrote_header = True
                    tmp_file.write(line + b"\n")
                    continue
                if not line:
                    continue
                tmp_file.write(line + b"\n")
    finally:
        tmp_file.close()

    if wrote_header:
        client.upload_file(tmp_file.name, bucket, tmp_key)

    os.unlink(tmp_file.name)
    return wrote_header


@task
def list_days(spec_payload: Mapping[str, object]) -> list[str]:
    spec_obj = backfill_spec_from_mapping(spec_payload)
    if not spec_obj.trigger_compact:
        return []
    start_dt = date.fromisoformat(spec_obj.start_date) if spec_obj.start_date else None

    client = get_boto3_client()
    prefix = f"{spec_obj.pieces_base_prefix.strip('/')}/dt="
    keys = _list_keys(client, bucket=DEFAULT_BUCKET_NAME, prefix=prefix)
    keys = [k for k in keys if k.endswith("/_SUCCESS")]
    dts = [dt for dt in (_parse_dt_from_key(k) for k in keys) if dt]
    if start_dt:
        dts = [dt for dt in dts if date.fromisoformat(dt) >= start_dt]
    return sorted(set(dts))


@task
def compact_by_day(trade_date: str, spec_payload: Mapping[str, object]) -> Mapping[str, object]:
    spec_obj = backfill_spec_from_mapping(spec_payload)
    client = get_boto3_client()

    day_prefix = _normalize_prefix(f"{spec_obj.pieces_base_prefix.strip('/')}/dt={trade_date}")
    success_keys = _list_keys(client, bucket=DEFAULT_BUCKET_NAME, prefix=day_prefix)
    success_keys = [k for k in success_keys if k.endswith("/_SUCCESS") and "/symbol=" in k]

    data_keys = [k[: -len("/_SUCCESS")] + "/data.csv" for k in success_keys]
    manifest_keys = [k[: -len("/_SUCCESS")] + "/manifest.json" for k in success_keys]
    if not data_keys:
        logger.warning(
            "No backfill data.csv found for %s on %s under %s",
            spec_obj.target,
            trade_date,
            day_prefix,
        )

    run_id = f"compact_{trade_date}"
    base_prefix = spec_obj.daily_key_template.split("/dt=", 1)[0]

    paths = prepare_paths(
        base_prefix=base_prefix,
        run_id=run_id,
        partition_date=trade_date,
        is_partitioned=True,
        store_namespace=DEFAULT_BUCKET_NAME,
    )

    _, tmp_prefix_key = parse_s3_uri(paths.tmp_partition_prefix)
    tmp_key = f"{tmp_prefix_key.strip('/')}/data.csv"

    row_count = 0
    data_written = False
    if spec_obj.compact_transformer:
        frames: list[pd.DataFrame] = []
        transformer = _resolve_callable(spec_obj.compact_transformer)
        for key in data_keys:
            obj = client.get_object(Bucket=DEFAULT_BUCKET_NAME, Key=key)
            frame = pd.read_csv(BytesIO(obj["Body"].read()))
            frames.append(frame)
        if frames:
            df = pd.concat(frames, ignore_index=True)
            df = transformer(df) if transformer else df
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            client.put_object(Bucket=DEFAULT_BUCKET_NAME, Key=tmp_key, Body=csv_bytes)
            row_count = int(len(df))
            data_written = True
    else:
        data_written = _stream_merge_csv_to_tmp(
            client=client, bucket=DEFAULT_BUCKET_NAME, data_keys=data_keys, tmp_key=tmp_key
        )
        if data_written:
            row_count = _sum_manifest_row_counts(
                client=client, bucket=DEFAULT_BUCKET_NAME, manifest_keys=manifest_keys
            )

    metrics = {"row_count": row_count, "file_count": 1 if data_written else 0, "has_data": int(data_written)}
    paths_payload = paths.__dict__ | {"partitioned": True}
    publish_result, manifest = commit_dataset(
        dest_name=spec_obj.target,
        run_id=run_id,
        paths_dict=paths_payload,
        metrics=metrics,
    )
    if manifest:
        logger.info("Compact manifest: %s", manifest)

    cleanup_dataset(paths_payload)
    return publish_result


@flow(name="dw_extractor_compact_flow")
def dw_extractor_compact_flow(run_conf: dict[str, Any] | None = None) -> None:  # noqa: ARG001
    specs = load_backfill_specs()
    flow_logger = get_run_logger()

    for spec in specs:
        spec_dict = spec.__dict__ | {"universe": spec.universe.__dict__}
        days = list_days.submit(spec_dict).result()
        for trade_date in days:
            flow_logger.info("Compacting target=%s date=%s", spec.target, trade_date)
            compact_by_day.submit(trade_date, spec_dict).result()


if __name__ == "__main__":
    dw_extractor_compact_flow()
