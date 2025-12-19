"""Extractor compact DAG (config-driven).

Compacts backfill pieces into final daily outputs:
  lake/raw/daily/<dataset>/<source>/dt=YYYY-MM-DD/data.csv

This DAG does not use `run_id`. It scans the backfill area and overwrites the
final daily output for each day (full overwrite).

dag_run.conf (optional):
  {
    "targets": ["etf_price_akshare"],
    "start_date": "2020-01-01"
  }
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from io import BytesIO

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from dags.extractor.backfill_specs import (
    backfill_spec_from_mapping,
    load_backfill_specs,
)
from dags.utils.etl_utils import cleanup_dataset, commit_dataset
from dags.utils.partition_utils import build_partition_paths, parse_s3_uri


def _resolve_callable(ref: str) -> Callable[..., object]:
    if ":" not in ref:
        raise ValueError(f"Invalid callable reference (expected module:function): {ref}")
    module_name, function_name = ref.split(":", 1)
    module = importlib.import_module(module_name)
    fn = getattr(module, function_name, None)
    if fn is None or not callable(fn):
        raise ValueError(f"Callable not found or not callable: {ref}")
    return fn


logger = logging.getLogger(__name__)

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

DEFAULT_AWS_CONN_ID = "MINIO_S3"
DEFAULT_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "stock-data")


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


def _conf_targets(conf: Mapping[str, object]) -> set[str] | None:
    raw = conf.get("targets")
    if not raw:
        return None
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("dag_run.conf.targets must be a list of strings")
    return {str(t) for t in raw}


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip("/") + "/"


def create_dw_extractor_compact_dag() -> DAG:
    specs = load_backfill_specs()

    dag = DAG(
        dag_id=DAG_ID,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["extractor", "compact"],
        max_active_runs=1,
    )

    with dag:
        all_done = EmptyOperator(task_id="all_done")

        for spec in specs:
            spec_dict = spec.__dict__ | {
                "universe": spec.universe.__dict__
            }  # stable dict, no asdict() recursion issues

            @task(task_id=f"{spec.target}.list_days")
            def list_days(spec_payload: Mapping[str, object]) -> list[str]:
                spec_obj = backfill_spec_from_mapping(spec_payload)
                ctx = get_current_context()
                conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
                targets = _conf_targets(conf)
                if targets is not None and spec_obj.target not in targets:
                    return []

                start = str(conf.get("start_date") or "").strip()
                start_dt = date.fromisoformat(start) if start else None

                s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)
                prefix = f"{spec_obj.pieces_base_prefix.strip('/')}/dt="
                keys = s3_hook.list_keys(bucket_name=DEFAULT_BUCKET_NAME, prefix=prefix) or []
                keys = [k for k in keys if k.endswith("/_SUCCESS")]
                dts = [dt for dt in (_parse_dt_from_key(k) for k in keys) if dt]
                if start_dt:
                    dts = [dt for dt in dts if date.fromisoformat(dt) >= start_dt]
                return sorted(set(dts))

            @task(task_id=f"{spec.target}.compact_by_day")
            def compact_by_day(
                trade_date: str, spec_payload: Mapping[str, object]
            ) -> Mapping[str, object]:
                spec_obj = backfill_spec_from_mapping(spec_payload)
                s3_hook = S3Hook(aws_conn_id=DEFAULT_AWS_CONN_ID)

                day_prefix = _normalize_prefix(
                    f"{spec_obj.pieces_base_prefix.strip('/')}/dt={trade_date}"
                )
                success_keys = (
                    s3_hook.list_keys(bucket_name=DEFAULT_BUCKET_NAME, prefix=day_prefix) or []
                )
                success_keys = [
                    k for k in success_keys if k.endswith("/_SUCCESS") and "/symbol=" in k
                ]

                data_keys = [k[: -len("/_SUCCESS")] + "/data.csv" for k in success_keys]
                frames: list[pd.DataFrame] = []
                for key in data_keys:
                    obj = s3_hook.get_key(key=key, bucket_name=DEFAULT_BUCKET_NAME)
                    if obj is None:
                        continue
                    payload: bytes = obj.get()["Body"].read()
                    frames.append(pd.read_csv(BytesIO(payload)))

                if frames:
                    df = pd.concat(frames, ignore_index=True, copy=False)
                else:
                    df = pd.DataFrame()

                if spec_obj.compact_transformer:
                    transformer = _resolve_callable(spec_obj.compact_transformer)
                    transformed = transformer(df, trade_date=trade_date, spec=spec_obj)
                    if transformed is None:
                        df = pd.DataFrame()
                    elif not isinstance(transformed, pd.DataFrame):
                        raise TypeError(
                            f"compact_transformer must return pandas.DataFrame|None, got {type(transformed)}"
                        )
                    else:
                        df = transformed

                # Standard ETL Flow via Utils
                run_id = f"compact_{trade_date}"
                base_prefix = spec_obj.daily_key_template.split("/dt=", 1)[0]

                paths = build_partition_paths(
                    base_prefix=base_prefix,
                    partition_date=trade_date,
                    run_id=run_id,
                    bucket_name=DEFAULT_BUCKET_NAME,
                )

                metrics = {
                    "row_count": int(len(df)),
                    "file_count": 0 if df.empty else 1,
                    "has_data": 0 if df.empty else 1,
                }

                # Mock paths_dict expected by etl_utils
                paths_dict = {
                    "partitioned": True,
                    "partition_date": trade_date,
                    "tmp_partition_prefix": paths.tmp_partition_prefix,
                    "canonical_prefix": paths.canonical_prefix,
                    "tmp_prefix": paths.tmp_prefix,
                    "manifest_path": paths.manifest_path,
                    "success_flag_path": paths.success_flag_path,
                }

                if metrics["has_data"]:
                    # Write to standardized tmp location
                    # paths.tmp_partition_prefix is a URI (s3://...), we need the key
                    _, tmp_prefix_key = parse_s3_uri(paths.tmp_partition_prefix)
                    tmp_key = f"{tmp_prefix_key.strip('/')}/data.csv"

                    csv_bytes = df.to_csv(index=False).encode("utf-8")
                    s3_hook.load_bytes(
                        bytes_data=csv_bytes,
                        key=tmp_key,
                        bucket_name=DEFAULT_BUCKET_NAME,
                        replace=True,
                    )

                commit_dataset(
                    dest_name=spec_obj.target,
                    run_id=run_id,
                    paths_dict=paths_dict,
                    metrics=metrics,
                    s3_hook=s3_hook,
                )

                cleanup_dataset(paths_dict, s3_hook)

                return {
                    "trade_date": trade_date,
                    "row_count": int(len(df)),
                    "symbol_file_count": int(len(data_keys)),
                }

            days = list_days(spec_dict)
            compact_by_day.partial(spec_payload=spec_dict).expand(trade_date=days) >> all_done

    return dag


dag = create_dw_extractor_compact_dag()
