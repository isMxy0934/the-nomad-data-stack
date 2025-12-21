"""Unified extractor flow (daily, config-driven)."""

from __future__ import annotations

import importlib
import logging
import os
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task

from dags.utils.extractor_utils import CsvPayload
from flows.dw_catalog_flow import dw_catalog_flow
from flows.extractor.increment.specs import load_extractor_specs
from flows.utils.dag_run_utils import parse_targets
from flows.utils.runtime import get_flow_run_id
from flows.utils.s3_utils import PrefectS3Uploader
from lakehouse_core.io.time import get_partition_date_str

logger = logging.getLogger(__name__)


def _safe_segment(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value).strip("_") or "x"


def _resolve_fetcher(fetcher_ref: str) -> Callable[[], CsvPayload | None]:
    if ":" not in fetcher_ref:
        raise ValueError(f"Invalid fetcher reference (expected module:function): {fetcher_ref}")
    module_name, function_name = fetcher_ref.split(":", 1)
    module = importlib.import_module(module_name)
    fetcher = getattr(module, function_name, None)
    if fetcher is None or not callable(fetcher):
        raise ValueError(f"Fetcher not found or not callable: {fetcher_ref}")
    return fetcher


@task(retries=3, retry_delay_seconds=60)
def fetch_to_tmp(target: str, fetcher: str, run_id: str) -> dict[str, object]:
    fetcher_callable = _resolve_fetcher(fetcher)
    payload = fetcher_callable()
    if payload is None or payload.record_count == 0:
        logger.info("No data for %s; skip write_raw.", target)
        return {"has_data": 0, "record_count": 0}

    base_dir = Path(os.getenv("EXTRACTOR_TMP_DIR", "/tmp")) / "dw_extractor"
    tmp_dir = base_dir / _safe_segment(run_id) / _safe_segment(target)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_file = tmp_dir / "data.csv"
    tmp_file.write_bytes(payload.csv_bytes)

    logger.info("Fetched %s records for %s into %s", payload.record_count, target, tmp_file)
    return {"has_data": 1, "record_count": payload.record_count, "tmp_file": str(tmp_file)}


@task(retries=3, retry_delay_seconds=60)
def write_raw(
    target: str,
    destination_key_template: str,
    fetched: Mapping[str, object],
    partition_date: str,
) -> str | None:
    if not int(fetched.get("has_data", 0)):
        return None

    tmp_file = fetched.get("tmp_file")
    if not tmp_file:
        raise ValueError("Missing tmp_file from fetch step")

    destination_key = destination_key_template.format(PARTITION_DATE=partition_date)
    uploader = PrefectS3Uploader()
    s3_path = uploader.upload_file(str(tmp_file), key=destination_key)

    try:
        Path(tmp_file).unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to delete tmp file %s", tmp_file)

    logger.info("write_raw done: target=%s s3_path=%s", target, s3_path)
    return s3_path


@flow(name="dw_extractor_flow")
def dw_extractor_flow(run_conf: dict[str, Any] | None = None) -> None:
    run_conf = run_conf or {}
    if bool(run_conf.get("init")):
        return

    targets = parse_targets(run_conf)
    if targets is not None:
        # extractor targets are not supported; keep Airflow behavior
        return

    specs = load_extractor_specs()
    run_id = get_flow_run_id()
    partition_date = str(run_conf.get("partition_date") or "").strip() or get_partition_date_str()
    flow_logger = get_run_logger()

    for spec in specs:
        flow_logger.info("Running extractor target=%s", spec.target)
        fetched = fetch_to_tmp.submit(spec.target, spec.fetcher, run_id).result()
        _ = write_raw.submit(
            spec.target,
            spec.destination_key_template,
            fetched,
            partition_date,
        ).result()

    dw_catalog_flow(run_conf=run_conf)


if __name__ == "__main__":
    dw_extractor_flow()
