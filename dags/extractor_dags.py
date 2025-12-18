"""Extractor DAGs (consolidated).

Goal: keep each dataset+source runnable independently (separate DAG IDs and schedules),
while keeping implementation in one place.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator

from dags.utils.extractor_utils import CsvPayload, ExtractorDagSpec
from dags.utils.s3_utils import S3Uploader
from dags.utils.time_utils import get_date_str, get_partition_date_str

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "extractor" / "config.yaml"


def _upload_csv_to_s3(*, destination_key: str, payload: CsvPayload) -> str:
    uploader = S3Uploader()
    uploader.upload_bytes(payload.csv_bytes, destination_key, replace=True)
    logger.info("Uploaded to S3: %s (records=%s)", destination_key, payload.record_count)
    return destination_key


def _resolve_fetcher(fetcher_ref: str) -> Callable[[], CsvPayload | None]:
    """Resolve a fetcher function from `module:function` string."""

    if ":" not in fetcher_ref:
        raise ValueError(f"Invalid fetcher reference (expected module:function): {fetcher_ref}")
    module_name, function_name = fetcher_ref.split(":", 1)
    module = importlib.import_module(module_name)
    fetcher = getattr(module, function_name, None)
    if fetcher is None or not callable(fetcher):
        raise ValueError(f"Fetcher not found or not callable: {fetcher_ref}")
    return fetcher


def run_extractor(*, fetcher: str, destination_key_template: str, dag_id: str) -> str | None:
    target_date_str = get_date_str()
    target_partition_date_str = get_partition_date_str()
    destination_key = destination_key_template.format(PARTITION_DATE=target_partition_date_str)

    logger.info(
        "Start extractor: dag_id=%s fetcher=%s target_date=%s partition_date=%s",
        dag_id,
        fetcher,
        target_date_str,
        target_partition_date_str,
    )

    fetcher_callable = _resolve_fetcher(fetcher)
    payload = fetcher_callable()
    if payload is None or payload.record_count == 0:
        logger.warning("No data for extractor %s; no-op.", dag_id)
        return None

    return _upload_csv_to_s3(destination_key=destination_key, payload=payload)


def build_extractor_dag(spec: ExtractorDagSpec) -> DAG:
    dag = DAG(
        dag_id=spec.dag_id,
        schedule=spec.schedule,
        start_date=spec.start_date,
        catchup=False,
        tags=spec.tags,
    )

    with dag:
        PythonOperator(
            task_id=spec.task_id,
            python_callable=run_extractor,
            op_kwargs={
                "fetcher": spec.fetcher,
                "destination_key_template": spec.destination_key_template,
                "dag_id": spec.dag_id,
            },
        )

    return dag


def _parse_start_date(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError("start_date must be a non-empty string")
    text = value.strip()
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return datetime.fromisoformat(text)
    return datetime.fromisoformat(text)


def _load_specs_from_config(path: Path) -> list[ExtractorDagSpec]:
    if not path.exists():
        raise FileNotFoundError(f"Extractor config not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("extractor config must be a mapping")
    extractors = raw.get("extractors")
    if not isinstance(extractors, Sequence):
        raise ValueError("extractor config must contain 'extractors' list")

    specs: list[ExtractorDagSpec] = []
    for item in extractors:
        if not isinstance(item, Mapping):
            raise ValueError("each extractor entry must be a mapping")
        dag_id = str(item.get("dag_id", "")).strip()
        task_id = str(item.get("task_id", "")).strip()
        destination_key_template = str(item.get("destination_key_template", "")).strip()
        fetcher = str(item.get("fetcher", "")).strip()
        if not dag_id or not task_id or not destination_key_template or not fetcher:
            raise ValueError(f"invalid extractor entry (missing required fields): {item}")

        schedule_raw = item.get("schedule")
        schedule = None if schedule_raw in {None, ""} else str(schedule_raw)

        tags_raw = item.get("tags") or []
        if not isinstance(tags_raw, Sequence):
            raise ValueError(f"tags must be a list for {dag_id}")
        tags = [str(t) for t in tags_raw]

        start_date = _parse_start_date(item.get("start_date"))

        specs.append(
            ExtractorDagSpec(
                dag_id=dag_id,
                task_id=task_id,
                schedule=schedule,
                start_date=start_date,
                tags=tags,
                destination_key_template=destination_key_template,
                fetcher=fetcher,
            )
        )

    return specs


def build_extractor_dags() -> dict[str, DAG]:
    specs = _load_specs_from_config(CONFIG_PATH)
    return {spec.dag_id: build_extractor_dag(spec) for spec in specs}


extractor_dags = build_extractor_dags()
globals().update(extractor_dags)
