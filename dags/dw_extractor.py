"""Unified extractor DAG (daily, config-driven).

This DAG consolidates all extractors into one `dw_extractor` DAG, while keeping
each "metric + source" independently runnable via `dag_run.conf.targets`.

Example:
  {"targets":["fund_price_akshare","stock_price_tushare"]}
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup

from dags.utils.extractor_utils import CsvPayload
from dags.utils.s3_utils import S3Uploader
from dags.utils.time_utils import get_partition_date_str

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "extractor" / "config.yaml"


@dataclass(frozen=True)
class ExtractorSpec:
    target: str
    tags: list[str]
    destination_key_template: str
    fetcher: str  # module:function


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


def _should_run_target(*, target: str, **context) -> bool:  # noqa: ANN001
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    targets = conf.get("targets")
    if not targets:
        return True
    if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
        raise ValueError("dag_run.conf.targets must be a list of strings")
    return target in {str(t) for t in targets}


def _fetch_to_tmp(*, target: str, fetcher: str, **context) -> dict[str, object]:  # noqa: ANN001
    fetcher_callable = _resolve_fetcher(fetcher)
    payload = fetcher_callable()
    if payload is None or payload.record_count == 0:
        logger.info("No data for %s; skip write_raw.", target)
        return {"has_data": 0, "record_count": 0}

    run_id = str(context.get("run_id") or context.get("dag_run").run_id)
    base_dir = Path(os.getenv("EXTRACTOR_TMP_DIR", "/tmp")) / "dw_extractor"
    tmp_dir = base_dir / _safe_segment(run_id) / _safe_segment(target)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    tmp_file = tmp_dir / "data.csv"
    tmp_file.write_bytes(payload.csv_bytes)

    logger.info("Fetched %s records for %s into %s", payload.record_count, target, tmp_file)
    return {"has_data": 1, "record_count": payload.record_count, "tmp_file": str(tmp_file)}


def _write_raw(
    *, target: str, destination_key_template: str, task_group_id: str, **context
) -> str | None:  # noqa: ANN001,E501
    ti = context["ti"]
    fetched = ti.xcom_pull(task_ids=f"{task_group_id}.fetch") or {}
    if not int(fetched.get("has_data", 0)):
        return None

    tmp_file = fetched.get("tmp_file")
    if not tmp_file:
        raise ValueError("Missing tmp_file from fetch step")

    partition_date = get_partition_date_str()
    destination_key = destination_key_template.format(PARTITION_DATE=partition_date)

    uploader = S3Uploader()
    s3_path = uploader.upload_file(tmp_file, key=destination_key, replace=True)

    try:
        Path(tmp_file).unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to delete tmp file %s", tmp_file)

    logger.info("write_raw done: target=%s s3_path=%s", target, s3_path)
    return s3_path


def _derive_target(entry: Mapping[str, object]) -> str:
    target = str(entry.get("target", "")).strip()
    if target:
        return target

    tags = entry.get("tags") or []
    if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes)):
        for tag in tags:
            text = str(tag)
            if text.count("_") >= 2:
                return text

    task_id = str(entry.get("task_id", "")).strip()
    if task_id.startswith("fetch_"):
        return task_id[len("fetch_") :]
    return task_id


def load_extractor_specs(path: Path = CONFIG_PATH) -> list[ExtractorSpec]:
    if not path.exists():
        raise FileNotFoundError(f"Extractor config not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("extractor config must be a mapping")
    extractors = raw.get("extractors")
    if not isinstance(extractors, Sequence) or isinstance(extractors, (str, bytes)):
        raise ValueError("extractor config must contain 'extractors' list")

    specs: list[ExtractorSpec] = []
    for entry in extractors:
        if not isinstance(entry, Mapping):
            raise ValueError("each extractor entry must be a mapping")

        target = _derive_target(entry)
        destination_key_template = str(entry.get("destination_key_template", "")).strip()
        fetcher = str(entry.get("fetcher", "")).strip()
        tags_raw = entry.get("tags") or []
        if not isinstance(tags_raw, Sequence) or isinstance(tags_raw, (str, bytes)):
            raise ValueError(f"tags must be a list for target={target}")

        if not target or not destination_key_template or not fetcher:
            raise ValueError(f"invalid extractor entry for target={target}: {entry}")

        specs.append(
            ExtractorSpec(
                target=target,
                tags=[str(t) for t in tags_raw],
                destination_key_template=destination_key_template,
                fetcher=fetcher,
            )
        )

    # Ensure stable ordering in UI
    return sorted(specs, key=lambda s: s.target)


def create_dw_extractor_dag() -> DAG:
    specs = load_extractor_specs()

    dag = DAG(
        dag_id="dw_extractor",
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["extractor"],
        max_active_runs=1,
    )

    with dag:
        for spec in specs:
            with TaskGroup(group_id=spec.target) as group:
                check = ShortCircuitOperator(
                    task_id="check",
                    python_callable=_should_run_target,
                    op_kwargs={"target": spec.target},
                )

                fetch = PythonOperator(
                    task_id="fetch",
                    python_callable=_fetch_to_tmp,
                    op_kwargs={"target": spec.target, "fetcher": spec.fetcher},
                )

                write_raw = PythonOperator(
                    task_id="write_raw",
                    python_callable=_write_raw,
                    op_kwargs={
                        "target": spec.target,
                        "destination_key_template": spec.destination_key_template,
                        "task_group_id": spec.target,
                    },
                )

                done = EmptyOperator(task_id="done")

                check >> fetch >> write_raw >> done

            # Expose groups at DAG level
            group  # noqa: B018

    return dag


dag = create_dw_extractor_dag()
