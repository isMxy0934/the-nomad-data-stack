from __future__ import annotations

import logging
import os
import shutil
import tempfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from lakehouse_core.domain.observability import log_event
from lakehouse_core.ingestion.loader import instantiate_component
from lakehouse_core.ingestion.settings import resolve_s3_settings
from lakehouse_core.io.time import get_partition_date_str
from lakehouse_core.store import Boto3S3Store
from lakehouse_core.store.object_store import ObjectStore

logger = logging.getLogger(__name__)


@dataclass
class IngestionRunResult:
    target: str
    status: str
    details: dict[str, Any]


def run_ingestion_configs(
    *,
    config_dir: Path,
    targets: Iterable[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    partition_date: str | None = None,
    run_id: str | None = None,
    write_mode: str = "overwrite",
    max_workers: int = 1,
    local_tmp_dir: Path | None = None,
    store: ObjectStore | None = None,
    base_uri: str | None = None,
) -> list[IngestionRunResult]:
    configs = list(Path(config_dir).glob("*.yaml"))
    results: list[IngestionRunResult] = []
    target_set = {t for t in (targets or []) if t}
    for config_path in configs:
        conf = _load_yaml(config_path)
        target = str(conf.get("target") or "")
        if target_set and target not in target_set:
            continue
        result = run_ingestion_config(
            config=conf,
            config_path=config_path,
            start_date=start_date,
            end_date=end_date,
            partition_date=partition_date,
            run_id=run_id,
            write_mode=write_mode,
            max_workers=max_workers,
            local_tmp_dir=local_tmp_dir,
            store=store,
            base_uri=base_uri,
        )
        results.append(result)
    return results


def run_ingestion_config(
    *,
    config: dict[str, Any] | None = None,
    config_path: Path | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    partition_date: str | None = None,
    run_id: str | None = None,
    write_mode: str = "overwrite",
    max_workers: int = 1,
    local_tmp_dir: Path | None = None,
    store: ObjectStore | None = None,
    base_uri: str | None = None,
) -> IngestionRunResult:
    if config is None:
        if not config_path:
            raise ValueError("config or config_path is required")
        config = _load_yaml(config_path)

    target = str(config.get("target") or "")
    if not target:
        raise ValueError("Config missing target")

    if partition_date is None:
        partition_date = get_partition_date_str()
    if start_date is None:
        start_date = partition_date
    if end_date is None:
        end_date = start_date

    if not run_id:
        run_id = f"manual_{partition_date.replace('-', '')}"
    safe_run_id = _sanitize_run_id(run_id)

    store, base_uri = _resolve_store(store, base_uri)
    temp_context: tempfile.TemporaryDirectory[str] | None = None
    if local_tmp_dir is None and not os.getenv("INGESTION_LOCAL_TMP"):
        temp_context = tempfile.TemporaryDirectory()
        local_root = Path(temp_context.name)
    else:
        local_root = _resolve_local_tmp(local_tmp_dir)
    run_dir = local_root / f"run_{safe_run_id}"
    results_dir = run_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    partitioner = instantiate_component(config["partitioner"])
    extractor = instantiate_component(config["extractor"])
    compactor = instantiate_component(config["compactor"])

    jobs = list(partitioner.generate_jobs(start_date=start_date, end_date=end_date))
    if not jobs:
        log_event(logger, "ingestion.plan.empty", target=target)
        return IngestionRunResult(target=target, status="skipped", details={"reason": "no_jobs"})

    extracted: list[dict[str, Any]] = []
    try:
        extracted = _run_extractions(
            extractor=extractor,
            jobs=jobs,
            results_dir=results_dir,
            max_workers=max_workers,
        )
        if not extracted:
            return IngestionRunResult(
                target=target, status="skipped", details={"reason": "no_data"}
            )

        compactor_result = compactor.compact(
            results=extracted,
            target=target,
            partition_date=partition_date,
            store=store,
            base_uri=base_uri,
            run_id=safe_run_id,
            local_dir=run_dir,
            write_mode=write_mode,
        )
        return IngestionRunResult(target=target, status="ok", details=compactor_result)
    finally:
        if temp_context is None:
            shutil.rmtree(run_dir, ignore_errors=True)
        else:
            temp_context.cleanup()


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid ingestion config: {path}")
    return data


def _sanitize_run_id(run_id: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)


def _resolve_store(store: ObjectStore | None, base_uri: str | None) -> tuple[ObjectStore, str]:
    if store and base_uri:
        return store, base_uri
    if store and not base_uri:
        raise ValueError("base_uri is required when store is provided")

    settings = resolve_s3_settings()
    if not settings.bucket:
        raise ValueError("S3_BUCKET_NAME is required for ingestion runner")
    if not settings.endpoint_url or not settings.access_key or not settings.secret_key:
        raise ValueError("S3 endpoint/access/secret are required for ingestion runner")

    store = Boto3S3Store(
        endpoint_url=settings.endpoint_url,
        access_key=settings.access_key,
        secret_key=settings.secret_key,
        region=settings.region,
        use_ssl=settings.use_ssl,
        url_style=settings.url_style,
        session_token=settings.session_token,
    )
    return store, f"s3://{settings.bucket}"


def _resolve_local_tmp(local_tmp_dir: Path | None) -> Path:
    if local_tmp_dir:
        root = local_tmp_dir
    else:
        root = Path(os.getenv("INGESTION_LOCAL_TMP", "tmp/ingestion"))
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _run_extractions(
    *,
    extractor,
    jobs: list,
    results_dir: Path,
    max_workers: int,
) -> list[dict[str, Any]]:
    def _extract_one(idx: int, job) -> dict[str, Any] | None:
        df: pd.DataFrame | None = extractor.extract(job)
        if df is None or df.empty:
            return None
        path = results_dir / f"part_{idx:06d}.parquet"
        df.to_parquet(path, index=False)
        return {"local_path": str(path), "row_count": len(df), "job_index": idx}

    extracted: list[dict[str, Any]] = []
    if max_workers <= 1:
        for idx, job in enumerate(jobs):
            result = _extract_one(idx, job)
            if result:
                extracted.append(result)
        return extracted

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_extract_one, idx, job): idx for idx, job in enumerate(jobs)}
        for future in as_completed(futures):
            result = future.result()
            if result:
                extracted.append(result)

    extracted.sort(key=lambda item: item["job_index"])
    return extracted
