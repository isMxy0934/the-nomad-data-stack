"""Unified extractor DAG (daily, config-driven).

This DAG consolidates all extractors into one `dw_extractor_dag` DAG.

It supports two execution modes:
1) Default (no provider): run all configured extractors in `config.yaml`.
2) Provider-driven (dynamic): run a Python "provider" that returns a list of
   extractor jobs, then dynamically map `fetch -> write_raw` per job.

Provider config is usually per-target (optional) in `dags/extractor/increment/config.yaml`:
  extractors:
    - target: some_extractor
      provider:
        callable: "dags.extractor.increment.target_providers:provide_jobs_from_catalog"
        kwargs:
          sql: "SELECT ... FROM ods.some_whitelist"

Optional global provider (rare; applies to all targets) can also be configured at top-level:
  provider:
    callable: "..."
    kwargs: {...}

Each returned job must be a mapping containing at least:
  {"target": "<extractor_target>"}
and can optionally include:
  {"fetcher_kwargs": {...}}  # forwarded into the fetcher callable

Runtime filtering (optional):
  dag_run.conf.extractor_targets = ["etf_universe_whitelist", ...]

Note: `dag_run.conf.targets` is reserved for DW layer targets (layer.table) and
is passed downstream to the DW pipeline via `dw_catalog_dag`.
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from dags.utils.extractor_utils import CsvPayload, ExtractorSpec
from dags.utils.s3_utils import S3Uploader
from lakehouse_core.io.time import get_partition_date_str

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.yaml"

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

ProviderCallable = Callable[..., object]
GLOBAL_PROVIDER_TARGET = "__global_provider__"


def _safe_segment(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value).strip("_") or "x"


def _resolve_fetcher(fetcher_ref: str) -> Callable[..., CsvPayload | None]:
    if ":" not in fetcher_ref:
        raise ValueError(f"Invalid fetcher reference (expected module:function): {fetcher_ref}")
    module_name, function_name = fetcher_ref.split(":", 1)
    module = importlib.import_module(module_name)
    fetcher = getattr(module, function_name, None)
    if fetcher is None or not callable(fetcher):
        raise ValueError(f"Fetcher not found or not callable: {fetcher_ref}")
    return fetcher


def _resolve_callable(ref: str) -> ProviderCallable:
    if ":" not in ref:
        raise ValueError(f"Invalid callable reference (expected module:function): {ref}")
    module_name, function_name = ref.split(":", 1)
    module = importlib.import_module(module_name)
    fn = getattr(module, function_name, None)
    if fn is None or not callable(fn):
        raise ValueError(f"Callable not found or not callable: {ref}")
    return cast(ProviderCallable, fn)


def _parse_extractor_targets(conf: Mapping[str, Any]) -> set[str] | None:
    raw = conf.get("extractor_targets")
    if not raw:
        return None
    if isinstance(raw, str):
        return {raw.strip()} if raw.strip() else None
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("dag_run.conf.extractor_targets must be a list of strings")
    targets = {str(t).strip() for t in raw if str(t).strip()}
    return targets or None


def _fetch_to_tmp(
    *,
    target: str,
    fetcher: str,
    fetcher_kwargs: Mapping[str, object] | None = None,
    run_id: str,
) -> dict[str, object]:  # noqa: ANN001,E501
    fetcher_callable = _resolve_fetcher(fetcher)
    fetcher_kwargs = fetcher_kwargs or {}
    try:
        payload = fetcher_callable(**fetcher_kwargs)
    except TypeError:
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


def _write_raw(
    *,
    target: str,
    destination_key_template: str,
    fetched: Mapping[str, object],
    partition_date: str,
) -> str | None:  # noqa: ANN001,E501
    if not int(fetched.get("has_data", 0)):
        return None

    tmp_file = fetched.get("tmp_file")
    if not tmp_file:
        raise ValueError("Missing tmp_file from fetch step")

    partition_date = str(partition_date or "").strip() or get_partition_date_str()
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


def load_target_provider(path: Path = CONFIG_PATH) -> tuple[str, dict[str, object]] | None:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("extractor config must be a mapping")
    provider = raw.get("provider")
    if not provider:
        return None
    if not isinstance(provider, Mapping):
        raise ValueError("provider must be a mapping")
    callable_ref = str(provider.get("callable", "")).strip()
    if not callable_ref:
        raise ValueError("provider.callable is required when provider is configured")
    kwargs_raw = provider.get("kwargs") or {}
    if not isinstance(kwargs_raw, Mapping):
        raise ValueError("provider.kwargs must be a mapping")
    return callable_ref, {str(k): cast(object, v) for k, v in kwargs_raw.items()}


def load_per_target_providers(
    path: Path = CONFIG_PATH,
) -> dict[str, tuple[str, dict[str, object]]]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("extractor config must be a mapping")
    extractors = raw.get("extractors")
    if not isinstance(extractors, Sequence) or isinstance(extractors, (str, bytes)):
        raise ValueError("extractor config must contain 'extractors' list")

    out: dict[str, tuple[str, dict[str, object]]] = {}
    for entry in extractors:
        if not isinstance(entry, Mapping):
            raise ValueError("each extractor entry must be a mapping")
        provider = entry.get("provider")
        if not provider:
            continue
        if not isinstance(provider, Mapping):
            raise ValueError("extractor entry provider must be a mapping")
        target = _derive_target(entry)
        callable_ref = str(provider.get("callable", "")).strip()
        if not callable_ref:
            raise ValueError(f"provider.callable is required for target={target}")
        kwargs_raw = provider.get("kwargs") or {}
        if not isinstance(kwargs_raw, Mapping):
            raise ValueError(f"provider.kwargs must be a mapping for target={target}")
        out[target] = (callable_ref, {str(k): cast(object, v) for k, v in kwargs_raw.items()})
    return out


def _spec_index(specs: Sequence[ExtractorSpec]) -> dict[str, ExtractorSpec]:
    out: dict[str, ExtractorSpec] = {}
    for spec in specs:
        if spec.target in out:
            raise ValueError(f"Duplicate extractor target in config: {spec.target}")
        out[spec.target] = spec
    return out


def _normalize_jobs_for_target(
    *,
    target: str,
    result: object,
    spec_by_target: Mapping[str, ExtractorSpec],
) -> list[dict[str, object]]:
    if result is None:
        return [{"target": target}]
    if not isinstance(result, Sequence) or isinstance(result, (str, bytes)):
        raise TypeError("provider must return a list of job mappings (or None)")
    jobs_out: list[dict[str, object]] = []
    for item in result:
        if not isinstance(item, Mapping):
            raise TypeError("each job returned by provider must be a mapping")
        has_target_key = "target" in item
        item_target = str(item.get("target") or target).strip()
        if not item_target:
            raise ValueError("job missing required 'target'")
        if item_target not in spec_by_target:
            available = ", ".join(sorted(spec_by_target.keys()))
            if (
                has_target_key
                and item_target != target
                and target in spec_by_target
                and "fetcher_kwargs" not in item
                and len(item) == 1
            ):
                raise ValueError(
                    f"Unknown extractor target from provider: {item_target} "
                    f"(available: {available}). "
                    "This usually means your provider is returning a list of symbols/values "
                    "but putting them under the reserved key 'target'. "
                    "For target-level expansion, return {'fetcher_kwargs': {...}} "
                    "or use provide_jobs_from_catalog_values/provide_fetcher_kwargs_from_catalog."
                )
            raise ValueError(
                f"Unknown extractor target from provider: {item_target} (available: {available})"
            )
        fetcher_kwargs = item.get("fetcher_kwargs")
        if fetcher_kwargs is None:
            if not has_target_key:
                job = {"target": item_target, "fetcher_kwargs": dict(item)}
            else:
                job = {"target": item_target}
        else:
            if not isinstance(fetcher_kwargs, Mapping):
                raise TypeError("job.fetcher_kwargs must be a mapping when provided")
            job = {"target": item_target, "fetcher_kwargs": dict(fetcher_kwargs)}
        jobs_out.append(job)
    return jobs_out


def _normalize_global_jobs(
    *,
    result: object,
    spec_by_target: Mapping[str, ExtractorSpec],
    specs: Sequence[ExtractorSpec],
) -> list[dict[str, object]]:
    if result is None:
        return [{"target": spec.target} for spec in specs]
    if not isinstance(result, Sequence) or isinstance(result, (str, bytes)):
        raise TypeError("provider must return a list of job mappings (or None)")
    jobs_out: list[dict[str, object]] = []
    for item in result:
        if not isinstance(item, Mapping):
            raise TypeError("each job returned by provider must be a mapping")
        item_target = str(item.get("target", "")).strip()
        if not item_target:
            raise ValueError("job missing required 'target'")
        if item_target not in spec_by_target:
            available = ", ".join(sorted(spec_by_target.keys()))
            raise ValueError(
                f"Unknown extractor target from provider: {item_target} (available: {available})"
            )
        fetcher_kwargs = item.get("fetcher_kwargs")
        if fetcher_kwargs is None:
            job: dict[str, object] = {"target": item_target}
        else:
            if not isinstance(fetcher_kwargs, Mapping):
                raise TypeError("job.fetcher_kwargs must be a mapping when provided")
            job = {"target": item_target, "fetcher_kwargs": dict(fetcher_kwargs)}
        jobs_out.append(job)
    return jobs_out


def _call_provider(
    *,
    callable_ref: str,
    kwargs: Mapping[str, object],
    target: str,
    spec: ExtractorSpec,
    specs: Sequence[ExtractorSpec],
    conf: Mapping[str, object],
) -> object:
    fn = _resolve_callable(callable_ref)
    try:
        return fn(spec=spec, target=target, specs=specs, conf=conf, **kwargs)
    except TypeError:
        try:
            return fn(specs=specs, conf=conf, **kwargs)
        except TypeError:
            return fn(**kwargs)


def create_dw_extractor_dag() -> DAG:
    dag = DAG(
        dag_id=DAG_ID,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["extractor"],
        max_active_runs=1,
        default_args=default_args,
    )

    with dag:
        specs = load_extractor_specs()
        spec_by_target = _spec_index(specs)
        provider = load_target_provider()
        per_target_providers = load_per_target_providers()

        @task(task_id="resolve_jobs")
        def resolve_jobs() -> list[str]:
            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            conf = (dag_run.conf or {}) if dag_run else {}

            if bool(conf.get("init")):
                return []

            allow_targets = _parse_extractor_targets(conf)
            if provider is not None:
                return [GLOBAL_PROVIDER_TARGET]

            targets = [spec.target for spec in specs]
            if allow_targets is not None:
                targets = [target for target in targets if target in allow_targets]
            return sorted(targets)

        @task(task_id="run_provider")
        def run_provider(target: str) -> list[dict[str, object]]:
            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            conf = (dag_run.conf or {}) if dag_run else {}

            if target == GLOBAL_PROVIDER_TARGET:
                if provider is None:
                    return []
                callable_ref, kwargs = provider
                fn = _resolve_callable(callable_ref)
                try:
                    result = fn(specs=specs, conf=conf, **kwargs)
                except TypeError:
                    result = fn(**kwargs)
                return _normalize_global_jobs(
                    result=result, spec_by_target=spec_by_target, specs=specs
                )

            spec = spec_by_target[target]
            per = per_target_providers.get(target)
            if per is None:
                return [{"target": target}]

            callable_ref, kwargs = per
            result = _call_provider(
                callable_ref=callable_ref,
                kwargs=kwargs,
                target=target,
                spec=spec,
                specs=specs,
                conf=conf,
            )
            return _normalize_jobs_for_target(
                target=target, result=result, spec_by_target=spec_by_target
            )

        @task(task_id="merge_jobs")
        def merge_jobs(provider_results: list[list[dict[str, object]]]) -> list[dict[str, object]]:
            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            conf = (dag_run.conf or {}) if dag_run else {}
            allow_targets = _parse_extractor_targets(conf)

            flattened: list[dict[str, object]] = []
            for group in provider_results or []:
                if not group:
                    continue
                flattened.extend(group)

            if allow_targets is not None:
                flattened = [
                    job for job in flattened if str(job.get("target", "")).strip() in allow_targets
                ]

            enriched: list[dict[str, object]] = []
            for job in flattened:
                target = str(job.get("target", "")).strip()
                if not target:
                    continue
                spec = spec_by_target[target]
                fetcher_kwargs_raw = job.get("fetcher_kwargs") or {}
                if not isinstance(fetcher_kwargs_raw, Mapping):
                    raise TypeError("job.fetcher_kwargs must be a mapping")
                enriched.append(
                    {
                        "target": target,
                        "fetcher": spec.fetcher,
                        "destination_key_template": spec.destination_key_template,
                        "fetcher_kwargs": dict(fetcher_kwargs_raw),
                    }
                )

            enriched.sort(key=lambda j: str(j.get("target")))
            return enriched

        @task(
            task_id="fetch_to_tmp",
            retries=default_args["retries"],
            retry_delay=default_args["retry_delay"],
        )
        def fetch_to_tmp(job: Mapping[str, object]) -> dict[str, object]:
            target = str(job.get("target", "")).strip()
            fetcher = str(job.get("fetcher", "")).strip()
            destination_key_template = str(job.get("destination_key_template", "")).strip()
            if not target or not fetcher or not destination_key_template:
                raise ValueError(f"Invalid job payload: {job}")

            fetcher_kwargs_raw = job.get("fetcher_kwargs") or {}
            if not isinstance(fetcher_kwargs_raw, Mapping):
                raise TypeError("job.fetcher_kwargs must be a mapping")

            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            ctx_run_id = ctx.get("run_id") or (dag_run.run_id if dag_run else None)
            if not ctx_run_id:
                raise ValueError("Missing run_id in Airflow context")

            fetched = _fetch_to_tmp(
                target=target,
                fetcher=fetcher,
                fetcher_kwargs=cast(Mapping[str, object], fetcher_kwargs_raw),
                run_id=str(ctx_run_id),
            )
            fetched["target"] = target
            fetched["destination_key_template"] = destination_key_template
            return fetched

        @task(
            task_id="write_raw",
            retries=default_args["retries"],
            retry_delay=default_args["retry_delay"],
        )
        def write_raw(fetched: Mapping[str, object]) -> str | None:
            target = str(fetched.get("target", "")).strip()
            if not target:
                raise ValueError("Missing target from fetch step")
            template = str(fetched.get("destination_key_template", "")).strip()
            if not template:
                raise ValueError("Missing destination_key_template from fetch step")

            ctx = get_current_context()
            dag_run = ctx.get("dag_run")
            conf = (dag_run.conf or {}) if dag_run else {}
            partition_date = str(conf.get("partition_date") or "").strip() or get_partition_date_str()
            return _write_raw(
                target=target,
                destination_key_template=template,
                fetched=fetched,
                partition_date=partition_date,
            )

        targets = resolve_jobs()
        provider_results = run_provider.expand(target=targets)
        jobs = merge_jobs(provider_results)
        fetched = fetch_to_tmp.expand(job=jobs)
        wrote = write_raw.expand(fetched=fetched)

        all_done = EmptyOperator(task_id="all_done", trigger_rule=TriggerRule.ALL_DONE)
        wrote >> all_done
        trigger_catalog = TriggerDagRunOperator(
            task_id="trigger_dw_catalog_dag",
            trigger_dag_id="dw_catalog_dag",
            wait_for_completion=False,
            reset_dag_run=True,
            trigger_rule=TriggerRule.ALL_DONE,
            conf={
                "partition_date": "{{ dag_run.conf.get('partition_date') if dag_run and dag_run.conf else None }}",
                "targets": "{{ dag_run.conf.get('targets') if dag_run and dag_run.conf else None }}",
                "init": "{{ dag_run.conf.get('init') if dag_run and dag_run.conf else None }}",
                "start_date": "{{ dag_run.conf.get('start_date') if dag_run and dag_run.conf else None }}",
                "end_date": "{{ dag_run.conf.get('end_date') if dag_run and dag_run.conf else None }}",
            },
        )
        all_done >> trigger_catalog

    return dag


dag = create_dw_extractor_dag()
