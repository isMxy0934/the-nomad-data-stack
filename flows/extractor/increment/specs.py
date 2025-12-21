from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import yaml

from dags.utils.extractor_utils import ExtractorSpec

CONFIG_PATH = Path(__file__).resolve().parents[3] / "dags" / "extractor" / "increment" / "config.yaml"


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

    return sorted(specs, key=lambda s: s.target)
