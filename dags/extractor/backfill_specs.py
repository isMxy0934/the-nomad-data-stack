from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

BACKFILL_CONFIG_PATH = Path(__file__).parent / "backfill_config.yaml"

@dataclass(frozen=True)
class UniverseSpec:
    from_target: str
    symbol_column: str = "symbol"


@dataclass(frozen=True)
class BackfillExtractorSpec:
    target: str
    tags: list[str]
    start_date: str
    end_date: str | None
    universe: UniverseSpec
    history_fetcher: str  # module:function
    shard_type: str = "monthly"  # monthly/none
    trade_date_column: str = "trade_date"
    pieces_base_prefix: str = ""  # key prefix without bucket
    daily_key_template: str = ""  # key path without bucket
    pool: str | None = None
    symbol_allowlist: list[str] | None = None
    compact_transformer: str | None = None  # module:function
    trigger_compact: bool = False


def backfill_spec_from_mapping(payload: Mapping[str, object]) -> BackfillExtractorSpec:
    start_date = str(payload.get("start_date", "")).strip()
    end_date = str(payload.get("end_date", "")).strip() if payload.get("end_date") else None
    trigger_compact = bool(payload.get("trigger_compact") or False)

    universe_raw = payload.get("universe")
    if isinstance(universe_raw, UniverseSpec):
        universe = universe_raw
    elif isinstance(universe_raw, Mapping):
        universe = UniverseSpec(
            from_target=str(universe_raw.get("from_target", "")).strip(),
            symbol_column=str(universe_raw.get("symbol_column", "symbol")).strip() or "symbol",
        )
    else:
        raise ValueError("Invalid spec payload: universe must be a mapping")

    return BackfillExtractorSpec(
        target=str(payload.get("target", "")).strip(),
        tags=[str(t) for t in (payload.get("tags") or [])],
        start_date=start_date,
        end_date=end_date,
        trigger_compact=trigger_compact,
        universe=universe,
        history_fetcher=str(payload.get("history_fetcher", "")).strip(),
        shard_type=str(payload.get("shard_type", "monthly")).strip() or "monthly",
        trade_date_column=str(payload.get("trade_date_column", "trade_date")).strip()
        or "trade_date",
        pieces_base_prefix=str(payload.get("pieces_base_prefix", "")).strip(),
        daily_key_template=str(payload.get("daily_key_template", "")).strip(),
        pool=str(payload.get("pool")).strip() if payload.get("pool") else None,
        symbol_allowlist=[str(s) for s in (payload.get("symbol_allowlist") or [])]
        if payload.get("symbol_allowlist")
        else None,
        compact_transformer=str(payload.get("compact_transformer")).strip()
        if payload.get("compact_transformer")
        else None,
    )


def _derive_target(entry: Mapping[str, object]) -> str:
    target = str(entry.get("target", "")).strip()
    if target:
        return target
    task_id = str(entry.get("task_id", "")).strip()
    if task_id:
        return task_id
    raise ValueError(f"Invalid backfill extractor entry (missing target): {entry}")


def load_backfill_specs(path: Path = BACKFILL_CONFIG_PATH) -> list[BackfillExtractorSpec]:
    if not path.exists():
        raise FileNotFoundError(f"Backfill config not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise ValueError("backfill config must be a mapping")

    extractors = raw.get("backfill_extractors") or []
    if not isinstance(extractors, Sequence) or isinstance(extractors, (str, bytes)):
        raise ValueError("backfill config must contain 'backfill_extractors' list")

    specs: list[BackfillExtractorSpec] = []
    for entry in extractors:
        if not isinstance(entry, Mapping):
            raise ValueError("each backfill extractor entry must be a mapping")

        target = _derive_target(entry)
        tags_raw = entry.get("tags") or []
        if not isinstance(tags_raw, Sequence) or isinstance(tags_raw, (str, bytes)):
            raise ValueError(f"tags must be a list for target={target}")

        start_date = str(entry.get("start_date", "")).strip()
        if not start_date:
            raise ValueError(f"start_date is required for target={target}")
        try:
            date.fromisoformat(start_date)
        except ValueError as exc:
            raise ValueError(f"Invalid start_date={start_date} for target={target}") from exc

        end_date = str(entry.get("end_date", "")).strip() if entry.get("end_date") else None
        if end_date:
            try:
                date.fromisoformat(end_date)
            except ValueError as exc:
                raise ValueError(f"Invalid end_date={end_date} for target={target}") from exc

        trigger_compact = entry.get("trigger_compact")
        if trigger_compact is None:
            trigger_compact = False
        if not isinstance(trigger_compact, bool):
            raise ValueError(f"trigger_compact must be a boolean for target={target}")

        universe_raw = entry.get("universe") or {}
        if not isinstance(universe_raw, Mapping):
            raise ValueError(f"universe must be a mapping for target={target}")
        from_target = str(universe_raw.get("from_target", "")).strip()
        symbol_column = str(universe_raw.get("symbol_column", "symbol")).strip() or "symbol"
        if not from_target:
            raise ValueError(f"universe.from_target is required for target={target}")

        history_fetcher = str(entry.get("history_fetcher", "")).strip()
        if not history_fetcher:
            raise ValueError(f"history_fetcher is required for target={target}")

        shard_type = str(entry.get("shard_type", "monthly")).strip() or "monthly"
        if shard_type not in {"monthly", "none"}:
            raise ValueError(f"Unsupported shard_type={shard_type} for target={target}")

        trade_date_column = (
            str(entry.get("trade_date_column", "trade_date")).strip() or "trade_date"
        )
        pieces_base_prefix = str(entry.get("pieces_base_prefix", "")).strip()
        daily_key_template = str(entry.get("daily_key_template", "")).strip()
        if not pieces_base_prefix or not daily_key_template:
            raise ValueError(
                f"pieces_base_prefix and daily_key_template are required for target={target}"
            )

        pool = entry.get("pool")
        pool_name = str(pool).strip() if pool is not None else None
        if pool_name == "":
            pool_name = None

        symbol_allowlist = entry.get("symbol_allowlist")
        if symbol_allowlist is not None:
            if isinstance(symbol_allowlist, str):
                raise ValueError(
                    f"symbol_allowlist must be a list, not a string: '{symbol_allowlist}' for target={target}"
                )
            if not isinstance(symbol_allowlist, Sequence):
                raise ValueError(f"symbol_allowlist must be a sequence/list for target={target}")

        allowlist = [str(s) for s in symbol_allowlist] if symbol_allowlist else None

        transformer = entry.get("compact_transformer")
        transformer_ref = str(transformer).strip() if transformer is not None else None
        if transformer_ref == "":
            transformer_ref = None

        specs.append(
            BackfillExtractorSpec(
                target=target,
                tags=[str(t) for t in tags_raw],
                start_date=start_date,
                end_date=end_date,
                trigger_compact=trigger_compact,
                universe=UniverseSpec(from_target=from_target, symbol_column=symbol_column),
                history_fetcher=history_fetcher,
                shard_type=shard_type,
                trade_date_column=trade_date_column,
                pieces_base_prefix=pieces_base_prefix,
                daily_key_template=daily_key_template,
                pool=pool_name,
                symbol_allowlist=allowlist,
                compact_transformer=transformer_ref,
            )
        )

    return sorted(specs, key=lambda s: s.target)
