from __future__ import annotations

from pathlib import Path

import pytest

from dags.extractor.backfill_specs import load_backfill_specs


def test_load_backfill_specs_empty_ok(tmp_path: Path) -> None:
    path = tmp_path / "backfill.yaml"
    path.write_text("backfill_extractors: []\n", encoding="utf-8")
    assert load_backfill_specs(path) == []


def test_load_backfill_specs_validates_required_fields(tmp_path: Path) -> None:
    path = tmp_path / "backfill.yaml"
    path.write_text(
        """
backfill_extractors:
  - target: x
    tags: ["extractor", "backfill"]
    universe: {from_target: u, symbol_column: symbol}
    history_fetcher: "m:f"
    pieces_base_prefix: "lake/raw/backfill/x/y"
    daily_key_template: "lake/raw/daily/x/y/dt={TRADE_DATE}/data.csv"
""".lstrip(),
        encoding="utf-8",
    )
    specs = load_backfill_specs(path)
    assert len(specs) == 1
    assert specs[0].target == "x"


def test_load_backfill_specs_rejects_bad_shard_type(tmp_path: Path) -> None:
    path = tmp_path / "backfill.yaml"
    path.write_text(
        """
backfill_extractors:
  - target: x
    universe: {from_target: u}
    history_fetcher: "m:f"
    shard_type: yearly
    pieces_base_prefix: "lake/raw/backfill/x/y"
    daily_key_template: "lake/raw/daily/x/y/dt={TRADE_DATE}/data.csv"
""".lstrip(),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_backfill_specs(path)
