from __future__ import annotations

from pathlib import Path

from dags.extractor.increment.dw_extractor_dag import load_per_target_providers


def test_load_per_target_providers_reads_nested_provider(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        """
extractors:
  - target: a
    tags: []
    destination_key_template: "lake/raw/daily/a/dt={PARTITION_DATE}/data.csv"
    fetcher: "some.module:fetch"
    provider:
      callable: "some.module:provide"
      kwargs:
        sql: "select 1"
  - target: b
    tags: []
    destination_key_template: "lake/raw/daily/b/dt={PARTITION_DATE}/data.csv"
    fetcher: "some.module:fetch"
""".lstrip(),
        encoding="utf-8",
    )

    providers = load_per_target_providers(config)
    assert providers["a"][0] == "some.module:provide"
    assert providers["a"][1]["sql"] == "select 1"
    assert "b" not in providers
