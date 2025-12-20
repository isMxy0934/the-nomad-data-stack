from __future__ import annotations

from pathlib import Path

from scripts.run_dw import main


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_run_dw_local_smoke(tmp_path: Path) -> None:
    dw_config = tmp_path / "dw_config.yaml"
    sql_base = tmp_path / "dags"
    local_root = tmp_path / "lake_root"

    _write(
        dw_config,
        """
layer_dependencies:
  dwd: []

sources: {}

table_dependencies:
  dwd:
    dwd_smoke: []
""".lstrip(),
    )
    _write(
        sql_base / "dwd" / "dwd_smoke.sql",
        "SELECT 1 AS x, DATE '${PARTITION_DATE}' AS dt;",
    )

    rc = main(
        [
            "--store",
            "local",
            "--local-root",
            str(local_root),
            "--dw-config",
            str(dw_config),
            "--sql-base-dir",
            str(sql_base),
            "--dt",
            "2025-01-01",
            "--run-id",
            "test",
            "--targets",
            "dwd.dwd_smoke",
        ]
    )
    assert rc == 0

    canonical_dir = local_root / "lake" / "dwd" / "dwd_smoke" / "dt=2025-01-01"
    assert (canonical_dir / "manifest.json").exists()
    assert (canonical_dir / "_SUCCESS").exists()
    assert any(p.suffix == ".parquet" for p in canonical_dir.rglob("*.parquet"))

