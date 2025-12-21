from __future__ import annotations

from pathlib import Path

import pytest

from lakehouse_core.domain.models import RunContext
from lakehouse_core.planning import DirectoryDWPlanner


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_directory_dw_planner_orders_layers_and_tables(tmp_path: Path) -> None:
    dw_config = tmp_path / "dw_config.yaml"
    sql_base = tmp_path / "dags"

    _write(
        dw_config,
        """
layer_dependencies:
  ods: []
  dwd: ["ods"]

sources:
  ods_foo:
    path: "lake/raw/foo"
    format: "csv"

table_dependencies:
  ods:
    ods_foo: []
  dwd:
    dwd_bar: []
""".lstrip(),
    )
    _write(sql_base / "ods" / "ods_foo.sql", "SELECT 1 AS x, '${PARTITION_DATE}' AS dt;")
    _write(
        sql_base / "dwd" / "dwd_bar.sql", "SELECT * FROM ods.ods_foo WHERE dt='${PARTITION_DATE}';"
    )

    planner = DirectoryDWPlanner(dw_config_path=dw_config, sql_base_dir=sql_base)
    context = RunContext(run_id="r1", partition_date="2025-01-02")
    specs = planner.build(context)

    assert [spec.name for spec in specs] == ["ods.ods_foo", "dwd.dwd_bar"]
    assert specs[0].inputs["source"]["path"] == "lake/raw/foo"
    assert specs[0].partition_date == "2025-01-02"


def test_directory_dw_planner_supports_date_ranges(tmp_path: Path) -> None:
    dw_config = tmp_path / "dw_config.yaml"
    sql_base = tmp_path / "dags"

    _write(
        dw_config,
        """
layer_dependencies:
  dwd: []

sources: {}

table_dependencies:
  dwd:
    dwd_bar: []
""".lstrip(),
    )
    _write(sql_base / "dwd" / "dwd_bar.sql", "SELECT '${PARTITION_DATE}' AS dt;")

    planner = DirectoryDWPlanner(dw_config_path=dw_config, sql_base_dir=sql_base)
    context = RunContext(run_id="r1", start_date="2025-01-01", end_date="2025-01-02")
    specs = planner.build(context)

    assert [spec.name for spec in specs] == ["dwd.dwd_bar:2025-01-01", "dwd.dwd_bar:2025-01-02"]
    assert [spec.partition_date for spec in specs] == ["2025-01-01", "2025-01-02"]


def test_directory_dw_planner_filters_targets(tmp_path: Path) -> None:
    dw_config = tmp_path / "dw_config.yaml"
    sql_base = tmp_path / "dags"

    _write(
        dw_config,
        """
layer_dependencies:
  ods: []
  dwd: ["ods"]

sources:
  ods_foo:
    path: "lake/raw/foo"
    format: "csv"

table_dependencies:
  ods:
    ods_foo: []
  dwd:
    dwd_bar: []
""".lstrip(),
    )
    _write(sql_base / "ods" / "ods_foo.sql", "SELECT '${PARTITION_DATE}' AS dt;")
    _write(sql_base / "dwd" / "dwd_bar.sql", "SELECT '${PARTITION_DATE}' AS dt;")

    planner = DirectoryDWPlanner(dw_config_path=dw_config, sql_base_dir=sql_base)

    specs = planner.build(
        RunContext(run_id="r1", partition_date="2025-01-01", targets=["dwd.dwd_bar"])
    )
    assert [spec.name for spec in specs] == ["dwd.dwd_bar"]

    specs = planner.build(RunContext(run_id="r1", partition_date="2025-01-01", targets=["ods"]))
    assert [spec.name for spec in specs] == ["ods.ods_foo"]


def test_directory_dw_planner_requires_partition_date_for_partitioned_tables(
    tmp_path: Path,
) -> None:
    dw_config = tmp_path / "dw_config.yaml"
    sql_base = tmp_path / "dags"

    _write(
        dw_config,
        """
layer_dependencies:
  dwd: []
sources: {}
table_dependencies:
  dwd:
    dwd_bar: []
""".lstrip(),
    )
    _write(sql_base / "dwd" / "dwd_bar.sql", "SELECT '${PARTITION_DATE}' AS dt;")

    planner = DirectoryDWPlanner(dw_config_path=dw_config, sql_base_dir=sql_base)
    with pytest.raises(ValueError, match="partition_date is required"):
        planner.build(RunContext(run_id="r1"))
