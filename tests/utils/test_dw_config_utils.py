import sys
from pathlib import Path

import pytest

from lakehouse_core.dw_config import (
    DWConfigError,
    SourceSpec,
    TableSpec,
    discover_tables_for_layer,
    load_dw_config,
    order_layers,
    order_tables_within_layer,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


def test_load_dw_config_and_order_layers(tmp_path: Path):
    config_path = tmp_path / "dw_config.yaml"
    config_path.write_text(
        """
layer_dependencies:
  dwd: ["ods"]
  dim: ["dwd"]

sources:
  ods_example:
    path: lake/raw/example
    format: csv

table_dependencies:
  dwd:
    dwd_table_b: ["dwd_table_a"]
  dim:
    dim_table_b: ["dim_table_a"]
        """,
        encoding="utf-8",
    )

    config = load_dw_config(config_path)
    assert config.layer_dependencies["dim"] == ["dwd"]
    assert config.table_dependencies["dwd"]["dwd_table_b"] == ["dwd_table_a"]
    assert config.sources["ods_example"] == SourceSpec(path="lake/raw/example", format="csv")

    ordered_layers = order_layers(config)
    assert ordered_layers == ["ods", "dwd", "dim"]


def test_load_dw_config_detects_layer_cycle(tmp_path: Path):
    config_path = tmp_path / "dw_config.yaml"
    config_path.write_text(
        """
layer_dependencies:
  dwd: ["dim"]
  dim: ["dwd"]
        """,
        encoding="utf-8",
    )

    with pytest.raises(DWConfigError):
        load_dw_config(config_path)


def test_discover_tables_requires_layer_prefix(tmp_path: Path):
    layer_dir = tmp_path / "dwd"
    layer_dir.mkdir()
    good_sql = layer_dir / "dwd_good.sql"
    bad_sql = layer_dir / "bad.sql"
    good_sql.write_text("SELECT 1", encoding="utf-8")
    bad_sql.write_text("SELECT 1", encoding="utf-8")

    tables = discover_tables_for_layer("dwd", tmp_path)
    assert tables == [
        TableSpec(layer="dwd", name="dwd_good", sql_path=good_sql, is_partitioned=False)
    ]

    assert discover_tables_for_layer("dim", tmp_path) == []


def test_order_tables_within_layer_toposorts(tmp_path: Path):
    tables = [
        TableSpec(layer="dwd", name="dwd_a", sql_path=tmp_path / "dwd_a.sql"),
        TableSpec(layer="dwd", name="dwd_b", sql_path=tmp_path / "dwd_b.sql"),
        TableSpec(layer="dwd", name="dwd_c", sql_path=tmp_path / "dwd_c.sql"),
    ]
    dependencies = {"dwd_c": ["dwd_b"], "dwd_b": ["dwd_a"]}

    ordered = order_tables_within_layer(tables, dependencies)
    assert [table.name for table in ordered] == ["dwd_a", "dwd_b", "dwd_c"]

    with pytest.raises(DWConfigError):
        order_tables_within_layer(tables, {"unknown": ["dwd_a"]})

    with pytest.raises(DWConfigError):
        order_tables_within_layer(tables, {"dwd_a": ["missing"]})
