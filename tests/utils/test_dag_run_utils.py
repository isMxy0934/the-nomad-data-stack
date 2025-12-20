import sys
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.utils.dag_run_utils import build_downstream_conf, parse_targets
except ImportError as exc:
    pytest.skip(
        f"dag run utils imports unavailable in this environment: {exc}", allow_module_level=True
    )


def test_parse_targets_none():
    assert parse_targets(None) is None
    assert parse_targets({}) is None
    assert parse_targets({"targets": None}) is None


def test_parse_targets_list():
    assert parse_targets({"targets": ["layer.table1", "layer.table2"]}) == [
        "layer.table1",
        "layer.table2",
    ]


def test_parse_targets_json_string():
    assert parse_targets({"targets": '["layer.table1", "layer.table2"]'}) == [
        "layer.table1",
        "layer.table2",
    ]


def test_parse_targets_single_string():
    assert parse_targets({"targets": "layer.table1"}) == ["layer.table1"]


def test_parse_targets_empty_values():
    assert parse_targets({"targets": "[]"}) is None
    assert parse_targets({"targets": ""}) is None
    assert parse_targets({"targets": " "}) is None


def test_parse_targets_invalid_format():
    with pytest.raises(ValueError, match="must use layer.table format"):
        parse_targets({"targets": ["table_without_layer"]})


def test_parse_targets_wildcard_error():
    with pytest.raises(ValueError, match="does not support wildcard"):
        parse_targets({"targets": ["layer.*"]})


def test_build_downstream_conf():
    conf = {"partition_date": "2024-01-01", "targets": ["ods.table1"]}
    result = build_downstream_conf(conf)
    assert result["partition_date"] == "2024-01-01"
    assert result["targets"] == ["ods.table1"]
    assert "start_date" in result
    assert result["start_date"] is None
