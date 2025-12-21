import pytest

from flows.utils.dag_run_utils import build_downstream_conf, parse_targets


def test_parse_targets_none() -> None:
    assert parse_targets({}) is None
    assert parse_targets({"targets": []}) is None


def test_parse_targets_list() -> None:
    assert parse_targets({"targets": ["ods.table_a"]}) == ["ods.table_a"]


def test_parse_targets_json_string() -> None:
    assert parse_targets({"targets": '["ods.table_a","dwd.table_b"]'}) == [
        "ods.table_a",
        "dwd.table_b",
    ]


def test_parse_targets_reject_wildcard() -> None:
    with pytest.raises(ValueError, match="wildcard"):
        parse_targets({"targets": ["ods.*"]})


def test_parse_targets_requires_layer_table() -> None:
    with pytest.raises(ValueError, match="layer.table"):
        parse_targets({"targets": ["ods_only"]})


def test_build_downstream_conf() -> None:
    conf = build_downstream_conf(
        {
            "partition_date": "2024-01-01",
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
            "init": True,
            "targets": '["ods.table_a"]',
        }
    )
    assert conf["partition_date"] == "2024-01-01"
    assert conf["start_date"] == "2024-01-01"
    assert conf["end_date"] == "2024-01-02"
    assert conf["init"] is True
    assert conf["targets"] == ["ods.table_a"]
