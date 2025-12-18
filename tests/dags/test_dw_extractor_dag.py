from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DAG  # noqa: E402

from dags.extractor.dw_extractor_dag import (  # noqa: E402
    create_dw_extractor_dag,
    load_extractor_specs,
)


def test_dw_extractor_dag_builds_expected_groups():
    dag: DAG = create_dw_extractor_dag()
    assert dag.dag_id == "dw_extractor_dag"

    specs = load_extractor_specs()
    for spec in specs:
        assert f"{spec.target}.check" in dag.task_ids
        assert f"{spec.target}.fetch" in dag.task_ids
        assert f"{spec.target}.write_raw" in dag.task_ids
        assert f"{spec.target}.done" in dag.task_ids

    assert "all_done" in dag.task_ids
    assert "trigger_dw_catalog_dag" in dag.task_ids
