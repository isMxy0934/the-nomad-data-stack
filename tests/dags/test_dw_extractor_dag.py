from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DAG  # noqa: E402

from dags.dw_extractor import create_dw_extractor_dag, load_extractor_specs  # noqa: E402


def test_dw_extractor_dag_builds_expected_groups():
    dag: DAG = create_dw_extractor_dag()
    assert dag.dag_id == "dw_extractor"

    specs = load_extractor_specs()
    for spec in specs:
        assert f"{spec.target}.gate" in dag.task_ids
        assert f"{spec.target}.fetch" in dag.task_ids
        assert f"{spec.target}.write_raw" in dag.task_ids
        assert f"{spec.target}.done" in dag.task_ids

