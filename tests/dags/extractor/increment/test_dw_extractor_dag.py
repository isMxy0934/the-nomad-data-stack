from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DAG  # noqa: E402

from dags.extractor.increment.dw_extractor_dag import (  # noqa: E402
    create_dw_extractor_dag,
    load_extractor_specs,
)


def test_dw_extractor_dag_builds_expected_groups():
    dag: DAG = create_dw_extractor_dag()
    assert dag.dag_id == "dw_extractor_dag"

    # Static tasks (mapped tasks appear by their base task_id).
    assert "resolve_jobs" in dag.task_ids
    assert "run_provider" in dag.task_ids
    assert "merge_jobs" in dag.task_ids
    assert "fetch_to_tmp" in dag.task_ids
    assert "write_raw" in dag.task_ids

    assert "all_done" in dag.task_ids
    assert "trigger_dw_catalog_dag" in dag.task_ids

    # Config should remain loadable.
    assert len(load_extractor_specs()) > 0
