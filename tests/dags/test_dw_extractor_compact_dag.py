from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DAG  # noqa: E402

from dags.extractor.dw_extractor_compact_dag import create_dw_extractor_compact_dag  # noqa: E402


def test_dw_extractor_compact_dag_builds() -> None:
    dag: DAG = create_dw_extractor_compact_dag()
    assert dag.dag_id == "dw_extractor_compact_dag"
    assert "all_done" in dag.task_ids
