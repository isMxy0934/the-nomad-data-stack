from __future__ import annotations

import pytest
from airflow.models import DAG  # noqa: E402

from dags.extractor.expect.dw_extractor_compact_dag import (
    create_dw_extractor_compact_dag,
)

airflow = pytest.importorskip("airflow")


def test_dw_extractor_compact_dag_builds() -> None:
    dag: DAG = create_dw_extractor_compact_dag()
    assert dag.dag_id == "dw_extractor_compact_dag"
    assert "all_done" in dag.task_ids
