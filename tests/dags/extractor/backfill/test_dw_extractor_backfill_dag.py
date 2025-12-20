from __future__ import annotations

import pytest
from airflow.models import DAG  # noqa: E402

from dags.extractor.backfill.dw_extractor_backfill_dag import (
    create_dw_extractor_backfill_dag,  # noqa: E402
)

airflow = pytest.importorskip("airflow")


def test_dw_extractor_backfill_dag_builds() -> None:
    dag: DAG = create_dw_extractor_backfill_dag()
    assert dag.dag_id == "dw_extractor_backfill_dag"
    assert "all_done" in dag.task_ids
