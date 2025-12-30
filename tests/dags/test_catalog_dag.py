from __future__ import annotations

import pytest
from airflow.models import DAG  # noqa: E402

from dags.dw_catalog_dag import create_catalog_dag

airflow = pytest.importorskip("airflow")


def test_create_catalog_dag_builds_tasks():
    dag: DAG = create_catalog_dag()
    assert dag.dag_id == "dw_catalog_dag"
    assert "migrate_catalog" in dag.task_ids
    assert "decide_dag_type" in dag.task_ids
    assert "trigger_dw_ods" in dag.task_ids
    assert "trigger_dw_ods_backfill" in dag.task_ids

    migrate = dag.get_task("migrate_catalog")
    branch = dag.get_task("decide_dag_type")
    trigger_daily = dag.get_task("trigger_dw_ods")
    trigger_backfill = dag.get_task("trigger_dw_ods_backfill")

    # Verify task dependencies: migrate -> branch -> [daily, backfill]
    assert migrate.downstream_task_ids == {branch.task_id}
    assert branch.downstream_task_ids == {trigger_daily.task_id, trigger_backfill.task_id}
    assert trigger_daily.trigger_dag_id == "dw_ods"
    assert trigger_backfill.trigger_dag_id == "dw_ods_backfill"
