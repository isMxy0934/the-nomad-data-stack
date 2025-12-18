from __future__ import annotations

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DAG  # noqa: E402

from dags.dw_catalog_dag import create_catalog_dag


def test_create_catalog_dag_builds_tasks():
    dag: DAG = create_catalog_dag()
    assert dag.dag_id == "dw_catalog_dag"
    assert "migrate_catalog" in dag.task_ids
    assert "trigger_dw_ods" in dag.task_ids

    migrate = dag.get_task("migrate_catalog")
    trigger = dag.get_task("trigger_dw_ods")
    assert migrate.downstream_task_ids == {trigger.task_id}
    assert trigger.trigger_dag_id == "dw_ods"
