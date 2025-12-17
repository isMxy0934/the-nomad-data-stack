from __future__ import annotations

from airflow.models import DAG

from dags.dw_catalog_dag import create_catalog_dag


def test_create_catalog_dag_builds_tasks():
    dag: DAG = create_catalog_dag()
    assert dag.dag_id == "duckdb_catalog_dag"
    assert "migrate_catalog" in dag.task_ids
    assert "refresh_ods_catalog" in dag.task_ids

    migrate = dag.get_task("migrate_catalog")
    refresh = dag.get_task("refresh_ods_catalog")
    assert migrate.downstream_task_ids == {refresh.task_id}

