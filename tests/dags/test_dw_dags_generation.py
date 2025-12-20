import sys
from pathlib import Path

import pytest
from airflow.models import DAG

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from dags.dw_dags import build_dw_dags
except ImportError as exc:
    pytest.skip(f"dw_dags imports unavailable in this environment: {exc}", allow_module_level=True)


def test_build_dw_dags():
    dag_map = build_dw_dags()
    assert isinstance(dag_map, dict)
    assert len(dag_map) > 0

    # We expect at least dw_ods and dw_dwd based on the project structure
    assert "dw_ods" in dag_map
    assert "dw_dwd" in dag_map

    for dag_id, dag in dag_map.items():
        assert isinstance(dag, DAG)
        assert dag.dag_id == dag_id


def test_dw_ods_structure():
    dag_map = build_dw_dags()
    ods_dag = dag_map["dw_ods"]

    # Check if some expected tables are in the DAG
    # Based on dags/ods/ods_daily_fund_price_akshare.sql
    assert "ods_daily_fund_price_akshare" in ods_dag.task_group_dict

    assert "ods_daily_fund_price_akshare.prepare" in ods_dag.task_dict
    assert "ods_daily_fund_price_akshare.load" in ods_dag.task_dict
    assert "ods_daily_fund_price_akshare.validate" in ods_dag.task_dict
    assert "ods_daily_fund_price_akshare.commit" in ods_dag.task_dict
    assert "ods_daily_fund_price_akshare.cleanup" in ods_dag.task_dict
