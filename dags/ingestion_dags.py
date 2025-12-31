"""
Ingestion DAG Factory.
Scans dags/ingestion/configs/*.yaml and generates Airflow DAGs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from lakehouse_core.ingestion.runner import run_ingestion_config
from lakehouse_core.io.time import get_partition_date_str

logger = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).parent / "ingestion" / "configs"
DEFAULT_ARGS = {
    "owner": "ingestion",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _load_yaml_configs() -> list[Path]:
    if not CONFIG_DIR.exists():
        logger.warning("Config directory not found: %s", CONFIG_DIR)
        return []
    return list(CONFIG_DIR.glob("*.yaml"))


def create_dag(config_path: Path):
    try:
        with open(config_path, encoding="utf-8") as handle:
            conf = yaml.safe_load(handle)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load config %s: %s", config_path, exc)
        return None

    target = conf["target"]
    dag_id = f"ingestion_{target}"

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        schedule=conf.get("schedule_interval", "@daily"),
        start_date=datetime.fromisoformat(conf.get("start_date", "2024-01-01")),
        catchup=conf.get("catchup", False),
        tags=["ingestion", target],
        max_active_runs=1,
        params={
            "start_date": Param(
                default=get_partition_date_str(),
                type="string",
                format="date",
                description="Start date (YYYY-MM-DD).",
            ),
            "end_date": Param(
                default=get_partition_date_str(),
                type="string",
                format="date",
                description="End date (YYYY-MM-DD). Defaults to start_date if empty.",
            ),
            "write_mode": Param(
                default="overwrite",
                type="string",
                description="Partition write mode: overwrite | skip_existing | fail_if_exists.",
            ),
            "max_workers": Param(
                default=1,
                type="integer",
                description="Parallel extractor workers inside the ingestion runner.",
            ),
        },
    )

    with dag:

        @task
        def run_ingestion(**context):
            dag_params = context.get("params") or {}
            dag_run_conf = context.get("dag_run").conf or {}

            start_date = dag_run_conf.get("start_date") or dag_params.get("start_date")
            end_date = dag_run_conf.get("end_date") or dag_params.get("end_date") or start_date
            partition_date = dag_run_conf.get("partition_date")
            write_mode = (
                dag_run_conf.get("write_mode") or dag_params.get("write_mode") or "overwrite"
            )
            max_workers = int(dag_run_conf.get("max_workers") or dag_params.get("max_workers") or 1)

            return run_ingestion_config(
                config_path=config_path,
                start_date=start_date,
                end_date=end_date,
                partition_date=partition_date,
                run_id=context.get("run_id"),
                write_mode=write_mode,
                max_workers=max_workers,
            )

        run_ingestion()

    return dag


for config_file in _load_yaml_configs():
    dag_object = create_dag(config_file)
    if dag_object:
        globals()[dag_object.dag_id] = dag_object
