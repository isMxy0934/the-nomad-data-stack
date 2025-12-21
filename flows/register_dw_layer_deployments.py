"""Register per-layer deployments for dw_layer_flow (Airflow-aligned)."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from flows.dw_layer_flow import dw_layer_flow
from lakehouse_core.domain.models import RunContext
from lakehouse_core.planning import (
    DirectoryDWPlanner,
    DWConfigError,
    load_dw_config,
    order_layers,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "dags" / "dw_config.yaml"
SQL_BASE_DIR = REPO_ROOT / "dags"


def _layers_with_tables() -> set[str]:
    planner = DirectoryDWPlanner(dw_config_path=CONFIG_PATH, sql_base_dir=SQL_BASE_DIR)
    specs = planner.build(RunContext(run_id="plan", extra={"allow_unrendered_sql": True}))
    return {spec.layer for spec in specs}


def register_layer_deployments(*, work_pool: str) -> None:
    config = load_dw_config(CONFIG_PATH)
    layers_with_tables = _layers_with_tables()

    for layer in order_layers(config):
        if layer not in layers_with_tables:
            continue
        try:
            deployment_name = f"dw-layer-{layer}"
            logger.info("Registering deployment %s", deployment_name)
            dw_layer_flow.deploy(
                name=deployment_name,
                work_pool_name=work_pool,
                parameters={"layer": layer, "run_conf": {}},
                tags=["dw", f"layer:{layer}"],
            )
        except DWConfigError:
            continue


def main() -> None:
    work_pool = os.getenv("PREFECT_WORK_POOL", "default")
    register_layer_deployments(work_pool=work_pool)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
