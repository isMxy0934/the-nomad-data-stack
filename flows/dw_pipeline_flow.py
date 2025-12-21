"""统一 DW Pipeline 入口 - 使用 Subflow 模式，UI 可清晰看到依赖关系."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger

from flows.dw_catalog_flow import dw_catalog_flow
from flows.dw_extractor_flow import dw_extractor_flow
from flows.dw_finish_flow import dw_finish_flow
from flows.dw_layer_flow import dw_layer_flow
from lakehouse_core.io.time import get_partition_date_str
from lakehouse_core.planning import load_dw_config, order_layers

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "dags" / "dw_config.yaml"


def _flow_run_name() -> str:
    from prefect.runtime import flow_run

    params = flow_run.parameters
    mode = params.get("mode") or "daily"
    conf = params.get("run_conf") or {}
    partition_date = conf.get("partition_date") or ""
    if partition_date:
        return f"dw-pipeline [{mode}] dt={partition_date}"
    return f"dw-pipeline [{mode}]"


@flow(name="dw_pipeline", flow_run_name=_flow_run_name)
def dw_pipeline_flow(
    mode: str = "daily",
    run_conf: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """统一 DW Pipeline 入口.

    Args:
        mode: 运行模式 - "daily"(每日) / "init"(初始化)
        run_conf: 运行配置
            - partition_date: 目标日期 (YYYY-MM-DD)
            - start_date: 回填开始日期
            - end_date: 回填结束日期
            - targets: 指定表列表 (e.g. ["ods.table1"])
            - init: 是否初始化模式
    """
    run_conf = run_conf or {}
    logger = get_run_logger()

    # 设置默认日期
    if not run_conf.get("partition_date"):
        run_conf["partition_date"] = get_partition_date_str()

    if mode == "init":
        run_conf["init"] = True

    logger.info("Pipeline 开始: mode=%s, date=%s", mode, run_conf.get("partition_date"))

    # Step 1: Extractor (init 模式跳过)
    if not run_conf.get("init"):
        logger.info("Step 1/4: 运行 Extractor")
        dw_extractor_flow(run_conf=run_conf)
    else:
        logger.info("Step 1/4: 跳过 Extractor (init 模式)")

    # Step 2: Catalog Migration
    logger.info("Step 2/4: 运行 Catalog Migration")
    dw_catalog_flow(run_conf=run_conf, skip_downstream=True)

    # Step 3: 按顺序执行各层
    config = load_dw_config(CONFIG_PATH)
    layers = order_layers(config)
    logger.info("Step 3/4: 执行数据层 %s", " -> ".join(layers))

    for layer in layers:
        logger.info("  -> 执行层: %s", layer)
        dw_layer_flow(layer=layer, run_conf=run_conf, skip_downstream=True)

    # Step 4: Finish
    logger.info("Step 4/4: 完成")
    dw_finish_flow(run_conf=run_conf)

    logger.info("Pipeline 完成!")
    return {"status": "success", "layers": layers}


if __name__ == "__main__":
    dw_pipeline_flow()

