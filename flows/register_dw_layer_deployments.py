"""
Script to dynamically register Prefect deployments for each DW layer found in dw_config.yaml.
This mimics the dynamic DAG generation in Airflow's dw_dags.py.

Usage:
    python -m flows.register_dw_layer_deployments
"""
import logging
import os
from pathlib import Path

from flows.dw_layer_flow import dw_layer_flow, _load_layer_specs
from lakehouse_core.planning import load_dw_config, order_layers, DWConfigError

# Setup paths
REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "dags" / "dw_config.yaml"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("register_deployments")

def register_layer_deployments():
    if not CONFIG_PATH.exists():
        logger.error(f"Config file not found at {CONFIG_PATH}")
        return

    logger.info(f"Loading config from {CONFIG_PATH}")
    config = load_dw_config(CONFIG_PATH)
    
    # Iterate over layers in order (ods -> dwd -> ...)
    for layer in order_layers(config):
        try:
            # Check if this layer has any tables defined
            ordered_specs, _ = _load_layer_specs(layer, config)
            if not ordered_specs:
                logger.info(f"Skipping layer '{layer}' (no tables found)")
                continue

            deployment_name = f"dw-layer-{layer}"
            logger.info(f"Registering deployment: {deployment_name}")

            # Use .from_source().deploy() pattern for modern Prefect
            # This ensures the entrypoint is correctly set relative to the repo root
            dw_layer_flow.from_source(
                source=str(REPO_ROOT),
                entrypoint="flows/dw_layer_flow.py:dw_layer_flow"
            ).deploy(
                name=deployment_name,
                work_pool_name="default", 
                parameters={"layer": layer},
                version=os.getenv("GIT_SHA", "dev"),
                tags=["dw", layer],
                build=False, 
                push=False,
            )
            
            logger.info(f"Successfully registered {deployment_name}")

        except DWConfigError as e:
            logger.warning(f"Skipping layer {layer} due to config error: {e}")
        except Exception as e:
            logger.error(f"Failed to register layer {layer}: {e}")

def ensure_default_work_pool():
    """Ensure the 'default' work pool exists to avoid deployment errors."""
    try:
        from prefect.client.orchestration import get_client
        from prefect.client.schemas.actions import WorkPoolCreate
        import asyncio
    except ImportError:
        logger.warning("Could not import prefect client libraries. Skipping work pool creation.")
        return

    async def _create_pool():
        async with get_client() as client:
            try:
                await client.read_work_pool("default")
            except Exception:
                logger.info("Work pool 'default' not found. Creating it now...")
                try:
                    await client.create_work_pool(WorkPoolCreate(name="default", type="process"))
                    logger.info("Successfully created 'default' work pool.")
                except Exception as e:
                    logger.error(f"Failed to create work pool: {e}")
    
    try:
        asyncio.run(_create_pool())
    except Exception as e:
        logger.warning(f"Could not ensure work pool existence: {e}")

if __name__ == "__main__":
    print(f"Project Root: {REPO_ROOT}")
    ensure_default_work_pool()
    register_layer_deployments()
