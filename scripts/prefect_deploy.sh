#!/usr/bin/env bash
set -euo pipefail

prefect deploy flows/dw_start_flow.py:dw_start_flow -n dw-start -p default
prefect deploy flows/dw_init_flow.py:dw_init_flow -n dw-init -p default
prefect deploy flows/dw_extractor_flow.py:dw_extractor_flow -n dw-extractor -p default
prefect deploy flows/dw_catalog_flow.py:dw_catalog_flow -n dw-catalog -p default
prefect deploy flows/dw_layer_flow.py:dw_layer_flow -n dw-layer -p default
prefect deploy flows/dw_finish_flow.py:dw_finish_flow -n dw-finish -p default
prefect deploy flows/dw_extractor_backfill_flow.py:dw_extractor_backfill_flow -n dw-extractor-backfill -p default
prefect deploy flows/dw_extractor_compact_flow.py:dw_extractor_compact_flow -n dw-extractor-compact -p default
