#!/usr/bin/env bash
set -euo pipefail

cd /opt/prefect/flows
prefect deploy --all
