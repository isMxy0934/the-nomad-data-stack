"""Global pytest configuration.

Some DAG modules import helpers as `utils.*` (Airflow typically puts the `dags/`
folder on `sys.path`, so `dags/utils` becomes importable as top-level `utils`).

In this repo, unit tests run from the project root, and we provide a lightweight
shim package at `utils/` that re-exports `dags.utils.*` for test/runtime parity.
"""

from __future__ import annotations

import sys
from pathlib import Path


def pytest_configure() -> None:
    root_dir = Path(__file__).resolve().parents[1]

    raw = str(root_dir)
    if raw not in sys.path:
        sys.path.insert(0, raw)
