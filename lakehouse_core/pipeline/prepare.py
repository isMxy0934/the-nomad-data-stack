from __future__ import annotations

from typing import Any

from lakehouse_core.api import prepare_paths
from lakehouse_core.domain.models import RunSpec
from lakehouse_core.io.paths import paths_to_dict


def prepare(
    *,
    spec: RunSpec,
    run_id: str,
    store_namespace: str,
    partition_date: str | None,
) -> dict[str, Any]:
    """Prepare canonical/tmp paths payload for orchestrators.

    Returns a JSON-serializable mapping suitable for Airflow XCom.
    Uses paths_to_dict for consistent serialization.
    """
    effective_dt = partition_date if spec.is_partitioned else None
    paths = prepare_paths(
        base_prefix=spec.base_prefix,
        run_id=run_id,
        partition_date=effective_dt,
        is_partitioned=spec.is_partitioned,
        store_namespace=store_namespace,
    )
    return dict(paths_to_dict(paths, partition_date=partition_date))
