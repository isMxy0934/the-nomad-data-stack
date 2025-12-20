from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def attach_catalog_if_available(connection, *, catalog_path: str | Path) -> bool:  # noqa: ANN001
    """Attach a DuckDB catalog database in READ_ONLY mode when it exists.

    Returns:
        True when attached, False when missing.
    """

    path = Path(catalog_path)
    if not path.exists():
        logger.info("Catalog file %s not found; skipping attach.", path)
        return False

    connection.execute(f"ATTACH '{path}' AS catalog (READ_ONLY);")
    # Prefer search_path to keep temp views resolvable while making catalog schemas visible.
    try:
        connection.execute("SET search_path='temp, catalog, main';")
    except Exception:  # noqa: BLE001
        connection.execute("USE catalog;")
    return True

