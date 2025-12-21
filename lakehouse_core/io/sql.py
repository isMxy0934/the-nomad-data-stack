"""SQL template utilities (orchestrator-agnostic)."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from string import Template


class MissingTemplateVariableError(ValueError):
    """Error raised when a required template variable is not provided."""


def load_sql(path: str | Path) -> str:
    """Load SQL text from a file and validate that it is not empty."""

    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    content = file_path.read_text(encoding="utf-8")
    if not content.strip():
        raise ValueError(f"SQL file is empty: {file_path}")

    return content


def render_sql(sql: str, variables: Mapping[str, str]) -> str:
    """Render a SQL template using `${VAR}` placeholders."""

    if not sql.strip():
        raise ValueError("SQL template is empty")

    try:
        rendered = Template(sql).substitute(**variables)
    except KeyError as exc:
        missing = exc.args[0]
        raise MissingTemplateVariableError(f"Missing template variable: {missing}") from exc

    normalized = rendered.strip()
    while normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()

    if not normalized:
        raise ValueError("Rendered SQL is empty")

    return normalized


def load_and_render_sql(path: str | Path, variables: Mapping[str, str]) -> str:
    """Convenience helper to load a SQL file and apply template rendering."""

    raw_sql = load_sql(path)
    return render_sql(raw_sql, variables)
