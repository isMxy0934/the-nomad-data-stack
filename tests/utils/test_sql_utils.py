import sys
from pathlib import Path

import pytest

from dags.utils.sql_utils import (  # pylint: disable=wrong-import-position
    MissingTemplateVariableError,
    load_and_render_sql,
    load_sql,
    render_sql,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))


def test_load_sql_raises_for_missing_file(tmp_path):
    missing_path = tmp_path / "missing.sql"
    with pytest.raises(FileNotFoundError):
        load_sql(missing_path)


def test_load_sql_raises_for_empty_file(tmp_path):
    empty_file = tmp_path / "empty.sql"
    empty_file.write_text("   \n\t", encoding="utf-8")

    with pytest.raises(ValueError):
        load_sql(empty_file)


def test_render_sql_successfully_substitutes_variables():
    template = "SELECT * FROM table WHERE dt = '${PARTITION_DATE}'"
    variables = {"PARTITION_DATE": "2024-01-01"}

    rendered = render_sql(template, variables)

    assert rendered == "SELECT * FROM table WHERE dt = '2024-01-01'"


def test_render_sql_raises_for_missing_variable():
    template = "SELECT * FROM table WHERE dt = '${PARTITION_DATE}'"

    with pytest.raises(MissingTemplateVariableError):
        render_sql(template, {})


def test_load_and_render_sql_end_to_end(tmp_path):
    sql_file = tmp_path / "query.sql"
    sql_file.write_text(
        "SELECT 1 AS col, '${PARTITION_DATE}' AS dt",
        encoding="utf-8",
    )

    rendered = load_and_render_sql(sql_file, {"PARTITION_DATE": "2024-02-02"})

    assert rendered == "SELECT 1 AS col, '2024-02-02' AS dt"


def test_load_and_render_sql_strips_trailing_semicolons(tmp_path: Path):
    sql_file = tmp_path / "query.sql"
    sql_file.write_text(
        "SELECT 1 AS col, '${PARTITION_DATE}' AS dt;\n",
        encoding="utf-8",
    )

    rendered = load_and_render_sql(sql_file, {"PARTITION_DATE": "2024-02-02"})

    assert rendered == "SELECT 1 AS col, '2024-02-02' AS dt"
