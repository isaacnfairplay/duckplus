from __future__ import annotations

import json
from pathlib import Path

import pytest

from duckplus.core.connection import Connection
from duckplus.core.exceptions import CatalogException


def test_read_csv_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "input.csv"
    path.write_text("id,name\n1,Ada\n2,Bob\n", encoding="utf-8")
    conn = Connection()
    relation = conn.read_csv(path)
    assert relation.columns == ("id", "name")
    assert list(relation) == [
        {"id": "1", "name": "Ada"},
        {"id": "2", "name": "Bob"},
    ]
    materialized = relation.materialize()
    assert materialized is relation


def test_read_json(tmp_path: Path) -> None:
    path = tmp_path / "input.json"
    payload = [{"id": 1, "name": "Ada"}]
    path.write_text(json.dumps(payload), encoding="utf-8")
    conn = Connection()
    relation = conn.read_json(path)
    assert relation.columns == ("id", "name")
    assert list(relation) == payload


@pytest.mark.parametrize("values", [[{"id": 1, "value": 3.14}]])
def test_read_parquet(tmp_path: Path, values: list[dict[str, object]]) -> None:
    pytest.importorskip("pyarrow")
    path = tmp_path / "input.parquet"
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    table = pa.Table.from_pylist(values)
    pq.write_table(table, path)
    conn = Connection()
    relation = conn.read_parquet(path)
    assert relation.columns == tuple(table.column_names)
    assert list(relation) == values


@pytest.mark.parametrize("sheet_name", [None, 0])
def test_read_excel_autoload(tmp_path: Path, sheet_name: str | int | None) -> None:
    pd = pytest.importorskip("pandas")
    path = tmp_path / "input.xlsx"
    frame = pd.DataFrame({"id": [1, 2], "name": ["Ada", "Bob"]})
    frame.to_excel(path, index=False)
    conn = Connection()
    # Ensure the first attempt raises and triggers LOAD excel.
    with pytest.raises(CatalogException):
        conn._read_excel(path, sheet_name=sheet_name)
    relation = conn.read_excel(path, sheet_name=sheet_name)
    assert list(relation) == frame.to_dict(orient="records")
    assert "excel" in conn._extensions  # type: ignore[attr-defined]

