"""DuckPlus connection facade."""

from __future__ import annotations

import csv
import importlib.util
import json
import logging
from importlib import import_module
from pathlib import Path
from typing import Any, Iterable, cast

from duckplus.core.exceptions import CatalogException
from duckplus.core.relation import Relation

LOGGER = logging.getLogger(__name__)


class Connection:
    """High level connection that produces relations from external sources."""

    __slots__ = ("_extensions",)

    def __init__(self) -> None:
        self._extensions: set[str] = set()

    # ------------------------------------------------------------------ CSV
    def read_csv(self, path: str | Path, *, delimiter: str = ",", quoting: str = '"') -> Relation:
        target = Path(path)
        with target.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter, quotechar=quoting)
            rows = list(reader)
        return Relation(reader.fieldnames or [], rows, materialized=True)

    # ------------------------------------------------------------------ JSON
    def read_json(self, path: str | Path) -> Relation:
        target = Path(path)
        with target.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, list):
            raise TypeError("JSON relation payload must be a list of records")
        columns = list(payload[0].keys()) if payload else []
        return Relation(columns, payload, materialized=True)

    # ---------------------------------------------------------------- Parquet
    def read_parquet(self, path: str | Path) -> Relation:
        pq = _require_pyarrow_parquet()
        table = pq.read_table(Path(path))
        rows = table.to_pylist()
        columns = table.column_names
        return Relation(columns, rows, materialized=True)

    # ------------------------------------------------------------------ Excel
    def read_excel(self, path: str | Path, *, sheet_name: str | int | None = None) -> Relation:
        try:
            return self._read_excel(Path(path), sheet_name=sheet_name)
        except CatalogException:
            LOGGER.info("LOAD excel extension for read_excel auto-retry")
            self._load_extension("excel")
            return self._read_excel(Path(path), sheet_name=sheet_name)

    def _read_excel(self, path: Path, *, sheet_name: str | int | None) -> Relation:
        if "excel" not in self._extensions:
            raise CatalogException("Excel extension not loaded")
        if importlib.util.find_spec("pandas") is None:
            raise CatalogException("pandas is required for excel support")
        pd = cast(Any, import_module("pandas"))
        frame = pd.read_excel(path, sheet_name=sheet_name)
        rows = frame.to_dict(orient="records")
        columns = list(frame.columns)
        return Relation(columns, rows, materialized=True)

    def _load_extension(self, name: str) -> None:
        LOGGER.debug("Loading extension %s", name)
        self._extensions.add(name)

    # ----------------------------------------------------------------- Helpers
    def from_rows(self, columns: Iterable[str], rows: Iterable[dict[str, object]]) -> Relation:
        return Relation(list(columns), list(rows), materialized=True)


def _require_pyarrow_parquet() -> Any:
    if importlib.util.find_spec("pyarrow") is None:
        raise CatalogException("pyarrow is required for parquet support")
    if importlib.util.find_spec("pyarrow.parquet") is None:
        raise CatalogException("pyarrow is required for parquet support")
    return import_module("pyarrow.parquet")
