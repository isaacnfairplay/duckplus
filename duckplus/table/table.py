"""Table abstraction built on top of relations."""

from __future__ import annotations

from typing import Callable, Iterable

from duckplus.core.relation import Relation


class Table:
    """In-memory table representation."""

    __slots__ = ("_name", "_columns", "_rows")

    def __init__(self, name: str) -> None:
        self._name = name
        self._columns: tuple[str, ...] | None = None
        self._rows: list[dict[str, object]] = []

    @property
    def name(self) -> str:
        return self._name

    def insert(
        self,
        relation: Relation | Callable[[], Relation],
        *,
        create: bool = False,
        overwrite: bool = False,
    ) -> None:
        if isinstance(relation, Relation):
            materialized = relation.materialize()
        else:
            candidate = relation()
            if not isinstance(candidate, Relation):
                raise TypeError("Table.insert callable must return a Relation instance")
            materialized = candidate.materialize()
        if self._columns is None:
            if not create:
                raise ValueError("table does not exist; pass create=True to initialize it")
            self._columns = materialized.columns
        if materialized.columns != self._columns:
            raise ValueError("column mismatch during insert")
        if overwrite:
            self._rows.clear()
        self._rows.extend(list(materialized))

    def to_relation(self) -> Relation:
        if self._columns is None:
            raise ValueError("table is empty")
        return Relation(self._columns, list(self._rows), materialized=True)

    def __iter__(self) -> Iterable[dict[str, object]]:
        return iter(self._rows)
