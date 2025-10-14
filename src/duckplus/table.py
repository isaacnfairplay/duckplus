"""Mutable table wrapper for Duck+."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Optional, Tuple, cast

from . import util
from .connect import DuckConnection
from .core import DuckRel
from .relation.core import Relation
from .schema import AnyRow


def _quote_identifier(identifier: str) -> str:
    """Return *identifier* quoted for SQL usage."""

    return util.quote_identifier(identifier)


def _normalize_table_reference(name: str) -> str:
    """Return a sanitized table reference supporting dotted paths."""

    if not isinstance(name, str):
        raise TypeError(
            "Table name must be provided as a string; "
            f"received {type(name).__name__}."
        )

    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    index = 0
    length = len(name)
    while index < length:
        char = name[index]
        if char == '"':
            current.append(char)
            if in_quotes and index + 1 < length and name[index + 1] == '"':
                current.append('"')
                index += 2
                continue
            in_quotes = not in_quotes
            index += 1
            continue
        if char == "." and not in_quotes:
            part = "".join(current).strip()
            if not part:
                raise ValueError(
                    "Table name contains an empty identifier segment around '.' separators; "
                    f"original value {name!r}."
                )
            util.ensure_identifier(part, allow_quoted=True)
            parts.append(part)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1

    if in_quotes:
        raise ValueError(
            f"Table name {name!r} contains an unterminated quoted identifier."
        )

    part = "".join(current).strip()
    if not part:
        raise ValueError(
            "Table name contains an empty identifier segment around '.' separators; "
            f"original value {name!r}."
        )
    util.ensure_identifier(part, allow_quoted=True)
    parts.append(part)

    return ".".join(parts)


class DuckTable:
    """Mutable wrapper around a DuckDB table."""

    def __init__(self, connection: DuckConnection, name: str) -> None:
        self._connection = connection
        self._name = _normalize_table_reference(name)

    @property
    def name(self) -> str:
        """Return the normalized table name."""

        return self._name

    def append(self, rel: Relation[AnyRow], *, by_name: bool = True) -> None:
        """Append rows from *rel* into the table."""

        table_columns = self._table_columns()
        relation = rel

        if by_name:
            ordered = util.resolve_columns(table_columns, rel.columns)
            relation = cast(Relation[AnyRow], rel.project_columns(*ordered))
        else:
            if len(rel.columns) != len(table_columns):
                raise ValueError(
                    "Relation column count must match table when by_name is False; "
                    f"relation has {len(rel.columns)} column(s) but table {self._name} "
                    f"has {len(table_columns)} column(s)."
                )

        relation.relation.insert_into(self._name)

    def insert_antijoin(self, rel: Relation[AnyRow], *, keys: Sequence[str]) -> int:
        """Insert rows from *rel* missing in the table based on *keys*."""

        if not keys:
            raise ValueError(
                "insert_antijoin() requires at least one key column name."
            )

        table_columns = self._table_columns()
        resolved_keys = util.resolve_columns(keys, table_columns)

        table_rel: Relation[AnyRow] = Relation(self._connection.raw.table(self._name))
        existing = cast(Relation[AnyRow], table_rel.project_columns(*resolved_keys))
        filtered = cast(Relation[AnyRow], rel.anti_join(existing))
        count = filtered.row_count()

        if count > 0:
            self.append(filtered)

        return count

    def insert_by_continuous_id(
        self,
        rel: Relation[AnyRow],
        *,
        id_column: str,
        inclusive: bool = False,
    ) -> int:
        """Insert rows from *rel* with IDs beyond the table's current maximum."""

        table_columns = self._table_columns()
        resolved_id = util.resolve_columns([id_column], table_columns)[0]

        raw = self._connection.raw
        query = f"SELECT max({_quote_identifier(resolved_id)}) FROM {self._name}"
        max_row: Optional[Tuple[Any, ...]] = raw.execute(query).fetchone()
        current_max = None if max_row is None else max_row[0]

        candidate: Relation[AnyRow]
        if current_max is None:
            candidate = rel
        else:
            rel_id = util.resolve_columns([resolved_id], rel.columns)[0]
            op = ">=" if inclusive else ">"
            candidate = cast(
                Relation[AnyRow],
                rel.filter(f"{_quote_identifier(rel_id)} {op} ?", current_max),
            )

        return self.insert_antijoin(candidate, keys=[resolved_id])

    # Internal helpers -------------------------------------------------

    def _table_columns(self) -> list[str]:
        relation = self._connection.raw.table(self._name)
        return list(relation.columns)

