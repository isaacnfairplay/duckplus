"""Table helpers for managed DuckDB inserts."""

from __future__ import annotations

# pylint: disable=import-error

from dataclasses import dataclass
from typing import Sequence

import duckdb  # type: ignore[import-not-found]

from ._table_utils import append_relation_data, normalise_target_columns, require_connection
from .duckcon import DuckCon
from .relation import Relation


@dataclass(frozen=True)
class Table:
    """Lightweight wrapper around a DuckDB table name."""

    duckcon: DuckCon
    name: str

    def insert(
        self,
        relation: Relation,
        *,
        target_columns: Sequence[str] | None = None,
        create: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Insert rows from a relation into the table."""

        connection = require_connection(self.duckcon, "Table.insert")
        if relation.duckcon is not self.duckcon:
            msg = "Relation originates from a different DuckCon"
            raise ValueError(msg)
        target_column_list = normalise_target_columns(target_columns, "Table.insert")
        append_relation_data(
            connection,
            relation.relation,
            self.name,
            "Table.insert",
            target_columns=target_column_list,
            create=create,
            overwrite=overwrite,
        )

    def insert_relation(
        self,
        relation: duckdb.DuckDBPyRelation,
        *,
        target_columns: Sequence[str] | None = None,
        create: bool = False,
        overwrite: bool = False,
    ) -> None:
        """Insert rows from a raw DuckDB relation into the table."""

        connection = require_connection(self.duckcon, "Table.insert_relation")
        target_column_list = normalise_target_columns(
            target_columns, "Table.insert_relation"
        )
        append_relation_data(
            connection,
            relation,
            self.name,
            "Table.insert_relation",
            target_columns=target_column_list,
            create=create,
            overwrite=overwrite,
        )
