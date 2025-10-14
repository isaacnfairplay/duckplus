"""Materialization strategies and result helpers for DuckRel."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence, TYPE_CHECKING

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from .relation.core import Relation
    from .schema import AnyRow


@dataclass(slots=True)
class MaterializeArtifacts:
    """Internal container produced by :class:`MaterializeStrategy` objects."""

    table: pa.Table | None = None
    relation: duckdb.DuckDBPyRelation | None = None
    columns: Sequence[str] | None = None
    path: Path | None = None


class MaterializeStrategy(Protocol):
    """Strategy used by :meth:`duckplus.core.DuckRel.materialize`."""

    def materialize(
        self,
        relation: duckdb.DuckDBPyRelation,
        columns: Sequence[str],
        *,
        into: duckdb.DuckDBPyConnection | None,
    ) -> MaterializeArtifacts:
        """Return the materialization artefacts for *relation*."""


class ArrowMaterializeStrategy:
    """Materialize relations via Arrow tables."""

    __slots__ = ("_retain_table",)

    def __init__(self, *, retain_table: bool = True) -> None:
        self._retain_table = retain_table

    def materialize(
        self,
        relation: duckdb.DuckDBPyRelation,
        columns: Sequence[str],
        *,
        into: duckdb.DuckDBPyConnection | None,
    ) -> MaterializeArtifacts:
        table = relation.to_arrow_table()
        if into is None:
            return MaterializeArtifacts(table=table)
        target = into.from_arrow(table)
        stored = table if self._retain_table else None
        return MaterializeArtifacts(
            table=stored,
            relation=target,
            columns=columns,
        )


class ParquetMaterializeStrategy:
    """Materialize relations through Parquet files."""

    __slots__ = ("_path", "_cleanup", "_suffix")

    def __init__(
        self,
        path: Path | None = None,
        *,
        cleanup: bool = False,
        suffix: str = ".parquet",
    ) -> None:
        self._path = path
        self._cleanup = cleanup
        self._suffix = suffix

    def materialize(
        self,
        relation: duckdb.DuckDBPyRelation,
        columns: Sequence[str],
        *,
        into: duckdb.DuckDBPyConnection | None,
    ) -> MaterializeArtifacts:
        import os
        import tempfile

        created_path = False
        file_path = self._path
        if file_path is None:
            handle, name = tempfile.mkstemp(prefix="duckplus_materialized_", suffix=self._suffix)
            os.close(handle)
            file_path = Path(name)
            created_path = True

        relation.to_parquet(str(file_path))
        table = pq.read_table(file_path)
        target = into.read_parquet(str(file_path)) if into is not None else None

        final_path: Path | None = file_path
        if self._cleanup and created_path:
            file_path.unlink(missing_ok=True)
            final_path = None

        return MaterializeArtifacts(
            table=table,
            relation=target,
            columns=columns,
            path=final_path,
        )


@dataclass(frozen=True, slots=True)
class Materialized:
    """Result of :meth:`duckplus.core.DuckRel.materialize`."""

    table: pa.Table | None
    relation: Relation[AnyRow] | None
    path: Path | None

    def require_table(self) -> pa.Table:
        """Return the Arrow table or raise if unavailable."""

        if self.table is None:
            raise ValueError(
                "Materialization strategy did not retain an Arrow table; "
                "call require_table() only when the chosen strategy keeps a table in memory."
            )
        return self.table

    def require_relation(self) -> "Relation[AnyRow]":
        """Return the materialized :class:`duckplus.DuckRel` or raise if unavailable."""

        if self.relation is None:
            raise ValueError(
                "Materialization strategy did not materialize into a relation; "
                "ensure a target connection was provided when required."
            )
        return self.relation


__all__ = [
    "MaterializeStrategy",
    "ArrowMaterializeStrategy",
    "ParquetMaterializeStrategy",
    "Materialized",
]
