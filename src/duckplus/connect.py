"""Connection helpers for Duck+."""

from __future__ import annotations

from contextlib import AbstractContextManager
from os import PathLike, fspath
from types import TracebackType
from typing import Literal, Optional, Self

import duckdb

Pathish = str | PathLike[str]


class DuckConnection(AbstractContextManager["DuckConnection"]):
    """Lightweight wrapper around :mod:`duckdb` connections."""

    def __init__(self, database: Optional[Pathish] = None, *, read_only: bool = False) -> None:
        db_name = ":memory:" if database is None else fspath(database)
        self._raw: duckdb.DuckDBPyConnection = duckdb.connect(database=db_name, read_only=read_only)
        self._closed: bool = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> Literal[False]:
        self.close()
        return False

    def close(self) -> None:
        """Close the underlying DuckDB connection."""

        if not self._closed:
            self._raw.close()
            self._closed = True

    @property
    def raw(self) -> duckdb.DuckDBPyConnection:
        """Return the underlying :class:`duckdb.DuckDBPyConnection`."""

        return self._raw


def connect(database: Optional[Pathish] = None, *, read_only: bool = False) -> DuckConnection:
    """Create a :class:`DuckConnection`.

    Parameters
    ----------
    database:
        Optional database path. Defaults to in-memory storage when ``None``.
    read_only:
        Whether the connection should be opened in read-only mode.
    """

    return DuckConnection(database=database, read_only=read_only)
