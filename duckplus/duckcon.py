"""Context manager utilities for DuckDB connections."""

# pylint: disable=import-error

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional

import duckdb  # type: ignore[import-not-found]


class DuckCon:
    """Context manager for managing a DuckDB connection.

    Parameters
    ----------
    database:
        The database path to connect to. Defaults to an in-memory database.
    **connect_kwargs:
        Additional keyword arguments forwarded to :func:`duckdb.connect`.
    """

    def __init__(self, database: str = ":memory:", **connect_kwargs: Any) -> None:
        self.database = database
        self.connect_kwargs = connect_kwargs
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self._helpers: dict[str, Callable[[duckdb.DuckDBPyConnection, Any], Any]] = {}

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        if self._connection is not None:
            raise RuntimeError("DuckDB connection is already open.")
        self._connection = duckdb.connect(database=self.database, **self.connect_kwargs)
        return self._connection

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    def is_open(self) -> bool:
        """Return ``True`` when the managed connection is open."""

        return self._connection is not None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Access the active DuckDB connection.

        Raises
        ------
        RuntimeError
            If the context manager is not currently managing an open connection.
        """

        if self._connection is None:
            raise RuntimeError("No active DuckDB connection. Use DuckCon as a context manager.")
        return self._connection

    def register_helper(
        self,
        name: str,
        helper: Callable[[duckdb.DuckDBPyConnection, Any], Any],
        *,
        overwrite: bool = False,
    ) -> None:
        """Register an I/O helper callable.

        Parameters
        ----------
        name:
            Name used to reference the helper.
        helper:
            Callable that receives the active connection as its first argument.
        overwrite:
            Whether to overwrite an existing helper with the same name.
        """

        if not overwrite and name in self._helpers:
            raise ValueError(f"Helper '{name}' is already registered.")
        self._helpers[name] = helper

    def apply_helper(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a registered helper with the active connection."""

        if name not in self._helpers:
            raise KeyError(f"Helper '{name}' is not registered.")
        helper = self._helpers[name]
        return helper(self.connection, *args, **kwargs)

    def table(self, name: str) -> "Table":
        """Return a managed table wrapper bound to this connection."""

        from .table import Table  # pylint: disable=import-outside-toplevel

        return Table(self, name)


if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from .table import Table
