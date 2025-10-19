"""Context manager utilities for DuckDB connections."""

# pylint: disable=import-error

from __future__ import annotations

import importlib
from dataclasses import dataclass
from collections.abc import Callable, Sequence
from functools import wraps
from typing import TYPE_CHECKING, Any, Optional, TypeVar, cast
from typing import Literal
import warnings

import duckdb  # type: ignore[import-not-found]


ExtraExtensionName = Literal["nanodbc", "excel"]


@dataclass(frozen=True)
class ExtensionInfo:  # pylint: disable=too-many-instance-attributes
    """Metadata describing DuckDB extension installation state."""

    name: str
    loaded: bool
    installed: bool
    install_path: str | None
    description: str | None
    aliases: tuple[str, ...]
    version: str | None
    install_mode: str | None
    installed_from: str | None


class DuckCon:  # pylint: disable=too-many-instance-attributes
    """Context manager for managing a DuckDB connection.

    Parameters
    ----------
    database:
        The database path to connect to. Defaults to an in-memory database.
    extra_extensions:
        Optional iterable of community extensions to install and load when the
        connection opens. Supported values currently include ``"nanodbc"`` and
        ``"excel"``.
    **connect_kwargs:
        Additional keyword arguments forwarded to :func:`duckdb.connect`.
    """

    def __init__(
        self,
        database: str = ":memory:",
        *,
        extra_extensions: Sequence[ExtraExtensionName] | None = None,
        **connect_kwargs: Any,
    ) -> None:
        self.database = database
        if extra_extensions is None:
            extensions: tuple[ExtraExtensionName, ...] = ()
        else:
            # Preserve order but avoid duplicate installation attempts.
            seen: set[ExtraExtensionName] = set()
            ordered: list[ExtraExtensionName] = []
            for extension in extra_extensions:
                if extension in seen:
                    continue
                seen.add(extension)
                ordered.append(extension)
            extensions = tuple(ordered)
        self.extra_extensions = extensions
        self.connect_kwargs = connect_kwargs
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        if self._connection is not None:
            raise RuntimeError("DuckDB connection is already open.")
        connection = duckdb.connect(database=self.database, **self.connect_kwargs)
        self._connection = connection

        try:
            self._initialise_extra_extensions()
        except Exception:  # pragma: no cover - defensive clean-up
            connection.close()
            self._connection = None
            raise

        return connection

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
        """Register a helper as a bound method on :class:`DuckCon`."""

        if hasattr(DuckCon, name) and not overwrite:
            raise ValueError(f"Helper '{name}' is already registered.")

        @wraps(helper)
        def bound(self: "DuckCon", *args: Any, **kwargs: Any) -> Any:
            return helper(self.connection, *args, **kwargs)

        setattr(DuckCon, name, bound)

    def apply_helper(self, name: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a registered helper with the active connection."""

        try:
            helper = cast(Callable[..., Any], getattr(self, name))
        except AttributeError as exc:
            raise KeyError(f"Helper '{name}' is not registered.") from exc

        if not callable(helper):
            raise TypeError(f"Attribute '{name}' is not callable.")
        return helper(*args, **kwargs)

    def load_nano_odbc(self, *, install: bool = True) -> None:
        """Install and load the nano-ODBC community extension.

        .. deprecated:: 0.0
            Use :class:`DuckCon`'s ``extra_extensions`` parameter instead. The
            method remains for backwards compatibility and forwards to the
            internal loader.
        """

        warnings.warn(
            "DuckCon.load_nano_odbc() is deprecated. Pass "
            "extra_extensions=(\"nanodbc\",) when constructing DuckCon instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        self._load_nano_odbc(install=install)

    def extensions(self) -> tuple[ExtensionInfo, ...]:  # pylint: disable=too-many-locals
        """Return metadata about available DuckDB extensions."""

        if not self.is_open:
            msg = (
                "DuckCon connection must be open to list extensions. Use DuckCon "
                "as a context manager."
            )
            raise RuntimeError(msg)

        columns = (
            "extension_name",
            "loaded",
            "installed",
            "install_path",
            "description",
            "aliases",
            "extension_version",
            "install_mode",
            "installed_from",
        )
        selected_columns = ", ".join(columns)
        query = f"SELECT {selected_columns} FROM duckdb_extensions()"
        rows = self.connection.execute(query).fetchall()
        infos = []
        for row in rows:
            (  # pragma: no branch - row unpack for clarity
                name,
                loaded,
                installed,
                install_path,
                description,
                aliases,
                version,
                install_mode,
                installed_from,
            ) = row
            info = ExtensionInfo(
                name=name,
                loaded=bool(loaded),
                installed=bool(installed),
                install_path=install_path,
                description=description,
                aliases=tuple(aliases or ()),
                version=version,
                install_mode=install_mode,
                installed_from=installed_from,
            )
            infos.append(info)
        return tuple(infos)

    def _initialise_extra_extensions(self) -> None:
        for extension in self.extra_extensions:
            if extension == "nanodbc":
                self._load_nano_odbc()
            elif extension == "excel":
                self._load_excel()
            else:  # pragma: no cover - exhaustive guard for Literal handling
                raise ValueError(f"Unsupported extension '{extension}'.")

    def _load_nano_odbc(self, *, install: bool = True) -> None:
        if not self.is_open:
            msg = (
                "DuckCon connection must be open to load extensions. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        connection = self.connection

        if install:
            self._install_via_duckdb_extensions("nano_odbc")
            try:
                connection.install_extension("nano_odbc")
            except duckdb.Error:
                # Installation failures are tolerated because the extension
                # might already exist on the machine from a prior run. DuckDB
                # installs community extensions globally per user profile.
                pass

        try:
            connection.load_extension("nano_odbc")
        except duckdb.Error as exc:  # pragma: no cover - error class coverage
            msg = (
                "Failed to load the nano-ODBC extension. Because DuckDB installs "
                "extensions per user profile, install nano_odbc manually via the "
                "DuckDB CLI or the duckdb-extensions package before creating the "
                "connection with DuckCon(extra_extensions=(\"nanodbc\",))."
            )
            raise RuntimeError(msg) from exc

    def _load_excel(self, *, install: bool = True) -> None:
        if not self.is_open:
            msg = (
                "DuckCon connection must be open to load extensions. "
                "Use DuckCon as a context manager."
            )
            raise RuntimeError(msg)

        connection = self.connection

        if install:
            self._install_via_duckdb_extensions("excel")
            try:
                connection.install_extension("excel")
            except duckdb.Error:
                # Installation failures can occur when the extension is already
                # installed for the user profile. DuckDB keeps community
                # extensions in a shared location, so we silently ignore these
                # cases to keep the helper idempotent.
                pass

        try:
            connection.load_extension("excel")
        except duckdb.Error as exc:  # pragma: no cover - error class coverage
            msg = (
                "Failed to load the Excel extension. Install it manually via the DuckDB CLI "
                "or the duckdb-extensions package before creating the connection with "
                "DuckCon(extra_extensions=(\"excel\",))."
            )
            raise RuntimeError(msg) from exc

    def _install_via_duckdb_extensions(self, extension: str) -> bool:
        try:
            module = importlib.import_module("duckdb_extensions")
        except ModuleNotFoundError:  # pragma: no cover - optional dependency
            return False

        import_extension = getattr(module, "import_extension", None)
        if import_extension is None:
            return False

        try:
            import_extension(extension)
        except Exception:  # pragma: no cover - install helper failure  # pylint: disable=broad-exception-caught
            return False
        return True

    def table(self, name: str) -> "Table":
        """Return a managed table wrapper bound to this connection."""

        from .table import Table  # pylint: disable=import-outside-toplevel

        return Table(self, name)

if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from .table import Table


HelperFunction = TypeVar("HelperFunction", bound=Callable[..., Any])


if TYPE_CHECKING:  # pragma: no cover - re-export for type checking
    from .io import duckcon_helper as duckcon_helper
else:

    def duckcon_helper(helper: HelperFunction) -> HelperFunction:
        """Proxy to :mod:`duckplus.io`'s decorator at call time."""

        from .io import duckcon_helper as _duckcon_helper

        return _duckcon_helper(helper)


