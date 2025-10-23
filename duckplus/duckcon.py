"""Context manager utilities for DuckDB connections."""

# pylint: disable=import-error

from __future__ import annotations

import importlib
from dataclasses import dataclass
from collections.abc import Callable, Mapping, Sequence
from functools import wraps
from typing import TYPE_CHECKING, Any, Optional, SupportsInt, TypeVar, cast
from typing import Literal
import warnings

import duckdb  # type: ignore[import-not-found]
from duckdb import sqltypes

from duckplus.static_typed.types import DuckDBType


ExtraExtensionName = Literal["nanodbc", "excel"]


TypeSpecifier = str | sqltypes.DuckDBPyType | DuckDBType
FieldMapping = Mapping[str, TypeSpecifier] | Sequence[tuple[str, TypeSpecifier]]


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

    def begin(self) -> duckdb.DuckDBPyConnection:
        """Begin a transaction on the managed connection."""

        return self.connection.begin()

    def commit(self) -> duckdb.DuckDBPyConnection:
        """Commit the active transaction on the managed connection."""

        return self.connection.commit()

    def rollback(self) -> duckdb.DuckDBPyConnection:
        """Roll back the active transaction on the managed connection."""

        return self.connection.rollback()

    def execute(
        self, query: Any, parameters: Any = None
    ) -> duckdb.DuckDBPyConnection:
        """Execute ``query`` with optional ``parameters`` using DuckDB."""

        return self.connection.execute(query, parameters)

    def sql(
        self, query: Any, *, alias: str = "", params: Any = None
    ) -> "Relation":
        """Run ``query`` and wrap the resulting relation in :class:`Relation`."""

        from .relation import Relation  # pylint: disable=import-outside-toplevel

        relation = self.connection.sql(query, alias=alias, params=params)
        return Relation.from_relation(self, relation)

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next row from the most recent query."""

        return self.connection.fetchone()

    def fetchmany(self, size: SupportsInt | None = None) -> list[tuple[Any, ...]]:
        """Return up to ``size`` rows from the most recent query."""

        helper = "DuckCon.fetchmany"
        if size is None:
            rows = self.connection.fetchmany()
        else:
            normalised = self._normalise_fetch_size(
                size, helper=helper, parameter="size"
            )
            rows = self.connection.fetchmany(normalised)
        return list(rows)

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining rows from the most recent query."""

        rows = self.connection.fetchall()
        return list(rows)

    def fetch_df(self, *, date_as_object: bool = False) -> Any:
        """Return the most recent result as a Pandas DataFrame."""

        helper = "DuckCon.fetch_df"
        self._require_module("pandas", helper, "pip install pandas")
        return self.connection.fetch_df(date_as_object=date_as_object)

    def fetchdf(self, *, date_as_object: bool = False) -> Any:
        """Alias for :meth:`fetch_df` matching DuckDB's naming."""

        return self.fetch_df(date_as_object=date_as_object)

    def fetch_df_chunk(
        self,
        vectors_per_chunk: SupportsInt | None = None,
        *,
        date_as_object: bool = False,
    ) -> Any:
        """Return a chunk of rows from the most recent result as a DataFrame."""

        helper = "DuckCon.fetch_df_chunk"
        self._require_module("pandas", helper, "pip install pandas")
        if vectors_per_chunk is None:
            return self.connection.fetch_df_chunk(date_as_object=date_as_object)
        normalised = self._normalise_fetch_size(
            vectors_per_chunk,
            helper=helper,
            parameter="vectors_per_chunk",
        )
        return self.connection.fetch_df_chunk(
            normalised, date_as_object=date_as_object
        )

    def fetchnumpy(self) -> dict[str, Any]:
        """Return the most recent result as NumPy arrays keyed by column."""

        helper = "DuckCon.fetchnumpy"
        self._require_module("numpy", helper, "pip install numpy")
        arrays = self.connection.fetchnumpy()
        return dict(arrays)

    def fetch_arrow_table(
        self, rows_per_batch: SupportsInt | None = None
    ) -> Any:
        """Return the most recent result as a PyArrow table."""

        helper = "DuckCon.fetch_arrow_table"
        self._require_module("pyarrow", helper, "pip install pyarrow")
        if rows_per_batch is None:
            return self.connection.fetch_arrow_table()
        normalised = self._normalise_fetch_size(
            rows_per_batch, helper=helper, parameter="rows_per_batch"
        )
        return self.connection.fetch_arrow_table(normalised)

    def fetch_record_batch(
        self, rows_per_batch: SupportsInt | None = None
    ) -> Any:
        """Return the most recent result as a PyArrow RecordBatch reader."""

        helper = "DuckCon.fetch_record_batch"
        self._require_module("pyarrow", helper, "pip install pyarrow")
        if rows_per_batch is None:
            return self.connection.fetch_record_batch()
        normalised = self._normalise_fetch_size(
            rows_per_batch, helper=helper, parameter="rows_per_batch"
        )
        return self.connection.fetch_record_batch(normalised)

    def close(self) -> None:
        """Close the managed connection if it is open."""

        if self._connection is None:
            return
        self._connection.close()
        self._connection = None

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """Return a new cursor object bound to the managed connection."""

        return self.connection.cursor()

    def install_extension(
        self, name: str, *, force_install: bool = False
    ) -> None:
        """Install a DuckDB extension on the managed connection."""

        if not isinstance(name, str):
            msg = "install_extension name must be a string"
            raise TypeError(msg)
        normalised = name.strip()
        if not normalised:
            msg = "install_extension name cannot be empty"
            raise ValueError(msg)
        self.connection.install_extension(normalised, force_install=force_install)

    def load_extension(self, name: str) -> None:
        """Load a previously installed DuckDB extension."""

        if not isinstance(name, str):
            msg = "load_extension name must be a string"
            raise TypeError(msg)
        normalised = name.strip()
        if not normalised:
            msg = "load_extension name cannot be empty"
            raise ValueError(msg)
        self.connection.load_extension(normalised)

    def interrupt(self) -> None:
        """Interrupt the currently running query on the managed connection."""

        self.connection.interrupt()

    def register(self, name: str, value: object) -> duckdb.DuckDBPyConnection:
        """Register ``value`` as a virtual table under ``name``."""

        if not isinstance(name, str):
            msg = "register name must be a string"
            raise TypeError(msg)
        normalised = name.strip()
        if not normalised:
            msg = "register name cannot be empty"
            raise ValueError(msg)
        return self.connection.register(normalised, value)

    def unregister(self, name: str) -> duckdb.DuckDBPyConnection:
        """Remove a previously registered virtual table."""

        if not isinstance(name, str):
            msg = "unregister name must be a string"
            raise TypeError(msg)
        normalised = name.strip()
        if not normalised:
            msg = "unregister name cannot be empty"
            raise ValueError(msg)
        return self.connection.unregister(normalised)

    def type(self, specifier: TypeSpecifier) -> sqltypes.DuckDBPyType:
        """Return a DuckDB type object for ``specifier``."""

        return self._normalise_type_specifier(
            specifier,
            helper="DuckCon.type",
            parameter="specifier",
        )

    def sqltype(self, specifier: TypeSpecifier) -> sqltypes.DuckDBPyType:
        """Alias for :meth:`type` preserving DuckDB's original API."""

        return self.type(specifier)

    def decimal_type(
        self,
        precision: SupportsInt,
        scale: SupportsInt,
    ) -> sqltypes.DuckDBPyType:
        """Create a DECIMAL type with the given ``precision`` and ``scale``."""

        helper = "DuckCon.decimal_type"
        normalised_precision = self._normalise_fetch_size(
            precision,
            helper=helper,
            parameter="precision",
        )
        normalised_scale = self._normalise_non_negative_int(
            scale,
            helper=helper,
            parameter="scale",
        )
        if normalised_scale > normalised_precision:
            msg = "DuckCon.decimal_type scale cannot exceed precision"
            raise ValueError(msg)
        return self.connection.decimal_type(normalised_precision, normalised_scale)

    def string_type(self, collation: str | None = None) -> sqltypes.DuckDBPyType:
        """Create a DuckDB string type with an optional ``collation``."""

        if collation is None:
            normalised = ""
        else:
            if not isinstance(collation, str):
                msg = "DuckCon.string_type collation must be a string"
                raise TypeError(msg)
            normalised = collation.strip()
        return self.connection.string_type(normalised)

    def list_type(self, element: TypeSpecifier) -> sqltypes.DuckDBPyType:
        """Create a DuckDB LIST type for ``element``."""

        helper = "DuckCon.list_type"
        duck_type = self._normalise_type_specifier(
            element,
            helper=helper,
            parameter="element",
        )
        return self.connection.list_type(duck_type)

    def array_type(
        self,
        element: TypeSpecifier,
        size: SupportsInt,
    ) -> sqltypes.DuckDBPyType:
        """Create a DuckDB ARRAY type of ``element`` with ``size`` entries."""

        helper = "DuckCon.array_type"
        duck_type = self._normalise_type_specifier(
            element,
            helper=helper,
            parameter="element",
        )
        normalised_size = self._normalise_fetch_size(
            size,
            helper=helper,
            parameter="size",
        )
        return self.connection.array_type(duck_type, normalised_size)

    def map_type(
        self,
        key_type: TypeSpecifier,
        value_type: TypeSpecifier,
    ) -> sqltypes.DuckDBPyType:
        """Create a DuckDB MAP type from ``key_type`` and ``value_type``."""

        helper = "DuckCon.map_type"
        normalised_key = self._normalise_type_specifier(
            key_type,
            helper=helper,
            parameter="key_type",
        )
        normalised_value = self._normalise_type_specifier(
            value_type,
            helper=helper,
            parameter="value_type",
        )
        return self.connection.map_type(normalised_key, normalised_value)

    def struct_type(self, fields: FieldMapping) -> sqltypes.DuckDBPyType:
        """Create a DuckDB STRUCT type with ``fields``."""

        helper = "DuckCon.struct_type"
        normalised = self._normalise_field_mapping(
            fields,
            helper=helper,
            parameter="fields",
        )
        return self.connection.struct_type(dict(normalised))

    def row_type(self, fields: FieldMapping) -> sqltypes.DuckDBPyType:
        """Create a DuckDB ROW type with ``fields``."""

        helper = "DuckCon.row_type"
        normalised = self._normalise_field_mapping(
            fields,
            helper=helper,
            parameter="fields",
        )
        return self.connection.row_type(dict(normalised))

    def union_type(self, members: FieldMapping) -> sqltypes.DuckDBPyType:
        """Create a DuckDB UNION type with ``members``."""

        helper = "DuckCon.union_type"
        normalised = self._normalise_field_mapping(
            members,
            helper=helper,
            parameter="members",
        )
        return self.connection.union_type(dict(normalised))

    def enum_type(
        self,
        name: str,
        base_type: TypeSpecifier,
        values: Sequence[str],
    ) -> sqltypes.DuckDBPyType:
        """Create a DuckDB ENUM type from ``values``."""

        helper = "DuckCon.enum_type"
        normalised_name = self._normalise_identifier(
            name,
            helper=helper,
            parameter="name",
        )
        normalised_type = self._normalise_type_specifier(
            base_type,
            helper=helper,
            parameter="base_type",
        )
        normalised_values = self._normalise_string_sequence(
            values,
            helper=helper,
            parameter="values",
        )
        return self.connection.enum_type(
            normalised_name,
            normalised_type,
            normalised_values,
        )

    def table(self, name: str) -> "Table":
        """Return a managed table wrapper bound to this connection."""

        from .table import Table  # pylint: disable=import-outside-toplevel

        return Table(self, name)

    @staticmethod
    def _require_module(module: str, helper: str, install_hint: str) -> Any:
        try:
            return importlib.import_module(module)
        except ModuleNotFoundError as exc:
            msg = f"{helper} requires {module}. Install it with {install_hint}."
            raise ModuleNotFoundError(msg) from exc

    @staticmethod
    def _normalise_fetch_size(
        value: SupportsInt, *, helper: str, parameter: str
    ) -> int:
        if isinstance(value, bool):
            msg = f"{helper} {parameter} must be an integer"
            raise TypeError(msg)
        try:
            normalised = int(value)
        except (TypeError, ValueError) as error:  # pragma: no cover - defensive
            msg = f"{helper} {parameter} must be an integer"
            raise TypeError(msg) from error
        if normalised <= 0:
            msg = f"{helper} {parameter} must be greater than zero"
            raise ValueError(msg)
        return normalised

    @staticmethod
    def _normalise_non_negative_int(
        value: SupportsInt,
        *,
        helper: str,
        parameter: str,
    ) -> int:
        if isinstance(value, bool):
            msg = f"{helper} {parameter} must be an integer"
            raise TypeError(msg)
        try:
            normalised = int(value)
        except (TypeError, ValueError) as error:  # pragma: no cover - defensive
            msg = f"{helper} {parameter} must be an integer"
            raise TypeError(msg) from error
        if normalised < 0:
            msg = f"{helper} {parameter} must be zero or greater"
            raise ValueError(msg)
        return normalised

    def _normalise_type_specifier(
        self,
        specifier: TypeSpecifier,
        *,
        helper: str,
        parameter: str,
    ) -> sqltypes.DuckDBPyType:
        if isinstance(specifier, sqltypes.DuckDBPyType):
            return specifier
        if isinstance(specifier, DuckDBType):
            rendered = specifier.render()
        elif isinstance(specifier, str):
            rendered = specifier.strip()
            if not rendered:
                msg = f"{helper} {parameter} cannot be empty"
                raise ValueError(msg)
        else:
            msg = (
                f"{helper} {parameter} must be a string, DuckDBType, or DuckDBPyType"
            )
            raise TypeError(msg)
        return self.connection.type(rendered)

    def _normalise_field_mapping(
        self,
        fields: FieldMapping,
        *,
        helper: str,
        parameter: str,
    ) -> list[tuple[str, sqltypes.DuckDBPyType]]:
        if isinstance(fields, Mapping):
            items = list(fields.items())
        else:
            if isinstance(fields, (str, bytes)):
                msg = f"{helper} {parameter} must be a mapping or sequence of pairs"
                raise TypeError(msg)
            items = []
            for index, entry in enumerate(fields):
                if not isinstance(entry, Sequence) or len(entry) != 2:
                    msg = (
                        f"{helper} {parameter}[{index}] must be a (name, type) pair"
                    )
                    raise TypeError(msg)
                items.append((entry[0], entry[1]))
        if not items:
            msg = f"{helper} {parameter} cannot be empty"
            raise ValueError(msg)
        normalised: list[tuple[str, sqltypes.DuckDBPyType]] = []
        seen: set[str] = set()
        for index, (name, specifier) in enumerate(items):
            normalised_name = self._normalise_identifier(
                name,
                helper=helper,
                parameter=f"{parameter}[{index}] name",
            )
            if normalised_name in seen:
                msg = (
                    f"{helper} {parameter} contains duplicate field '{normalised_name}'"
                )
                raise ValueError(msg)
            seen.add(normalised_name)
            normalised_type = self._normalise_type_specifier(
                specifier,
                helper=helper,
                parameter=f"{parameter}[{normalised_name}] type",
            )
            normalised.append((normalised_name, normalised_type))
        return normalised

    @staticmethod
    def _normalise_identifier(
        value: str,
        *,
        helper: str,
        parameter: str,
    ) -> str:
        if not isinstance(value, str):
            msg = f"{helper} {parameter} must be a string"
            raise TypeError(msg)
        normalised = value.strip()
        if not normalised:
            msg = f"{helper} {parameter} cannot be empty"
            raise ValueError(msg)
        return normalised

    @staticmethod
    def _normalise_string_sequence(
        values: Sequence[str],
        *,
        helper: str,
        parameter: str,
    ) -> list[str]:
        if isinstance(values, (str, bytes)):
            msg = f"{helper} {parameter} must be a sequence of strings"
            raise TypeError(msg)
        normalised: list[str] = []
        seen: set[str] = set()
        for index, value in enumerate(values):
            if not isinstance(value, str):
                msg = f"{helper} {parameter}[{index}] must be a string"
                raise TypeError(msg)
            trimmed = value.strip()
            if not trimmed:
                msg = f"{helper} {parameter}[{index}] cannot be empty"
                raise ValueError(msg)
            if trimmed in seen:
                msg = (
                    f"{helper} {parameter} contains duplicate value '{trimmed}'"
                )
                raise ValueError(msg)
            seen.add(trimmed)
            normalised.append(trimmed)
        if not normalised:
            msg = f"{helper} {parameter} cannot be empty"
            raise ValueError(msg)
        return normalised

    if TYPE_CHECKING:  # pragma: no cover - import for typing only
        from .relation import Relation
        from .table import Table


HelperFunction = TypeVar("HelperFunction", bound=Callable[..., Any])


if TYPE_CHECKING:  # pragma: no cover - re-export for type checking
    from .io import duckcon_helper as duckcon_helper
else:

    def duckcon_helper(helper: HelperFunction) -> HelperFunction:
        """Proxy to :mod:`duckplus.io`'s decorator at call time."""

        from .io import duckcon_helper as _duckcon_helper

        return _duckcon_helper(helper)


