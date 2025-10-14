"""Connection helpers for Duck+."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from os import PathLike, fspath
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Optional, Self, cast

import duckdb

from . import util
from .core import DuckRel
from .relation.core import Relation
from .schema import AnyRow

if TYPE_CHECKING:
    from . import io as io_module
    from .odbc import MySQLStrategy, PostgresStrategy
    from .table import DuckTable
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame
else:  # pragma: no cover - runtime aliases
    PandasDataFrame = object
    PolarsDataFrame = object

Pathish = str | PathLike[str]


def _validate_connection_string(value: object) -> str:
    """Return *value* cast to ``str`` after validation."""

    if not isinstance(value, str):
        raise TypeError(
            "Connection string must be provided as a string; "
            f"received {type(value).__name__}."
        )

    if not value or not value.strip():
        raise ValueError("Connection string must not be empty.")

    return value


def _validate_query(value: object, *, parameter: str) -> str:
    """Return *value* cast to ``str`` after validation."""

    if not isinstance(value, str):
        raise TypeError(
            f"{parameter} must be provided as a string; "
            f"received {type(value).__name__}."
        )

    if not value or not value.strip():
        raise ValueError(f"{parameter} must not be empty.")

    return value


class DuckConnection(AbstractContextManager["DuckConnection"]):
    """Lightweight wrapper around :mod:`duckdb` connections."""

    def __init__(
        self,
        database: Optional[Pathish] = None,
        *,
        read_only: bool = False,
        config: Mapping[str, str] | None = None,
    ) -> None:
        db_name = ":memory:" if database is None else fspath(database)
        config_map = None if config is None else {util.ensure_identifier(k): str(v) for k, v in config.items()}
        if config_map is None:
            self._raw = duckdb.connect(database=db_name, read_only=read_only)
        else:
            self._raw = duckdb.connect(database=db_name, read_only=read_only, config=config_map)
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

    def read_parquet(
        self,
        paths: "io_module.PathsLike",
        /,
        *,
        binary_as_string: bool | None = None,
        file_row_number: bool | None = None,
        filename: bool | None = None,
        hive_partitioning: bool | None = None,
        union_by_name: bool | None = None,
        can_have_nan: bool | None = None,
        compression: "io_module.ParquetCompression" | None = None,
        parquet_version: "io_module.ParquetVersion" | None = None,
        debug_use_openssl: bool | None = None,
        explicit_cardinality: int | None = None,
    ) -> DuckRel[AnyRow]:
        """Read Parquet data via :mod:`duckplus.io`.

        Parameters mirror :func:`duckplus.io.read_parquet`; see that function for
        the full list of supported options and examples.
        """

        from . import io as io_module

        reader = io_module._ParquetReader.from_connection(
            self,
            paths,
            binary_as_string=binary_as_string,
            file_row_number=file_row_number,
            filename=filename,
            hive_partitioning=hive_partitioning,
            union_by_name=union_by_name,
            can_have_nan=can_have_nan,
            compression=compression,
            parquet_version=parquet_version,
            debug_use_openssl=debug_use_openssl,
            explicit_cardinality=explicit_cardinality,
        )
        return reader.run()

    def read_csv(
        self,
        paths: "io_module.PathsLike",
        /,
        *,
        encoding: str = "utf-8",
        header: bool | int = True,
        delimiter: str | None = None,
        quote: str | None = None,
        escape: str | None = None,
        nullstr: str | Sequence[str] | None = None,
        sample_size: int | None = None,
        auto_detect: bool | None = None,
        ignore_errors: bool | None = None,
        dateformat: str | None = None,
        timestampformat: str | None = None,
        decimal_separator: "io_module.CSVDecimalSeparator" | None = None,
        columns: Mapping[str, util.DuckDBType] | None = None,
        all_varchar: bool | None = None,
        parallel: bool | None = None,
        allow_quoted_nulls: bool | None = None,
        null_padding: bool | None = None,
        normalize_names: bool | None = None,
        union_by_name: bool | None = None,
        filename: bool | None = None,
        hive_partitioning: bool | None = None,
        hive_types_autocast: bool | None = None,
        hive_types: Mapping[str, util.DuckDBType] | None = None,
        files_to_sniff: int | None = None,
        compression: "io_module.CSVCompression" | None = None,
        thousands: str | None = None,
    ) -> DuckRel[AnyRow]:
        """Read CSV data via :mod:`duckplus.io`.

        Parameters mirror :func:`duckplus.io.read_csv`; see that function for the
        detailed option reference and examples.
        """

        from . import io as io_module

        reader = io_module._CSVReader.from_connection(
            self,
            paths,
            encoding=encoding,
            header=header,
            delimiter=delimiter,
            quote=quote,
            escape=escape,
            nullstr=nullstr,
            sample_size=sample_size,
            auto_detect=auto_detect,
            ignore_errors=ignore_errors,
            dateformat=dateformat,
            timestampformat=timestampformat,
            decimal_separator=decimal_separator,
            columns=columns,
            all_varchar=all_varchar,
            parallel=parallel,
            allow_quoted_nulls=allow_quoted_nulls,
            null_padding=null_padding,
            normalize_names=normalize_names,
            union_by_name=union_by_name,
            filename=filename,
            hive_partitioning=hive_partitioning,
            hive_types_autocast=hive_types_autocast,
            hive_types=hive_types,
            files_to_sniff=files_to_sniff,
            compression=compression,
            thousands=thousands,
        )
        return reader.run()

    def read_json(
        self,
        paths: "io_module.PathsLike",
        /,
        *,
        columns: Mapping[str, util.DuckDBType] | None = None,
        sample_size: int | None = None,
        maximum_depth: int | None = None,
        records: "io_module.JSONRecords" | None = None,
        format: "io_module.JSONFormat" | None = None,
        dateformat: str | None = None,
        timestampformat: str | None = None,
        compression: "io_module.JSONCompression" | None = None,
        maximum_object_size: int | None = None,
        ignore_errors: bool | None = None,
        convert_strings_to_integers: bool | None = None,
        field_appearance_threshold: float | int | None = None,
        map_inference_threshold: int | None = None,
        maximum_sample_files: int | None = None,
        filename: bool | None = None,
        hive_partitioning: bool | None = None,
        union_by_name: bool | None = None,
        hive_types: Mapping[str, util.DuckDBType] | None = None,
        hive_types_autocast: bool | None = None,
        auto_detect: bool | None = None,
    ) -> DuckRel[AnyRow]:
        """Read JSON or NDJSON data via :mod:`duckplus.io`.

        Parameters mirror :func:`duckplus.io.read_json`; see that function for
        the detailed option reference and examples.
        """

        from . import io as io_module

        reader = io_module._JSONReader.from_connection(
            self,
            paths,
            columns=columns,
            sample_size=sample_size,
            maximum_depth=maximum_depth,
            records=records,
            format=format,
            dateformat=dateformat,
            timestampformat=timestampformat,
            compression=compression,
            maximum_object_size=maximum_object_size,
            ignore_errors=ignore_errors,
            convert_strings_to_integers=convert_strings_to_integers,
            field_appearance_threshold=field_appearance_threshold,
            map_inference_threshold=map_inference_threshold,
            maximum_sample_files=maximum_sample_files,
            filename=filename,
            hive_partitioning=hive_partitioning,
            union_by_name=union_by_name,
            hive_types=hive_types,
            hive_types_autocast=hive_types_autocast,
            auto_detect=auto_detect,
        )
        return reader.run()

    def from_pandas(self, frame: PandasDataFrame) -> DuckRel[AnyRow]:
        """Return a relation constructed from a pandas DataFrame."""

        return cast(Relation[AnyRow], Relation.from_pandas(frame, connection=self))

    def from_polars(self, frame: PolarsDataFrame) -> DuckRel[AnyRow]:
        """Return a relation constructed from a Polars DataFrame."""

        return cast(Relation[AnyRow], Relation.from_polars(frame, connection=self))

    def table(self, name: str) -> "DuckTable":
        """Return a :class:`duckplus.DuckTable` wrapper for *name* on this connection."""

        from .table import DuckTable

        return DuckTable(self, name)


def connect(
    database: Optional[Pathish] = None,
    *,
    read_only: bool = False,
    config: Mapping[str, str] | None = None,
) -> DuckConnection:
    """Create a :class:`duckplus.DuckConnection`.

    Parameters
    ----------
    database:
        Optional database path. Defaults to in-memory storage when ``None``.
    read_only:
        Whether the connection should be opened in read-only mode.
    config:
        Optional DuckDB configuration parameters to apply when opening the
        connection.
    """

    return DuckConnection(database=database, read_only=read_only, config=config)


def load_extensions(conn: DuckConnection, extensions: Sequence[str]) -> None:
    """Load DuckDB extensions by name."""

    if not extensions:
        return

    raw = conn.raw
    for name in extensions:
        normalized = util.ensure_identifier(name)
        raw.load_extension(normalized)


def attach_nanodbc(
    conn: DuckConnection,
    *,
    alias: str,
    connection_string: str,
    read_only: bool = True,
    load_extension: bool = True,
) -> None:
    """Attach an ODBC data source via the ``nanodbc`` extension.

    Parameters
    ----------
    conn:
        Target connection that will host the attached database.
    alias:
        Schema name to expose the remote database under. Must be a valid
        DuckDB identifier.
    connection_string:
        ODBC connection string describing the target data source.
    read_only:
        When ``True`` (the default) the attachment is marked as read-only. Set
        to ``False`` to request write access when the ODBC source permits it.
    load_extension:
        Automatically load the ``nanodbc`` extension before attaching. Disable
        when the extension is already loaded.
    """

    normalized_connection_string = _validate_connection_string(connection_string)

    alias_identifier = util.ensure_identifier(alias)

    if load_extension:
        load_extensions(conn, ["nanodbc"])

    options = ["TYPE ODBC"]
    if read_only:
        options.append("READ_ONLY")
    else:
        options.append("READ_ONLY=FALSE")

    option_clause = ", ".join(options)

    sql = f"ATTACH ? AS {alias_identifier} ({option_clause})"
    conn.raw.execute(sql, [normalized_connection_string])


def query_nanodbc(
    conn: DuckConnection,
    *,
    connection_string: str,
    query: str,
    load_extension: bool = True,
) -> DuckRel[AnyRow]:
    """Execute a remote query via the ``nanodbc`` extension.

    Parameters
    ----------
    conn:
        Target connection that will materialize the query results.
    connection_string:
        ODBC connection string describing the target data source.
    query:
        SQL query to execute upstream. The text is passed through to the ODBC
        data source without modification.
    load_extension:
        Automatically load the ``nanodbc`` extension before querying. Disable
        when the extension is already loaded.
    """

    normalized_connection_string = _validate_connection_string(connection_string)
    normalized_query = _validate_query(query, parameter="Query text")

    if load_extension:
        load_extensions(conn, ["nanodbc"])

    relation = conn.raw.sql(
        "SELECT * FROM odbc_query(?, ?)",
        params=(normalized_connection_string, normalized_query),
    )
    return Relation(relation)


def __getattr__(name: str) -> Any:
    if name in {"MySQLStrategy", "PostgresStrategy"}:
        from .odbc import MySQLStrategy as _MySQLStrategy, PostgresStrategy as _PostgresStrategy

        mapping = {
            "MySQLStrategy": _MySQLStrategy,
            "PostgresStrategy": _PostgresStrategy,
        }
        value = mapping[name]
        globals()[name] = value
        return value
    raise AttributeError(name)


__all__ = [
    "DuckConnection",
    "attach_nanodbc",
    "connect",
    "load_extensions",
    "MySQLStrategy",
    "PostgresStrategy",
    "query_nanodbc",
]
