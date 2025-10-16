"""File-based relation helpers built on top of :class:`duckplus.DuckCon`."""

# pylint: disable=import-error,too-many-arguments,redefined-builtin,too-many-locals

from __future__ import annotations
from os import PathLike, fspath
from typing import Mapping, Sequence
from uuid import uuid4

import duckdb  # type: ignore[import-not-found]

from ..duckcon import DuckCon
from ..relation import Relation

__all__ = [
    "read_csv",
    "read_parquet",
    "read_json",
    "append_csv",
    "append_ndjson",
]

PathType = str | PathLike[str]


def _filter_none(**options: object) -> dict[str, object]:
    """Return a dictionary containing only non-``None`` values."""

    return {key: value for key, value in options.items() if value is not None}


def _require_connection(duckcon: DuckCon, helper: str) -> duckdb.DuckDBPyConnection:
    if not duckcon.is_open:
        msg = (
            f"DuckCon connection must be open to call {helper}. "
            "Use DuckCon as a context manager."
        )
        raise RuntimeError(msg)
    return duckcon.connection


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def _prepare_table_identifier(table: str, helper: str) -> str:
    if not isinstance(table, str):
        msg = f"{helper} table name must be a string"
        raise TypeError(msg)
    stripped = table.strip()
    if not stripped:
        msg = f"{helper} table name cannot be empty"
        raise ValueError(msg)
    parts = stripped.split(".")
    if any(not part for part in parts):
        msg = f"{helper} table name '{table}' is not a valid qualified identifier"
        raise ValueError(msg)
    return ".".join(_quote_identifier(part) for part in parts)


def _normalise_target_columns(
    target_columns: Sequence[str] | None, helper: str
) -> tuple[str, ...] | None:
    if target_columns is None:
        return None
    if isinstance(target_columns, (str, bytes)):
        msg = f"{helper} target_columns must be a sequence of column names"
        raise TypeError(msg)

    normalised: list[str] = []
    seen: set[str] = set()
    for column in target_columns:
        if not isinstance(column, str):
            msg = f"{helper} target_columns must only contain strings"
            raise TypeError(msg)
        trimmed = column.strip()
        if not trimmed:
            msg = f"{helper} target column names cannot be empty"
            raise ValueError(msg)
        key = trimmed.casefold()
        if key in seen:
            msg = f"{helper} target column '{column}' specified multiple times"
            raise ValueError(msg)
        seen.add(key)
        normalised.append(trimmed)

    if not normalised:
        msg = f"{helper} target_columns must contain at least one column"
        raise ValueError(msg)
    return tuple(normalised)


def _append_relation_data(
    connection: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    table: str,
    helper: str,
    *,
    target_columns: tuple[str, ...] | None,
    create: bool,
    overwrite: bool,
) -> None:
    table_identifier = _prepare_table_identifier(table, helper)
    view_name = f"duckplus_{helper}_{uuid4().hex}"
    relation.create_view(view_name, replace=True)
    quoted_view = _quote_identifier(view_name)

    try:
        if create:
            if target_columns is not None:
                msg = f"{helper} does not support target_columns when create=True"
                raise ValueError(msg)
            if overwrite:
                connection.execute(f"DROP TABLE IF EXISTS {table_identifier}")
            connection.execute(
                f"CREATE TABLE {table_identifier} AS SELECT * FROM {quoted_view}"
            )
        else:
            if overwrite:
                connection.execute(f"DELETE FROM {table_identifier}")
            if target_columns is None:
                connection.execute(
                    f"INSERT INTO {table_identifier} SELECT * FROM {quoted_view}"
                )
            else:
                columns_sql = ", ".join(
                    _quote_identifier(column) for column in target_columns
                )
                connection.execute(
                    f"INSERT INTO {table_identifier} ({columns_sql}) "
                    f"SELECT {columns_sql} FROM {quoted_view}"
                )
    finally:
        connection.execute(f"DROP VIEW IF EXISTS {quoted_view}")


def read_csv(
    duckcon: DuckCon,
    source: PathType,
    *,
    header: bool = True,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    sample_size: int | None = None,
    auto_detect: bool = True,
    columns: Mapping[str, str] | None = None,
) -> Relation:
    """Load a CSV file into a :class:`Relation`."""

    connection = _require_connection(duckcon, "read_csv")
    path = fspath(source)

    kwargs: dict[str, object] = {
        "header": header,
        "delimiter": delimiter,
        "auto_detect": auto_detect,
    }
    kwargs.update(
        _filter_none(
            quotechar=quotechar,
            escapechar=escapechar,
            sample_size=sample_size,
            columns=dict(columns) if columns is not None else None,
        )
    )

    relation = connection.read_csv(path, **kwargs)
    return Relation.from_relation(duckcon, relation)


def read_parquet(
    duckcon: DuckCon,
    source: PathType,
    *,
    binary_as_string: bool | None = None,
    file_row_number: bool | None = None,
    filename: bool | None = None,
    hive_partitioning: bool | None = None,
    columns: Sequence[str] | None = None,
) -> Relation:
    """Load a Parquet file into a :class:`Relation`."""

    connection = _require_connection(duckcon, "read_parquet")
    path = fspath(source)

    kwargs = _filter_none(
        binary_as_string=binary_as_string,
        file_row_number=file_row_number,
        filename=filename,
        hive_partitioning=hive_partitioning,
        columns=list(columns) if columns is not None else None,
    )

    relation = connection.read_parquet(path, **kwargs)
    return Relation.from_relation(duckcon, relation)


def read_json(
    duckcon: DuckCon,
    source: PathType,
    *,
    columns: object | None = None,
    sample_size: object | None = None,
    maximum_depth: object | None = None,
    records: str | None = None,
    format: str | None = None,
    date_format: object | None = None,
    timestamp_format: object | None = None,
    compression: object | None = None,
    maximum_object_size: object | None = None,
    ignore_errors: object | None = None,
    convert_strings_to_integers: object | None = None,
    field_appearance_threshold: object | None = None,
    map_inference_threshold: object | None = None,
    maximum_sample_files: object | None = None,
    filename: object | None = None,
    hive_partitioning: object | None = None,
    union_by_name: object | None = None,
    hive_types: object | None = None,
    hive_types_autocast: object | None = None,
) -> Relation:
    """Load a JSON document or JSON Lines file into a :class:`Relation`."""

    connection = _require_connection(duckcon, "read_json")
    path = fspath(source)

    kwargs = _filter_none(
        columns=columns,
        sample_size=sample_size,
        maximum_depth=maximum_depth,
        records=records,
        format=format,
        date_format=date_format,
        timestamp_format=timestamp_format,
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
    )

    relation = connection.read_json(path, **kwargs)
    return Relation.from_relation(duckcon, relation)


def append_csv(
    duckcon: DuckCon,
    table: str,
    source: PathType,
    *,
    target_columns: Sequence[str] | None = None,
    create: bool = False,
    overwrite: bool = False,
    header: bool = True,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    sample_size: int | None = None,
    auto_detect: bool = True,
    columns: Mapping[str, str] | None = None,
) -> None:
    """Append rows from a CSV file into a DuckDB table."""

    connection = _require_connection(duckcon, "append_csv")
    target_column_list = _normalise_target_columns(target_columns, "append_csv")
    path = fspath(source)

    kwargs: dict[str, object] = {
        "header": header,
        "delimiter": delimiter,
        "auto_detect": auto_detect,
    }
    kwargs.update(
        _filter_none(
            quotechar=quotechar,
            escapechar=escapechar,
            sample_size=sample_size,
            columns=dict(columns) if columns is not None else None,
        )
    )

    relation = connection.read_csv(path, **kwargs)
    _append_relation_data(
        connection,
        relation,
        table,
        "append_csv",
        target_columns=target_column_list,
        create=create,
        overwrite=overwrite,
    )


def append_ndjson(
    duckcon: DuckCon,
    table: str,
    source: PathType,
    *,
    target_columns: Sequence[str] | None = None,
    create: bool = False,
    overwrite: bool = False,
    columns: object | None = None,
    sample_size: object | None = None,
    maximum_depth: object | None = None,
    records: object | None = "auto",
    format: object | None = None,
    date_format: object | None = None,
    timestamp_format: object | None = None,
    compression: object | None = None,
    maximum_object_size: object | None = None,
    ignore_errors: object | None = None,
    convert_strings_to_integers: object | None = None,
    field_appearance_threshold: object | None = None,
    map_inference_threshold: object | None = None,
    maximum_sample_files: object | None = None,
    filename: object | None = None,
    hive_partitioning: object | None = None,
    union_by_name: object | None = None,
    hive_types: object | None = None,
    hive_types_autocast: object | None = None,
) -> None:
    """Append NDJSON rows into a DuckDB table."""

    connection = _require_connection(duckcon, "append_ndjson")
    target_column_list = _normalise_target_columns(target_columns, "append_ndjson")
    path = fspath(source)

    kwargs = _filter_none(
        columns=columns,
        sample_size=sample_size,
        maximum_depth=maximum_depth,
        records=records,
        format=format,
        date_format=date_format,
        timestamp_format=timestamp_format,
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
    )

    relation = connection.read_json(path, **kwargs)
    _append_relation_data(
        connection,
        relation,
        table,
        "append_ndjson",
        target_columns=target_column_list,
        create=create,
        overwrite=overwrite,
    )
