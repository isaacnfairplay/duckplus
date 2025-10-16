"""File-based relation helpers built on top of :class:`duckplus.DuckCon`."""

# pylint: disable=import-error,too-many-arguments,redefined-builtin,too-many-locals

from __future__ import annotations
from os import PathLike, fspath
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Any, TypedDict, cast

import duckdb  # type: ignore[import-not-found]

from .._table_utils import quote_identifier, require_connection
from ..duckcon import DuckCon
from ..relation import Relation

__all__ = [
    "read_csv",
    "read_parquet",
    "read_json",
]

PathLikeInput = Path | PathLike[str] | str


def _ensure_path(value: PathLikeInput) -> Path | str:
    """Normalise a supported path-like input into a DuckDB-compatible path."""

    if isinstance(value, Path):
        return value

    if isinstance(value, str):
        return value

    return Path(fspath(value))


def _normalise_path_argument(
    source: PathLikeInput | Sequence[PathLikeInput],
) -> str | list[str]:
    """Return a DuckDB-compatible path or sequence of paths."""

    if isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray)):
        return [str(_ensure_path(item)) for item in source]

    return str(_ensure_path(source))


class CSVReadKeywordOptions(TypedDict, total=False):
    """DuckDB ``read_csv`` keyword arguments supported by duckplus wrappers."""

    header: bool
    delimiter: str
    quotechar: str
    escapechar: str
    sample_size: int
    auto_detect: bool
    columns: object
    dtype: object
    names: list[str]
    na_values: list[str]
    null_padding: bool
    force_not_null: list[str]
    files_to_sniff: int
    decimal: str
    date_format: str
    timestamp_format: str
    encoding: str
    compression: str
    hive_types_autocast: bool
    all_varchar: bool
    hive_partitioning: bool
    comment: str
    max_line_size: int
    store_rejects: bool
    rejects_table: str
    rejects_limit: int
    rejects_scan: str
    union_by_name: bool
    filename: bool
    normalize_names: bool
    ignore_errors: bool
    allow_quoted_nulls: bool
    auto_type_candidates: object
    parallel: bool
    skiprows: int


def _filter_none(**options: object) -> dict[str, object]:
    """Return a dictionary containing only non-``None`` values."""

    return {key: value for key, value in options.items() if value is not None}


def _build_csv_options(
    *,
    header: bool | None = None,
    delimiter: str | None = None,
    delim: str | None = None,
    quotechar: str | None = None,
    quote: str | None = None,
    escapechar: str | None = None,
    escape: str | None = None,
    sample_size: int | None = None,
    auto_detect: bool | None = None,
    columns: object | None = None,
    dtype: object | None = None,
    names: Sequence[str] | None = None,
    na_values: Sequence[str] | None = None,
    null_padding: bool | None = None,
    force_not_null: Sequence[str] | None = None,
    files_to_sniff: int | None = None,
    decimal: str | None = None,
    decimal_separator: str | None = None,
    date_format: str | None = None,
    dateformat: str | None = None,
    timestamp_format: str | None = None,
    timestampformat: str | None = None,
    encoding: str | None = None,
    compression: str | None = None,
    hive_types_autocast: bool | None = None,
    all_varchar: bool | None = None,
    hive_partitioning: bool | None = None,
    comment: str | None = None,
    max_line_size: int | None = None,
    maximum_line_size: int | None = None,
    store_rejects: bool | None = None,
    rejects_table: str | None = None,
    rejects_limit: int | None = None,
    rejects_scan: str | None = None,
    union_by_name: bool | None = None,
    filename: bool | None = None,
    normalize_names: bool | None = None,
    ignore_errors: bool | None = None,
    allow_quoted_nulls: bool | None = None,
    auto_type_candidates: Sequence[str] | str | None = None,
    parallel: bool | None = None,
    skiprows: int | None = None,
    skip: int | None = None,
) -> CSVReadKeywordOptions:
    """Normalise keyword arguments shared between CSV readers."""

    if delim is not None:
        if delimiter is not None and delimiter != delim:
            msg = "Both 'delimiter' and alias 'delim' were provided"
            raise ValueError(msg)
        delimiter = delim

    if quote is not None:
        if quotechar is not None and quotechar != quote:
            msg = "Both 'quotechar' and alias 'quote' were provided"
            raise ValueError(msg)
        quotechar = quote

    if escape is not None:
        if escapechar is not None and escapechar != escape:
            msg = "Both 'escapechar' and alias 'escape' were provided"
            raise ValueError(msg)
        escapechar = escape

    if decimal_separator is not None:
        if decimal is not None:
            msg = "Both 'decimal' and alias 'decimal_separator' were provided"
            raise ValueError(msg)
        decimal = decimal_separator

    if dateformat is not None:
        if date_format is not None:
            msg = "Both 'date_format' and alias 'dateformat' were provided"
            raise ValueError(msg)
        date_format = dateformat

    if timestampformat is not None:
        if timestamp_format is not None:
            msg = "Both 'timestamp_format' and alias 'timestampformat' were provided"
            raise ValueError(msg)
        timestamp_format = timestampformat

    if maximum_line_size is not None:
        if max_line_size is not None:
            msg = "Both 'max_line_size' and alias 'maximum_line_size' were provided"
            raise ValueError(msg)
        max_line_size = maximum_line_size

    if skip is not None:
        if skiprows is not None:
            msg = "Both 'skiprows' and alias 'skip' were provided"
            raise ValueError(msg)
        skiprows = skip

    options: dict[str, object] = {}

    def set_option(name: str, value: Any) -> None:
        if isinstance(value, Mapping):
            options[name] = dict(value)  # type: ignore[assignment]
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            options[name] = list(value)  # type: ignore[assignment]
        else:
            options[name] = value  # type: ignore[assignment]

    simple_options: dict[str, object | None] = {
        "header": header,
        "delimiter": delimiter,
        "quotechar": quotechar,
        "escapechar": escapechar,
        "sample_size": sample_size,
        "auto_detect": auto_detect,
        "columns": columns,
        "dtype": dtype,
        "null_padding": null_padding,
        "files_to_sniff": files_to_sniff,
        "decimal": decimal,
        "date_format": date_format,
        "timestamp_format": timestamp_format,
        "encoding": encoding,
        "compression": compression,
        "hive_types_autocast": hive_types_autocast,
        "all_varchar": all_varchar,
        "hive_partitioning": hive_partitioning,
        "comment": comment,
        "max_line_size": max_line_size,
        "store_rejects": store_rejects,
        "rejects_table": rejects_table,
        "rejects_limit": rejects_limit,
        "rejects_scan": rejects_scan,
        "union_by_name": union_by_name,
        "filename": filename,
        "normalize_names": normalize_names,
        "ignore_errors": ignore_errors,
        "allow_quoted_nulls": allow_quoted_nulls,
        "parallel": parallel,
        "skiprows": skiprows,
    }

    for key, value in simple_options.items():
        if value is not None:
            set_option(key, value)

    if names is not None:
        set_option("names", names)

    if na_values is not None:
        set_option("na_values", na_values)

    if force_not_null is not None:
        set_option("force_not_null", force_not_null)

    if auto_type_candidates is not None:
        set_option("auto_type_candidates", auto_type_candidates)

    return cast(CSVReadKeywordOptions, options)


class ParquetReadKeywordOptions(TypedDict, total=False):
    """DuckDB ``read_parquet`` keyword arguments supported by duckplus wrappers."""

    binary_as_string: bool
    file_row_number: bool
    filename: bool
    hive_partitioning: bool
    union_by_name: bool
    compression: str


def _normalise_parquet_source(
    source: Path | Sequence[Path],
    *,
    directory: bool = False,
    glob: str | Sequence[str] = "*.parquet",
) -> str | list[str]:
    """Return a DuckDB-compatible Parquet glob or list of globs."""

    if directory:
        if isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray)):
            msg = "read_parquet directory mode expects a single path"
            raise TypeError(msg)

        directory_path = Path(_ensure_path(source))
        if not directory_path.is_dir():
            msg = f"Parquet directory '{directory_path}' does not exist or is not a directory"
            raise ValueError(msg)

        patterns: Sequence[str]
        if isinstance(glob, str):
            patterns = (glob,)
        else:
            patterns = tuple(glob)
            if not patterns:
                msg = "Parquet directory glob patterns cannot be empty"
                raise ValueError(msg)

        collected: set[str] = set()
        for pattern in patterns:
            for path in directory_path.glob(pattern):
                if path.is_file():
                    collected.add(str(path))

        if not collected:
            formatted_patterns = ", ".join(patterns)
            msg = (
                f"No Parquet files matched glob(s) {formatted_patterns!r} in directory "
                f"'{directory_path}'"
            )
            raise FileNotFoundError(msg)

        return sorted(collected)

    return _normalise_path_argument(source)


def _build_parquet_options(
    *,
    binary_as_string: bool | None = None,
    file_row_number: bool | None = None,
    filename: bool | None = None,
    hive_partitioning: bool | None = None,
    union_by_name: bool | None = None,
    compression: str | None = None,
) -> ParquetReadKeywordOptions:
    """Normalise keyword arguments shared between Parquet readers."""

    return cast(
        ParquetReadKeywordOptions,
        _filter_none(
            binary_as_string=binary_as_string,
            file_row_number=file_row_number,
            filename=filename,
            hive_partitioning=hive_partitioning,
            union_by_name=union_by_name,
            compression=compression,
        ),
    )


class JSONReadKeywordOptions(TypedDict, total=False):
    """DuckDB ``read_json`` keyword arguments supported by duckplus wrappers."""

    columns: object
    sample_size: object
    maximum_depth: object
    records: object
    format: object
    date_format: object
    timestamp_format: object
    compression: object
    maximum_object_size: object
    ignore_errors: object
    convert_strings_to_integers: object
    field_appearance_threshold: object
    map_inference_threshold: object
    maximum_sample_files: object
    filename: object
    hive_partitioning: object
    union_by_name: object
    hive_types: object
    hive_types_autocast: object


def _build_json_options(
    *,
    columns: object | None = None,
    sample_size: object | None = None,
    maximum_depth: object | None = None,
    records: object | None = None,
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
) -> JSONReadKeywordOptions:
    """Normalise keyword arguments shared between JSON readers."""

    options = _filter_none(
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

    if columns is not None:
        if isinstance(columns, Mapping):
            options["columns"] = dict(columns)
        elif isinstance(columns, Sequence) and not isinstance(columns, (str, bytes, bytearray)):
            options["columns"] = list(columns)

    return cast(JSONReadKeywordOptions, options)


def read_csv(
    duckcon: DuckCon,
    source: Path | Sequence[Path],
    *,
    header: bool | None = None,
    delimiter: str | None = None,
    delim: str | None = None,
    quotechar: str | None = None,
    quote: str | None = None,
    escapechar: str | None = None,
    escape: str | None = None,
    sample_size: int | None = None,
    auto_detect: bool | None = None,
    columns: object | None = None,
    dtype: object | None = None,
    names: Sequence[str] | None = None,
    na_values: Sequence[str] | None = None,
    null_padding: bool | None = None,
    force_not_null: Sequence[str] | None = None,
    files_to_sniff: int | None = None,
    decimal: str | None = None,
    decimal_separator: str | None = None,
    date_format: str | None = None,
    dateformat: str | None = None,
    timestamp_format: str | None = None,
    timestampformat: str | None = None,
    encoding: str | None = None,
    compression: str | None = None,
    hive_types_autocast: bool | None = None,
    all_varchar: bool | None = None,
    hive_partitioning: bool | None = None,
    comment: str | None = None,
    max_line_size: int | None = None,
    maximum_line_size: int | None = None,
    store_rejects: bool | None = None,
    rejects_table: str | None = None,
    rejects_limit: int | None = None,
    rejects_scan: str | None = None,
    union_by_name: bool | None = None,
    filename: bool | None = None,
    normalize_names: bool | None = None,
    ignore_errors: bool | None = None,
    allow_quoted_nulls: bool | None = None,
    auto_type_candidates: Sequence[str] | str | None = None,
    parallel: bool | None = None,
    skiprows: int | None = None,
    skip: int | None = None,
) -> Relation:
    """Load a CSV file into a :class:`Relation`."""

    connection = require_connection(duckcon, "read_csv")
    path = _normalise_path_argument(source)

    kwargs = _build_csv_options(
        header=header,
        delimiter=delimiter,
        delim=delim,
        quotechar=quotechar,
        quote=quote,
        escapechar=escapechar,
        escape=escape,
        sample_size=sample_size,
        auto_detect=auto_detect,
        columns=columns,
        dtype=dtype,
        names=names,
        na_values=na_values,
        null_padding=null_padding,
        force_not_null=force_not_null,
        files_to_sniff=files_to_sniff,
        decimal=decimal,
        decimal_separator=decimal_separator,
        date_format=date_format,
        dateformat=dateformat,
        timestamp_format=timestamp_format,
        timestampformat=timestampformat,
        encoding=encoding,
        compression=compression,
        hive_types_autocast=hive_types_autocast,
        all_varchar=all_varchar,
        hive_partitioning=hive_partitioning,
        comment=comment,
        max_line_size=max_line_size,
        maximum_line_size=maximum_line_size,
        store_rejects=store_rejects,
        rejects_table=rejects_table,
        rejects_limit=rejects_limit,
        rejects_scan=rejects_scan,
        union_by_name=union_by_name,
        filename=filename,
        normalize_names=normalize_names,
        ignore_errors=ignore_errors,
        allow_quoted_nulls=allow_quoted_nulls,
        auto_type_candidates=auto_type_candidates,
        parallel=parallel,
        skiprows=skiprows,
        skip=skip,
    )

    relation = connection.read_csv(path, **kwargs)  # type: ignore[arg-type, misc]
    return Relation.from_relation(duckcon, relation)


def read_parquet(
    duckcon: DuckCon,
    source: Path | Sequence[Path],
    *,
    binary_as_string: bool | None = None,
    file_row_number: bool | None = None,
    filename: bool | None = None,
    hive_partitioning: bool | None = None,
    union_by_name: bool | None = None,
    compression: str | None = None,
    directory: bool = False,
    partition_id_column: str | None = None,
    partition_glob: str | Sequence[str] = "*.parquet",
) -> Relation:
    """Load a Parquet file into a :class:`Relation`."""

    connection = require_connection(duckcon, "read_parquet")
    path = _normalise_parquet_source(source, directory=directory, glob=partition_glob)

    include_filename = filename
    if partition_id_column is not None:
        include_filename = True

    kwargs = _build_parquet_options(
        binary_as_string=binary_as_string,
        file_row_number=file_row_number,
        filename=include_filename,
        hive_partitioning=hive_partitioning,
        union_by_name=union_by_name,
        compression=compression,
    )

    relation = connection.read_parquet(path, **kwargs)  # type: ignore[arg-type]

    if partition_id_column is not None:
        casefolded = {column.casefold() for column in relation.columns}
        if partition_id_column.casefold() in casefolded:
            msg = (
                "Partition identifier column '{partition}' collides with existing "
                "Parquet data column"
            ).format(partition=partition_id_column)
            raise ValueError(msg)

        identifier = quote_identifier(partition_id_column)
        stem_expression = (
            "regexp_replace("
            "regexp_replace(filename, '^.*[\\\\/]', ''), "
            "'(?i)\\.parquet$', ''"
            ")"
        )
        relation = relation.project(f"*, {stem_expression} AS {identifier}")

    return Relation.from_relation(duckcon, relation)


def read_json(
    duckcon: DuckCon,
    source: Path | Sequence[Path],
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

    connection = require_connection(duckcon, "read_json")
    path = _normalise_path_argument(source)

    kwargs = _build_json_options(
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

    relation = connection.read_json(path, **kwargs)  # type: ignore[arg-type]
    return Relation.from_relation(duckcon, relation)
