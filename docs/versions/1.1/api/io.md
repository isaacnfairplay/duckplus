# ``duckplus.io``

The :mod:`duckplus.io` module wraps DuckDB's file readers so callers receive
:class:`duckplus.relation.Relation` instances tied to their originating
connection. Helpers share consistent parameter names, validate conflicting
aliases, and expose keyword-argument TypedDicts for editor support.

## Shared utilities

- ``PathLikeInput`` – type alias accepting ``Path`` objects, ``os.PathLike``
  instances, or strings.
- ``CSVReadKeywordOptions`` / ``ParquetReadKeywordOptions`` /
  ``JSONReadKeywordOptions`` – ``TypedDict`` definitions describing supported
  keyword arguments. DuckPlus normalises aliases (for example ``delimiter`` and
  ``delim``) and sequences.

## Readers

- ``read_csv(duckcon, source, **options)`` – call DuckDB's ``read_csv`` with a
  normalised path or list of paths. Supports DuckDB's sampling, schema, and error
  handling options. Raises ``ValueError`` when mutually exclusive aliases are
  provided or when path sequences mix types.
- ``read_parquet(duckcon, source, *, binary_as_string=None, file_row_number=None,
  filename=None, hive_partitioning=None, union_by_name=None, compression=None,
  directory=False, partition_id_column=None, partition_glob="*.parquet")`` – read
  Parquet files or directories. Directory mode expands glob patterns and ensures
  matches exist. When ``partition_id_column`` is provided DuckPlus derives the
  filename stem into a new column, guarding against collisions.
- ``read_json(duckcon, source, **options)`` – call DuckDB's ``read_json`` with
  normalised keyword arguments. All DuckDB options are passed through while
  preserving dictionaries and sequences.

Each reader validates that the associated :class:`duckplus.duckcon.DuckCon` is
open before delegating to DuckDB and wraps the resulting relation via
:class:`Relation.from_relation`.
