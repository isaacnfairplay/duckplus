# ``duckplus.relation``

:class:`Relation` is an immutable wrapper around ``duckdb.DuckDBPyRelation``.
Instances retain their originating :class:`duckplus.duckcon.DuckCon`, cache
column metadata, and surface ergonomic helpers for exporting, transforming, and
joining data. Methods never mutate the underlying relation; each returns a new
``Relation`` bound to the same connection.

## Metadata and exporters

- ``columns`` – tuple of column names returned by DuckDB.
- ``types`` – tuple of DuckDB type strings aligned with ``columns``.
- ``relation`` – the underlying ``DuckDBPyRelation`` for advanced scenarios.
- ``row_count()`` – executes ``COUNT(*)`` to report the number of rows produced.
- ``null_ratios()`` – computes the ratio of ``NULL`` values per column.
- ``sample_pandas(limit=50)`` – return a ``pandas.DataFrame`` sample.
- ``iter_pandas_batches(batch_size, *, limit=None)`` – yield Pandas batches using
  DuckDB's batch reader.
- ``sample_arrow(limit=50)`` – fetch a ``pyarrow.Table`` sample.
- ``iter_arrow_batches(batch_size, *, limit=None)`` – yield Arrow tables in
  batches.
- ``sample_polars(limit=50)`` – materialise a ``polars.DataFrame``.
- ``iter_polars_batches(batch_size, *, limit=None)`` – stream Polars batches
  lazily.

All exporters validate that the associated ``DuckCon`` is open and raise clear
errors when optional dependencies (``pandas``, ``pyarrow``, ``polars``) are
missing.

## Construction helpers

- ``Relation.from_relation(duckcon, relation)`` – wrap an existing
  ``DuckDBPyRelation``.
- ``Relation.from_sql(duckcon, query)`` – execute SQL on the managed connection
  and wrap the result.
- ``Relation.from_odbc_query(duckcon, connection_string, query, *, parameters=None)`` –
  issue an ``odbc_query`` via the nano-ODBC extension.
- ``Relation.from_odbc_table(duckcon, connection_string, table)`` – scan an ODBC
  table using nano-ODBC.
- ``Relation.from_excel(duckcon, source, **options)`` – read from an Excel file
  after loading DuckDB's ``excel`` extension. Supports DuckDB's ``read_excel``
  options (sheet selection, header handling, dtype coercion, etc.).

Each constructor requires an open ``DuckCon``. ODBC helpers surface actionable
errors when the nano-ODBC extension is not available.

## Column transformations

- ``transform(**replacements)`` – apply ``SELECT * REPLACE`` expressions. Values
  may be SQL snippets or Python types (``int``, ``float``, ``str``, ``bool``,
  ``bytes``) that DuckPlus casts automatically. Missing columns raise
  ``KeyError``.
- ``add(**expressions)`` – add new columns using typed expressions from
  :mod:`duckplus.typed`. Column dependencies are validated before executing the
  projection.

## Joins

- ``join(other, *, on=None)`` – inner join with column conflict detection.
- ``left_join(other, *, on=None)`` – left join variant.
- ``right_join(other, *, on=None)`` – right join variant.
- ``outer_join(other, *, on=None)`` – full outer join.
- ``semi_join(other, *, on=None)`` – semi join that preserves rows from the left
  relation when matches exist.
- ``asof_join(other, *, on=None, tolerance=None, direction="backward")`` –
  time-aware join that aligns rows by the nearest key subject to tolerance and
  direction rules.

Join helpers accept either a mapping of left/right column names or an iterable of
column pairs. A helpful ``ValueError`` is raised when columns are missing or when
case-insensitive conflicts occur.

## Aggregations and filtering

- ``aggregate(group_by=None, *filters, **aggregations)`` – compute aggregate
  columns with optional group-by keys and post-aggregation filters. Aggregation
  expressions must come from :mod:`duckplus.typed`.
- ``filter(*conditions)`` – filter rows using SQL snippets or typed boolean
  expressions. Multiple conditions are ``AND``-combined.

## File outputs

- ``append_csv(target, *, unique_id_column=None, match_all_columns=False,
  mutate=True, header=True, delimiter=",", quotechar='"', encoding="utf-8")`` –
  append or create CSV files. ``unique_id_column`` or ``match_all_columns``
  triggers deduplication before writing, while ``mutate=False`` performs a dry
  run and returns the deduplicated relation without touching the filesystem.
- ``append_parquet(target, *, unique_id_column=None, match_all_columns=False,
  mutate=False, temp_directory=None, compression=None)`` – append to Parquet
  files. When ``mutate=False`` the method returns the deduplicated relation; when
  ``mutate=True`` it writes through a temporary file (optionally placed in
  ``temp_directory``) before replacing the destination. ``compression`` forwards
  to DuckDB's writer.
- ``write_parquet_dataset(directory, *, partition_column, filename_template="{partition}.parquet",
  partition_actions=None, default_action="overwrite", immutable=False)`` – write
  a partitioned Parquet dataset. ``partition_actions`` overrides the default
  behaviour per partition and ``immutable=True`` enforces append-only semantics.

All writers require an open connection and reuse DuckDB's ``write_parquet`` or
union helpers under the hood.

## Column selection

- ``rename(**renames)`` – rename columns, raising ``ValueError`` on missing keys
  or conflicts.
- ``rename_if_exists(**renames)`` – rename only the columns that exist; others are
  ignored.
- ``keep(*columns)`` – project the listed columns. Missing columns raise
  ``KeyError``.
- ``keep_if_exists(*columns)`` – keep the subset of provided columns that exist
  (returns ``self`` when none remain).
- ``drop(*columns)`` – drop the specified columns, raising ``KeyError`` when a
  column is absent.
- ``drop_if_exists(*columns)`` – drop available columns while skipping the rest.

These helpers use case-insensitive matching but preserve original casing in the
returned relation.
