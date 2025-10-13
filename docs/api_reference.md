# Duck+ API Reference

Duck+ exposes a small, opinionated surface area that wraps DuckDB with typed,
immutable relational helpers and explicit mutation primitives. This reference
summarizes the public API exported from `duckplus.__all__` and explains how the
pieces fit together.

## Quick navigation

- [Connection management](#connection-management-duckplusconnect)
  - [`DuckConnection`](#duckconnection)
  - [`connect()`](#connect)
  - [`load_extensions()`](#load_extensions)
  - [`attach_nanodbc()`](#attach_nanodbc)
  - [`query_nanodbc()`](#query_nanodbc)
- [ODBC strategies](#odbc-strategies-duckplusodbc)
  - [SQL Server](#sqlserverstrategy)
  - [PostgreSQL](#postgresstrategy)
  - [MySQL](#mysqlstrategy)
  - [IBM i Access](#ibmiaccessstrategy)
  - [Excel](#excelstrategy)
  - [Access](#accessstrategy)
  - [DuckDB DSN](#duckdbdsnstrategy)
  - [Custom ODBC](#customodbcstrategy)
- [Relational transformations](#relational-transformations-duckpluscore)
  - [Join, partition, and ASOF helpers](#join-partition-and-asof-helpers)
  - [Filter expression helpers](#filter-expression-helpers)
  - [`DuckRel`](#duckrel)

## Connection management (`duckplus.connect`)

### `DuckConnection`

`DuckConnection` is a lightweight wrapper around a `duckdb.DuckDBPyConnection`
that implements :class:`contextlib.AbstractContextManager`. It accepts an
optional database path (``None`` defaults to in-memory connections), a
``read_only`` flag, and a mapping of DuckDB configuration parameters. Keys in
``config`` are sanitized through :func:`duckplus.util.ensure_identifier` before
being passed to DuckDB. Exiting the context automatically closes the underlying
connection, and the `raw` property exposes the wrapped
`duckdb.DuckDBPyConnection`.【F:src/duckplus/connect.py†L15-L61】

### `connect()`

`connect()` is a convenience constructor that forwards keyword arguments to
`DuckConnection` and returns the wrapper instance. It is the primary entry point
for establishing connections in user code.【F:src/duckplus/connect.py†L64-L85】

### `load_extensions()`

`load_extensions(conn, extensions)` takes a `DuckConnection` and a sequence of
extension names, normalizes each identifier, and executes DuckDB's
``LOAD`` command. Providing an empty sequence is a no-op.【F:src/duckplus/connect.py†L115-L124】

### `attach_nanodbc()`

`attach_nanodbc(conn, alias, connection_string, read_only=True,
load_extension=True)` validates the requested schema alias, optionally loads
the ``nanodbc`` extension, and issues an ``ATTACH`` statement that exposes the
remote database under the provided alias. Attachments default to read-only mode
but can request write access when the upstream source allows it.【F:src/duckplus/connect.py†L127-L170】

### `query_nanodbc()`

`query_nanodbc(conn, connection_string, query, load_extension=True)` executes an
upstream SQL query through DuckDB's ``odbc_query`` table function. The helper
validates the provided connection string and query text, optionally loads the
``nanodbc`` extension, and wraps the resulting relation in a `DuckRel` so the
results can flow directly into Duck+ pipelines.【F:src/duckplus/connect.py†L173-L206】

## ODBC strategies (`duckplus.odbc`)

Duck+ offers a small strategy framework for managing ODBC connection strings in
concert with :class:`duckplus.secrets.SecretManager`. Each strategy produces a
`SecretDefinition` via :meth:`definition` or persists credentials directly with
the manager through :meth:`register`. When the secret exists in the registry the
strategy can reconstruct the full ODBC connection string with
:meth:`connection_string`, allowing helpers such as ``attach_nanodbc`` to consume
the resolved value. Driver-oriented helpers share an internal base that anchors
the ``DRIVER`` fragment and key ordering so connections behave consistently
across ecosystems.【F:src/duckplus/odbc.py†L1-L493】

<details id="sqlserverstrategy">
<summary><code>SQLServerStrategy</code> — Microsoft SQL Server drivers</summary>

Models SQL Server ODBC definitions. Requires ``SERVER``, ``DATABASE``, ``UID``,
and ``PWD`` parameters and optionally accepts ``PORT``, ``APP``, or ``WSID``
overrides. Version 18 defaults ``ENCRYPT`` to ``yes`` and callers can opt into
or out of encryption or trusted certificates via constructor flags.【F:src/duckplus/odbc.py†L281-L319】

</details>

<details id="postgresstrategy">
<summary><code>PostgresStrategy</code> — PostgreSQL Unicode drivers</summary>

Targets PostgreSQL Unicode drivers with sensible defaults for ``SERVER``,
``DATABASE``, ``UID``, and ``PWD`` secrets. Optional parameters capture
``PORT``, ``SSLMODE``, ``APPLICATIONNAME``, or ``CLIENTENCODING`` while the
constructor can fix an ``SSLMODE`` default for consistent security posture.【F:src/duckplus/odbc.py†L322-L347】

</details>

<details id="mysqlstrategy">
<summary><code>MySQLStrategy</code> — MySQL Unicode or ANSI drivers</summary>

Composes driver strings for MySQL Unicode or ANSI drivers. Expects ``SERVER``,
``DATABASE``, ``UID``, and ``PWD`` secrets, allows ``PORT`` or bitwise ``OPTION``
flags, and can bake in ``SSLMODE`` or ``CHARSET`` defaults so every connection
string stays synchronized.【F:src/duckplus/odbc.py†L350-L383】

</details>

<details id="ibmiaccessstrategy">
<summary><code>IBMiAccessStrategy</code> — IBM i Access / AS400</summary>

Targets the IBM i Access driver used with AS/400 systems. Expects ``SYSTEM``,
``UID``, and ``PWD`` secrets while supporting optional catalog controls like
``DATABASE``, ``DBQ``, and ``LIBL``. Convenience arguments populate the library
list and naming convention without leaking the values into version control.【F:src/duckplus/odbc.py†L242-L278】

</details>

<details id="excelstrategy">
<summary><code>ExcelStrategy</code> — Windows Excel driver</summary>

Composes connection strings for the Windows Excel driver. Requires ``DBQ`` (the
workbook path) and can toggle ``READONLY``, ``HDR``, or ``IMEX`` flags via
secrets or constructor options.【F:src/duckplus/odbc.py†L386-L416】

</details>

<details id="accessstrategy">
<summary><code>AccessStrategy</code> — Microsoft Access driver</summary>

Mirrors the Microsoft Access ODBC driver. Requires ``DBQ`` and accepts ``PWD``
or ``READONLY`` overrides for secured databases.【F:src/duckplus/odbc.py†L419-L449】

</details>

<details id="duckdbdsnstrategy">
<summary><code>DuckDBDsnStrategy</code> — DuckDB ODBC DSN helper</summary>

Frontloads the DSN fragment for DuckDB's ODBC driver while letting callers store
``DATABASE`` or ``READONLY`` settings as secrets.【F:src/duckplus/odbc.py†L452-L471】

</details>

<details id="customodbcstrategy">
<summary><code>CustomODBCStrategy</code> — extend to other drivers</summary>

Serves as an escape hatch for drivers not covered by the built-ins, forwarding
required and optional key expectations directly to the base implementation while
allowing deterministic defaults for additional parameters.【F:src/duckplus/odbc.py†L474-L493】

</details>

## Relational transformations (`duckplus.core`)

`duckplus.core` re-exports the immutable relation wrapper and supporting helpers so callers can import everything needed for relational work from a single module while the implementations remain in focused files.【F:src/duckplus/core.py†L5-L47】

### Join, partition, and ASOF helpers

- :class:`ColumnPredicate` compares two columns with one of ``= != < <= > >=`` and validates the operator choice.【F:src/duckplus/_core_specs.py†L10-L23】
- :class:`ExpressionPredicate` wraps an arbitrary SQL predicate string and ensures it is provided as non-empty text.【F:src/duckplus/_core_specs.py†L27-L37】
- :class:`JoinSpec` bundles equality key pairs with optional predicates, normalising inputs and rejecting malformed or empty specifications.【F:src/duckplus/_core_specs.py†L43-L103】
- :class:`PartitionSpec` restricts join partitions to equality-only keys and offers `of_columns()`/`from_mapping()` constructors for symmetric or aliased definitions.【F:src/duckplus/_core_specs.py†L114-L151】
- :class:`AsofOrder` captures left/right ordering columns for ASOF joins, and :class:`AsofSpec` extends :class:`JoinSpec` with ordering direction and optional tolerance validation (``nearest`` requires a tolerance).【F:src/duckplus/_core_specs.py†L106-L184】
- :class:`JoinProjection` configures how joins handle name collisions, defaulting to erroring when duplicates would be produced unless suffixes are supplied.【F:src/duckplus/_core_specs.py†L187-L200】

### Filter expression helpers

- :class:`FilterExpression` renders validated SQL fragments and supports boolean composition, while :func:`FilterExpression.raw` lets callers opt into raw SQL snippets when needed.【F:src/duckplus/filters.py†L90-L136】
- :func:`column` / :func:`col` create column references that support comparison operators and guarantee identifier validation.【F:src/duckplus/filters.py†L23-L88】
- :func:`equals`, :func:`not_equals`, :func:`less_than`, :func:`less_than_or_equal`, :func:`greater_than`, and :func:`greater_than_or_equal` build structured comparisons from keyword arguments, combining conditions with ``AND`` automatically.【F:src/duckplus/filters.py†L138-L188】

### `DuckRel`

`DuckRel` wraps a `duckdb.DuckDBPyRelation`, enforcing immutability while eagerly tracking projected columns and types so lookups remain case-insensitive without losing the original casing.【F:src/duckplus/duckrel.py†L129-L198】

#### Column metadata

- `columns`, `columns_lower`, and `columns_lower_set` return the projected names in canonical or case-folded form for reliable resolution.【F:src/duckplus/duckrel.py†L170-L185】
- `column_types` mirrors DuckDB's type names for each projected column.【F:src/duckplus/duckrel.py†L187-L191】

#### Projection and column management

- `project_columns(*names, missing_ok=False)` keeps a subset of columns, with strict missing-column errors unless explicitly allowed.【F:src/duckplus/duckrel.py†L200-L217】
- `drop(*names, missing_ok=False)` removes columns while guarding against dropping the entire projection.【F:src/duckplus/duckrel.py†L219-L244】
- `project({alias: expression, ...})` compiles explicit projections and infers new types from the resulting relation.【F:src/duckplus/duckrel.py†L246-L264】
- `rename_columns(**mappings)` applies DuckDB's ``RENAME`` star modifier with duplicate-source/target checks.【F:src/duckplus/duckrel.py†L266-L301】
- `transform_columns(**expressions)` rewrites existing columns via ``REPLACE`` while templating ``{column}`` / ``{col}`` placeholders with the quoted identifier.【F:src/duckplus/duckrel.py†L302-L335】
- `add_columns(**expressions)` appends computed columns and rejects duplicate output names.【F:src/duckplus/duckrel.py†L337-L365】

#### Filtering and splitting

- `filter(expression, *parameters)` accepts SQL strings with ``?`` placeholders or structured :class:`FilterExpression` instances, coercing parameters and validating counts before delegating to DuckDB.【F:src/duckplus/duckrel.py†L367-L391】
- `split(expression, *parameters)` returns matching and remainder relations, using the same rendering pipeline and negating the predicate for the remainder side.【F:src/duckplus/duckrel.py†L393-L415】

#### Join families

Duck+ provides both natural joins (matching shared column names) and explicit joins driven by :class:`JoinSpec`.

- `natural_inner`, `natural_left`, `natural_right`, and `natural_full` join on shared or aliased columns, defaulting to `strict=True` so missing keys raise descriptive errors.【F:src/duckplus/duckrel.py†L417-L479】
- `natural_asof` builds an :class:`AsofSpec` from shared keys plus ordering metadata before executing the ASOF join.【F:src/duckplus/duckrel.py†L481-L503】
- `semi_join` and `anti_join` reuse the natural join resolution to include or exclude matching rows without projecting right-hand columns.【F:src/duckplus/duckrel.py†L715-L739】
- `left_inner`, `left_outer`, `left_right`, `inner_join`, and `outer_join` accept explicit :class:`JoinSpec` instances, optionally guided by :class:`JoinProjection` to suffix collisions.【F:src/duckplus/duckrel.py†L637-L701】
- `asof_join` consumes an explicit :class:`AsofSpec`, validating temporal types and tolerance requirements before delegating to DuckDB.【F:src/duckplus/duckrel.py†L702-L714】
- Partition-aware joins pair a :class:`PartitionSpec` with a :class:`JoinSpec`; helpers (`partitioned_inner`, `partitioned_left`, `partitioned_right`, `partitioned_full`) flow through `partitioned_join()` which enforces equality-only partition keys.【F:src/duckplus/duckrel.py†L567-L635】
- `inspect_partitions()` summarises per-partition row counts for two relations, marking partitions present on both sides to aid planning.【F:src/duckplus/duckrel.py†L504-L565】
- Join projections always emit left columns first, append right columns afterwards, and raise deterministic errors if duplicates would surface without opting into suffixes.【F:src/duckplus/duckrel.py†L1005-L1045】

#### Ordering, limits, casting, and materialisation

- `order_by(column='asc'|'desc', ...)` resolves column names case-insensitively, validates directions, and forwards to DuckDB's ordering API.【F:src/duckplus/duckrel.py†L741-L764】
- `limit(count)` enforces a non-negative integer before delegating to DuckDB's ``LIMIT`` operator.【F:src/duckplus/duckrel.py†L766-L777】
- `cast_columns()` and `try_cast_columns()` validate requested DuckDB types and rewrite the projection with `CAST`/`TRY_CAST` expressions.【F:src/duckplus/duckrel.py†L779-L884】
- `materialize(strategy=None, into=None)` executes a :class:`~duckplus.materialize.MaterializeStrategy`, wrapping any returned relation in a new `DuckRel` and raising if the strategy yields no artefact.【F:src/duckplus/duckrel.py†L800-L845】

The accompanying strategies live in :mod:`duckplus.materialize`:

- `ArrowMaterializeStrategy(retain_table=True)` materializes via Arrow tables, optionally retaining the in-memory table when registering on another connection.【F:src/duckplus/materialize.py†L40-L65】
- `ParquetMaterializeStrategy(path=None, cleanup=False, suffix='.parquet')` writes through Parquet files, managing temporary paths and optional cleanup before reading back into DuckDB when requested.【F:src/duckplus/materialize.py†L67-L115】
- `Materialized` bundles the resulting Arrow table, wrapped relation, and output path with `require_table()`/`require_relation()` guards.【F:src/duckplus/materialize.py†L118-L145】
## Table mutations (`duckplus.table`)

`DuckTable` represents a mutable DuckDB table anchored to a `DuckConnection`.
Table names are validated to support dotted identifiers and quoted segments
without silently changing casing.【F:src/duckplus/table.py†L1-L76】

- `append(rel, by_name=True)` inserts rows from a `DuckRel`. When ``by_name`` is
  true (the default) the relation is projected to match the table's column names;
  disabling it enforces identical column counts.【F:src/duckplus/table.py†L78-L111】
- `insert_antijoin(rel, keys=[...])` filters incoming rows by performing an
  anti join on the provided key columns and appends only unseen rows, returning
  the number of inserted records.【F:src/duckplus/table.py†L113-L154】
- `insert_by_continuous_id(rel, id_column, inclusive=False)` reads the current
  maximum ID from the table, filters the relation to values greater than (or
  greater-or-equal when ``inclusive``) that ID, and delegates to
  `insert_antijoin` for idempotent ingestion.【F:src/duckplus/table.py†L156-L194】

## File IO helpers (`duckplus.io`)

All IO helpers accept :class:`pathlib.Path` objects or any value that implements
``__fspath__``. Sequences of paths are validated to be non-empty and converted
into the formats DuckDB expects.【F:src/duckplus/io.py†L1-L161】【F:src/duckplus/io.py†L227-L275】

The reader helpers are available directly on :class:`~duckplus.connect.DuckConnection`
so callers can stay within the connection namespace. The original
``duckplus.io`` functions remain as thin wrappers around those methods for
backwards compatibility.【F:src/duckplus/connect.py†L51-L120】【F:src/duckplus/io.py†L680-L812】

### Readers

- `DuckConnection.read_parquet(paths, **options)` (also available as
  `duckplus.io.read_parquet(conn, paths, **options)`) wraps
  `DuckDBPyConnection.read_parquet` after validating option names/types against
  `ParquetReadOptions`, returning a `DuckRel`. Duck+ raises descriptive
  `RuntimeError` messages if DuckDB fails to read the files.【F:src/duckplus/connect.py†L85-L106】【F:src/duckplus/io.py†L680-L743】

  Supported keyword arguments:

  - `binary_as_string` (bool): Treat `BINARY` columns as strings.
  - `file_row_number` (bool): Emit a row-number column per file.
  - `filename` (bool): Include the originating file name.
  - `hive_partitioning` (bool): Enable Hive-style directory discovery.
  - `union_by_name` (bool): Align schemas by column name.
  - `can_have_nan` (bool): Permit `NaN` statistics.
  - `compression` (:class:`Literal`["auto", "none", "uncompressed", "snappy", "gzip", "zstd", "lz4", "brotli"]):
    Override the expected codec.
  - `parquet_version` (:class:`Literal`["PARQUET_1_0", "PARQUET_2_0"]): Force version handling.
  - `debug_use_openssl` (bool): Prefer OpenSSL for crypto primitives.
  - `explicit_cardinality` (int): Supply a planner hint with the expected row count.
- `DuckConnection.read_csv(paths, encoding='utf-8', header=True, **options)`
  mirrors `duckplus.io.read_csv` so callers can configure the same
  TypedDict-validated options while enforcing explicit encoding/header
  types before delegating to DuckDB.【F:src/duckplus/connect.py†L108-L131】【F:src/duckplus/io.py†L745-L787】

  Supported keyword arguments:

  - `delimiter` (str): Column delimiter (default `","`).
  - `quote` (str | None): Quote character; empty string disables quoting.
  - `escape` (str | None): Escape character for quoted fields.
  - `nullstr` (str | Sequence[str] | None): Values interpreted as `NULL`.
  - `sample_size` (int): Bytes scanned for auto-detection.
  - `auto_detect` (bool): Enable DuckDB's schema inference.
  - `ignore_errors` (bool): Skip malformed rows.
  - `dateformat` (str): Format string for `DATE` columns.
  - `timestampformat` (str): Format string for `TIMESTAMP` columns.
  - `decimal_separator` (:class:`Literal`[",", "."]): Decimal marker.
  - `columns` (Mapping[str, :data:`duckplus.util.DuckDBType`]): Explicit column types.
  - `all_varchar` (bool): Force every column to `VARCHAR`.
  - `parallel` (bool): Allow parallel parsing.
  - `allow_quoted_nulls` (bool): Interpret quoted `nullstr` values as `NULL`.
  - `null_padding` (bool): Enable null padding for fixed-width values.
  - `normalize_names` (bool): Request identifier normalisation.
  - `union_by_name` (bool): Align schemas by column name.
  - `filename` (bool): Include the originating file name.
  - `hive_partitioning` (bool): Enable Hive directory discovery.
  - `hive_types_autocast` (bool): Autocast Hive partition columns.
  - `hive_types` (Mapping[str, :data:`duckplus.util.DuckDBType`]): Override partition types.
  - `files_to_sniff` (int): Cap the number of sampled files.
  - `compression` (:class:`Literal`["auto", "none", "gzip", "zstd", "bz2", "lz4", "xz", "snappy"]): Expected compression.
  - `thousands` (str): Thousands separator.
- `DuckConnection.read_json(paths, **options)` provides JSON and NDJSON
  ingestion, forwarding strongly-typed options that mirror DuckDB's
  reader implementation.【F:src/duckplus/connect.py†L133-L143】【F:src/duckplus/io.py†L789-L812】

  Supported keyword arguments:

  - `columns` (Mapping[str, :data:`duckplus.util.DuckDBType`]): Explicit column types.
  - `sample_size` (int): Bytes sampled for schema inference.
  - `maximum_depth` (int): Maximum nesting depth.
  - `records` (:class:`Literal`["auto", "array", "records"]): Array/record interpretation.
  - `format` (:class:`Literal`["auto", "newline_delimited", "unstructured"]): JSON, NDJSON or log parsing.
  - `dateformat` (str): Format string for `DATE` coercion.
  - `timestampformat` (str): Format string for `TIMESTAMP` coercion.
  - `compression` (:class:`Literal`["auto", "none", "gzip", "zstd", "bz2", "lz4", "xz", "snappy"]): Expected compression.
  - `maximum_object_size` (int): Limit on parsed object size.
  - `ignore_errors` (bool): Skip malformed entries.
  - `convert_strings_to_integers` (bool): Coerce numeric-looking strings.
  - `field_appearance_threshold` (float): Minimum proportion before creating a struct field.
  - `map_inference_threshold` (int): Minimum object size for `MAP` inference.
  - `maximum_sample_files` (int): Files sampled during detection.
  - `filename` (bool): Include the originating file name.
  - `hive_partitioning` (bool): Enable Hive directory discovery.
  - `union_by_name` (bool): Align schemas by column name.
  - `hive_types` (Mapping[str, :data:`duckplus.util.DuckDBType`]): Override partition types.
  - `hive_types_autocast` (bool): Autocast Hive partition values.
  - `auto_detect` (bool): Enable DuckDB's schema inference.

### Writers

- `write_parquet(rel, path, compression='zstd', **options)` writes the relation
  using DuckDB's Parquet writer through a temporary file-and-rename sequence so
  partially written files are not left behind. Compression defaults to `zstd` and
  validated options mirror DuckDB's keyword arguments.【F:src/duckplus/io.py†L814-L873】

  Supported keyword arguments:

  - `row_group_size` (int): Rows per Parquet row group.
  - `row_group_size_bytes` (int): Maximum bytes per row group.
  - `partition_by` (Sequence[str]): Partition columns.
  - `write_partition_columns` (bool): Persist partition columns in the output.
  - `per_thread_output` (bool): Emit a file per writer thread.
- `write_csv(rel, path, encoding='utf-8', header=True, **options)` mirrors the
  same durability pattern and validates encoding/header before dispatching to
  DuckDB.【F:src/duckplus/io.py†L875-L914】

  Supported keyword arguments:

  - `delimiter` (str): Output delimiter (default `","`).
  - `quote` (str | None): Quote character; empty string disables quoting.
  - `escape` (str | None): Escape character.
  - `null_rep` (str | None): Representation for `NULL` values.
  - `date_format` (str | None): Format string for `DATE` values.
  - `timestamp_format` (str | None): Format string for `TIMESTAMP` values.
  - `quoting` (:class:`Literal`["all", "minimal", "nonnumeric", "none"]): DuckDB quoting policy.
  - `compression` (:class:`Literal`["auto", "none", "gzip", "zstd", "bz2", "lz4", "xz", "snappy"]): Output compression.
  - `per_thread_output` (bool): Emit a file per writer thread.
  - `partition_by` (Sequence[str]): Partition columns.
  - `write_partition_columns` (bool): Persist partition columns in the output.

### Append helpers

- `append_csv(table, path, encoding='utf-8', header=True, **options)` reads the
  file via :meth:`DuckConnection.read_csv` on the table's connection and
  delegates to `DuckTable.append`.
- `append_parquet(table, paths, **options)` uses
  :meth:`DuckConnection.read_parquet` for ingestion.
- `append_ndjson(table, path, **options)` forces the JSON format to
  ``newline_delimited`` before calling :meth:`DuckConnection.read_json` and
  appending the rows.【F:src/duckplus/io.py†L916-L954】

## Secrets management (`duckplus.secrets`)

Duck+ keeps secrets definitions connection-independent so pipelines can register
credentials before the DuckDB ``secrets`` extension loads.

- `SecretDefinition` captures the desired name, engine, and parameter mapping
  and exposes `normalized()` which sanitizes identifiers, returning a
  `SecretRecord`.【F:src/duckplus/secrets.py†L1-L54】
- `SecretRecord` stores the normalized form and can be converted back to a
  definition for display or editing.【F:src/duckplus/secrets.py†L56-L74】
- `SecretRegistry` is an in-memory catalog with `has_secret`, `list_secrets`,
  `get_secret`, `save`, and `drop_secret`. Attempting to save without `replace`
  when a name already exists raises, guaranteeing explicit overwrite semantics.【F:src/duckplus/secrets.py†L76-L136】
- `SecretManager(connection, registry=None, auto_load=True)` wraps a
  `DuckConnection`, optionally reusing a shared registry, and lazily attempts to
  load DuckDB's ``secrets`` extension. `create_secret()` always writes into the
  registry and mirrors the secret into DuckDB when the extension is available;
  `drop_secret()` removes it from both places; `sync()` lets callers copy cached
  secrets into DuckDB later. All identifier inputs are validated so they remain
  safe for interpolation into SQL statements.【F:src/duckplus/secrets.py†L138-L226】【F:src/duckplus/secrets.py†L228-L264】

## HTML preview (`duckplus.html`)

`to_html(rel, max_rows=100, null_display='', **style)` renders a lightweight HTML
preview of a relation. Column headers preserve original casing, cell values are
escaped inside DuckDB, and truncated datasets add a `<tfoot>` summary. Optional
``class``/``id`` attributes can be supplied via keyword arguments (`class_` to
avoid clashing with Python keywords).【F:src/duckplus/html.py†L1-L94】

## Command line interface (`duckplus.cli`)

`cli_main` (exported as `duckplus.cli_main`) wraps :func:`duckplus.cli.main`.
The CLI offers a read-only SQL runner, schema inspection, and an optional REPL.
Connections are opened in read-only mode when a database path is provided; errors
from DuckDB or the filesystem are surfaced as user-friendly messages.【F:src/duckplus/cli.py†L1-L120】

## Public namespace

Importing from `duckplus` provides all of the classes and helpers documented
above through the module's ``__all__`` definition, making `from duckplus import
DuckRel, DuckTable, connect` the canonical entry point for most applications.【F:src/duckplus/__init__.py†L64-L94】

## Demo walkthroughs

The following lightweight walkthroughs combine the primitives above so teams can
visualize how the API reference maps onto everyday tasks without relying on any
additional helpers.

### Demo: Build a transformation pipeline

This example chains a handful of `DuckRel` helpers to prepare a transformed view
before materializing it for downstream use.

```python
from pathlib import Path

from duckplus import connect

with connect() as conn:
    # Load two datasets from disk into immutable DuckRel wrappers.
    staging = conn.read_parquet([Path("/data/staging_orders.parquet")])
    reference = conn.read_csv([Path("/data/customer_lookup.csv")])

    enriched = (
        staging
        # Cast total to a DECIMAL column for downstream precision.
        .cast_columns(total="DECIMAL(18,2)")
        # Join on shared customer_id while tolerating extra right-side columns.
        .natural_left(reference, allow_collisions=True)
        # Filter to shipped orders in the current quarter.
        .filter("status = ? AND ship_date >= ?", "SHIPPED", "2024-01-01")
        .order_by(order_id="asc")
        .limit(1000)
    )

    # Spill the relation to an Arrow table for analytics clients.
    arrow_snapshot = enriched.materialize().require_table()
```

- `DuckConnection.read_parquet` and `DuckConnection.read_csv` validate paths
  and wrap the resulting relations in `DuckRel` for further
  composition.【F:src/duckplus/connect.py†L85-L143】【F:src/duckplus/io.py†L680-L812】
- `cast_columns`, `natural_left`, `filter`, `order_by`, and `limit` each return a
  new `DuckRel`, ensuring the pipeline stays immutable and case-aware.【F:src/duckplus/core.py†L533-L807】
- `materialize()` defaults to the Arrow strategy and ensures the resulting table
  can be reused without mutating the original relation.【F:src/duckplus/core.py†L844-L904】【F:src/duckplus/materialize.py†L21-L55】

### Demo: Append only unseen rows into a fact table

Mutable table helpers complement the immutable pipeline by enforcing explicit
ingestion semantics.

```python
from duckplus import DuckTable, connect

with connect("warehouse.duckdb") as conn:
    fact_orders = DuckTable(conn, "analytics.fact_orders")
    staging = conn.read_parquet([Path("/loads/fact_orders_delta.parquet")])

    inserted = fact_orders.insert_antijoin(staging, keys=["order_id"])
    print(f"Inserted {inserted} new rows")
```

- `DuckTable` validates the dotted identifier without changing casing, keeping
  schema ownership explicit.【F:src/duckplus/table.py†L1-L76】
- `insert_antijoin` performs a case-aware anti join using the provided keys and
  returns the number of appended rows for observability.【F:src/duckplus/table.py†L113-L194】
- `DuckConnection.read_parquet` mirrors the same path validation behaviour shown
  earlier, so ingestion always flows through typed helpers.【F:src/duckplus/connect.py†L85-L106】【F:src/duckplus/io.py†L680-L787】

### Demo: Provision and sync connection secrets

Secrets management is designed to be connection-aware without relying on global
state.

```python
from duckplus import SecretDefinition, SecretManager, connect

with connect() as conn:
    manager = SecretManager(conn)
    definition = SecretDefinition(
        name="GCS_BACKUP",
        engine="gcs",
        parameters={"project_id": "analytics-prod", "key_file": "/secrets/key.json"},
    )

    manager.create_secret(definition, replace=True)
    manager.sync()  # Mirrors cached secrets into the DuckDB connection.
```

- `SecretDefinition.normalized()` guarantees safe identifier casing before the
  secret ever reaches DuckDB.【F:src/duckplus/secrets.py†L1-L74】
- `SecretManager` coordinates registry storage with optional extension loading
  and exposes `create_secret`/`sync` helpers for deterministic mirroring.【F:src/duckplus/secrets.py†L138-L264】
