# Duck+ API Reference

Duck+ exposes a small, opinionated surface area that wraps DuckDB with typed,
immutable relational helpers and explicit mutation primitives. This reference
summarizes the public API exported from `duckplus.__all__` and explains how the
pieces fit together.

- Connections live in :mod:`duckplus.connect` and surface context-managed
  access to a raw `duckdb.DuckDBPyConnection`.
- Immutable relational work happens through :class:`duckplus.core.DuckRel` and
  its supporting join specifications.
- Mutable table helpers sit behind :class:`duckplus.table.DuckTable`.
- File readers/writers live in :mod:`duckplus.io` and consistently favour
  :class:`pathlib.Path` objects or other :func:`os.fspath`-compatible inputs.
- Materialization strategies in :mod:`duckplus.materialize` define how
  relations spill to Arrow, DuckDB, or disk.
- :mod:`duckplus.secrets` provides a registry plus a connection-aware manager
  that mirrors definitions into DuckDB when the optional ``secrets`` extension
  is present.
- Presentation extras live in :mod:`duckplus.html` and the CLI entrypoint in
  :mod:`duckplus.cli`.

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
:meth:`connection_string`, allowing helpers such as
``attach_nanodbc`` to consume the resolved value. Driver-oriented helpers share
an internal base that anchors the ``DRIVER`` fragment and key ordering so
connections behave consistently across ecosystems.【F:src/duckplus/odbc.py†L1-L493】

- :class:`SQLServerStrategy(secret_name, version=17|18, encrypt=None,
  trust_server_certificate=None, options=None)` models the Microsoft SQL Server
  drivers. It requires ``SERVER``, ``DATABASE``, ``UID``, and ``PWD`` parameters
  and accepts optional ``PORT``, ``APP``, or ``WSID`` overrides. Version 18
  defaults ``ENCRYPT`` to ``yes`` and callers can opt into or out of encryption
  or trusted certificates via the constructor flags.【F:src/duckplus/odbc.py†L281-L319】
- :class:`PostgresStrategy(secret_name, driver=None, sslmode=None,
  options=None)` targets the PostgreSQL Unicode drivers with sensible defaults
  for ``SERVER``, ``DATABASE``, ``UID``, and ``PWD`` secrets. Optional parameters
  capture ``PORT``, ``SSLMODE``, ``APPLICATIONNAME``, or ``CLIENTENCODING`` while
  the constructor can fix an ``SSLMODE`` default for consistent security
  posture.【F:src/duckplus/odbc.py†L322-L347】
- :class:`MySQLStrategy(secret_name, version='8.0', ansi=False,
  ssl_mode=None, charset=None, options=None)` composes driver strings for MySQL
  Unicode or ANSI drivers. It expects ``SERVER``, ``DATABASE``, ``UID``, and
  ``PWD`` secrets, allows ``PORT`` or bitwise ``OPTION`` flags, and can bake in
  ``SSLMODE`` or ``CHARSET`` defaults so every connection string stays
  synchronized.【F:src/duckplus/odbc.py†L350-L383】
- :class:`IBMiAccessStrategy(secret_name, library_list=None, naming=None,
  options=None)` targets the IBM i Access driver used with AS/400 systems. It
  expects ``SYSTEM``, ``UID``, and ``PWD`` secrets while supporting optional
  catalog controls like ``DATABASE``, ``DBQ``, and ``LIBL``. Convenience
  arguments populate the library list and naming convention without leaking the
  values into version control.【F:src/duckplus/odbc.py†L242-L278】
- :class:`ExcelStrategy(secret_name, read_only=None, options=None)` composes
  connection strings for the Windows Excel driver. The strategy requires ``DBQ``
  (the workbook path) and can toggle ``READONLY``, ``HDR``, or ``IMEX`` flags via
  secrets or constructor options.【F:src/duckplus/odbc.py†L386-L416】
- :class:`AccessStrategy(secret_name, read_only=None, options=None)` mirrors the
  Microsoft Access ODBC driver, requiring ``DBQ`` and accepting ``PWD`` or
  ``READONLY`` overrides for secured databases.【F:src/duckplus/odbc.py†L419-L449】
- :class:`DuckDBDsnStrategy(secret_name, dsn='DuckDB', options=None)` frontloads
  the DSN fragment for DuckDB's ODBC driver while letting callers store
  ``DATABASE`` or ``READONLY`` settings as secrets.【F:src/duckplus/odbc.py†L452-L471】
- :class:`CustomODBCStrategy(secret_name, driver, required_keys=(),
  optional_keys=(), default_options=None, options=None)` serves as an escape
  hatch for drivers not covered by the built-ins, forwarding required and
  optional key expectations directly to the base implementation while allowing
  deterministic defaults for additional parameters.【F:src/duckplus/odbc.py†L474-L493】

## Relational transformations (`duckplus.core`)

### Join and partition helpers

- :class:`ColumnPredicate` compares two columns with one of ``= != < <= > >=``
  and validates the operator.【F:src/duckplus/core.py†L63-L83】
- :class:`ExpressionPredicate` wraps an arbitrary SQL predicate string and
  ensures the expression is non-empty.【F:src/duckplus/core.py†L86-L111】
- :class:`JoinSpec` bundles equality key pairs with optional join predicates.
  Input sequences are validated for type, arity, and emptiness so callers cannot
  produce malformed joins.【F:src/duckplus/core.py†L114-L190】
- :class:`PartitionSpec` restricts to equality-only keys and offers
  `of_columns()` and `from_mapping()` constructors for symmetric and aliased
  partitioning definitions.【F:src/duckplus/core.py†L193-L238】
- :class:`AsofOrder` captures the left/right ordering columns for ASOF joins, and
  :class:`AsofSpec` extends `JoinSpec` with ordering, direction (backward,
  forward, nearest), and optional tolerance requirements (``nearest`` mandates a
  tolerance).【F:src/duckplus/core.py†L201-L237】
- :class:`JoinProjection` configures how joins handle name collisions. By
  default Duck+ forbids duplicate output names; enabling collisions optionally
  applies suffixes (``("_1", "_2")`` by default).【F:src/duckplus/core.py†L240-L270】

### `DuckRel`

`DuckRel` wraps a `duckdb.DuckDBPyRelation` and enforces immutability. Metadata
about projected columns and types is tracked eagerly so column resolution is
case-insensitive while preserving original casing. Attempting to reassign
internal attributes raises, preserving referential transparency.【F:src/duckplus/core.py†L344-L418】

Key capabilities include:

#### Column metadata

- `columns`, `columns_lower`, and `columns_lower_set` provide the projected
  names in their original or lowercased forms, enabling case-insensitive lookups
  while keeping the canonical casing intact.【F:src/duckplus/core.py†L384-L409】
- `column_types` mirrors DuckDB's type names for each projected column.【F:src/duckplus/core.py†L411-L415】

#### Projection and filtering

- `project_columns(*names, missing_ok=False)` keeps a subset of columns. Missing
  names raise unless ``missing_ok`` is `True`, in which case unresolved names are
  ignored and the original relation is returned when nothing resolves.【F:src/duckplus/core.py†L417-L447】
- `project({alias: expression, ...})` compiles explicit projection expressions
  and returns a new `DuckRel` with inferred types from the resulting relation.
  Expressions must be strings to avoid implicit SQL serialization.【F:src/duckplus/core.py†L449-L471】
- `filter(expression, *parameters)` accepts a SQL predicate with positional ``?``
  placeholders. Parameters are coerced into SQL literals, and argument counts are
  validated against the number of placeholders.【F:src/duckplus/core.py†L473-L531】

#### Ordering, limits, and casting

- `order_by(column='asc'|'desc', ...)` resolves column names case-insensitively
  and validates directions before delegating to DuckDB's ordering API.【F:src/duckplus/core.py†L720-L776】
- `limit(count)` enforces a non-negative integer and forwards to DuckDB's
  ``LIMIT`` operator.【F:src/duckplus/core.py†L778-L807】
- `cast_columns()` and `try_cast_columns()` accept mappings of column names to
  DuckDB type names, applying `CAST` or `TRY_CAST` while validating requested
  types against Duck+'s supported set.【F:src/duckplus/core.py†L809-L857】

#### Join families

Duck+ provides both natural joins (matching shared column names) and explicit
joins via `JoinSpec`.

- `natural_inner`, `natural_left`, `natural_right`, and `natural_full` join on
  shared columns, optionally accepting keyword aliases when the right-hand name
  differs. By default they enforce `strict=True`, raising if neither shared nor
  aliased columns exist.【F:src/duckplus/core.py†L533-L690】【F:src/duckplus/core.py†L1009-L1057】
- `semi_join` and `anti_join` reuse the natural join resolution to keep or drop
  matches from the left relation, preserving explicit projection semantics.【F:src/duckplus/core.py†L698-L742】
- `left_inner`, `left_outer`, `left_right`, `inner_join`, and `outer_join` accept
  `JoinSpec` instances to drive explicit equality and predicate joins; the
  optional `JoinProjection` controls collision handling.【F:src/duckplus/core.py†L640-L717】
- `asof_join` consumes an `AsofSpec`, while `natural_asof` builds the spec from
  shared keys plus ordering information. Both validate temporal column types and
  support backward/forward/nearest semantics with optional tolerances.【F:src/duckplus/core.py†L547-L628】【F:src/duckplus/core.py†L572-L619】
- Partition-aware joins compose a `PartitionSpec` with a `JoinSpec`. Helpers for
  each join type (`partitioned_inner`, `partitioned_left`, `partitioned_right`,
  `partitioned_full`) all delegate through `partitioned_join()` which enforces
  equality-only partition keys.【F:src/duckplus/core.py†L592-L639】
- `inspect_partitions()` summarizes per-partition row counts for two relations
  and flags partitions present in both sides, aiding join planning prior to
  committing to partitioned execution.【F:src/duckplus/core.py†L619-L688】

Joins always project left columns first and drop duplicate right-side keys
unless collision handling is explicitly enabled. Attempts to emit duplicate
output names raise deterministic errors, mirroring Duck+'s strict defaults for
column casing and projection.【F:src/duckplus/core.py†L1059-L1097】

#### Materialization

`materialize(strategy=None, into=None)` runs the relation through a
`MaterializeStrategy`. The default `ArrowMaterializeStrategy` returns an in-memory
Arrow table. Providing ``into`` expects the strategy to yield a new relation on
that connection; strategies that produce neither a table nor a path raise to
prevent silent no-ops. The helper wraps the resulting relation back into a
`DuckRel` so downstream code can continue chaining transformations.【F:src/duckplus/core.py†L844-L904】

The accompanying strategies live in :mod:`duckplus.materialize`:

- `ArrowMaterializeStrategy(retain_table=True)` converts the relation to an
  Arrow table and optionally registers it on another connection.【F:src/duckplus/materialize.py†L21-L55】
- `ParquetMaterializeStrategy(path=None, cleanup=False, suffix='.parquet')`
  writes to a Parquet file (creating a temporary file when no path is supplied),
  optionally cleans up temporary artefacts, and can import the file into another
  connection.【F:src/duckplus/materialize.py†L58-L111】
- `Materialized` bundles the resulting Arrow table, wrapped relation, and output
  path, with `require_table()` / `require_relation()` helpers that raise when the
  requested artefact is unavailable.【F:src/duckplus/materialize.py†L114-L150】

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
- `DuckConnection.read_csv(paths, encoding='utf-8', header=True, **options)`
  mirrors `duckplus.io.read_csv` so callers can configure the same
  TypedDict-validated options while enforcing explicit encoding/header
  types before delegating to DuckDB.【F:src/duckplus/connect.py†L108-L131】【F:src/duckplus/io.py†L745-L787】
- `DuckConnection.read_json(paths, **options)` provides JSON and NDJSON
  ingestion, forwarding strongly-typed options that mirror DuckDB's
  reader implementation.【F:src/duckplus/connect.py†L133-L143】【F:src/duckplus/io.py†L789-L812】

### Writers

- `write_parquet(rel, path, compression='zstd', **options)` writes the relation
  using DuckDB's Parquet writer through a temporary file-and-rename sequence so
  partially written files are not left behind. Compression defaults to `zstd` and
  validated options mirror DuckDB's keyword arguments.【F:src/duckplus/io.py†L814-L873】
- `write_csv(rel, path, encoding='utf-8', header=True, **options)` mirrors the
  same durability pattern and validates encoding/header before dispatching to
  DuckDB.【F:src/duckplus/io.py†L875-L914】

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
