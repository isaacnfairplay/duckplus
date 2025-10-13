.. _duckplus_api_reference:

Duck+ API Reference
===================

Duck+ exposes a small, opinionated surface area that wraps DuckDB with typed,
immutable relational helpers and explicit mutation primitives. This reference
summarizes the public API exported from ``duckplus.__all__`` and explains how the
pieces fit together.

.. contents:: Quick navigation
   :local:
   :depth: 2

.. _connection-management-duckplusconnect:

Connection management (``duckplus.connect``)
--------------------------------------------

.. _duckconnection:

DuckConnection
^^^^^^^^^^^^^^

``DuckConnection`` is a lightweight wrapper around a
``duckdb.DuckDBPyConnection`` that implements :class:`contextlib.AbstractContextManager`.
It accepts an optional database path (``None`` defaults to in-memory
connections), a ``read_only`` flag, and a mapping of DuckDB configuration
parameters. Keys in ``config`` are sanitized through
:func:`duckplus.util.ensure_identifier` before being passed to DuckDB. Exiting
the context automatically closes the underlying connection, and the ``raw``
property exposes the wrapped ``duckdb.DuckDBPyConnection``.【F:src/duckplus/connect.py†L15-L61】

.. _connect:

``connect()``
^^^^^^^^^^^^^

``connect()`` is a convenience constructor that forwards keyword arguments to
``DuckConnection`` and returns the wrapper instance. It is the primary entry
point for establishing connections in user code.【F:src/duckplus/connect.py†L64-L85】

.. _load_extensions:

``load_extensions()``
^^^^^^^^^^^^^^^^^^^^^

``load_extensions(conn, extensions)`` takes a ``DuckConnection`` and a sequence
of extension names, normalizes each identifier, and executes DuckDB's ``LOAD``
command. Providing an empty sequence is a no-op.【F:src/duckplus/connect.py†L115-L124】

.. _attach_nanodbc:

``attach_nanodbc()``
^^^^^^^^^^^^^^^^^^^^

``attach_nanodbc(conn, alias, connection_string, read_only=True,
load_extension=True)`` validates the requested schema alias, optionally loads
the ``nanodbc`` extension, and issues an ``ATTACH`` statement that exposes the
remote database under the provided alias. Attachments default to read-only mode
but can request write access when the upstream source allows it.【F:src/duckplus/connect.py†L127-L170】

.. _query_nanodbc:

``query_nanodbc()``
^^^^^^^^^^^^^^^^^^^

``query_nanodbc(conn, connection_string, query, load_extension=True)`` executes
an upstream SQL query through DuckDB's ``odbc_query`` table function. The helper
validates the provided connection string and query text, optionally loads the
``nanodbc`` extension, and wraps the resulting relation in a ``DuckRel`` so the
results can flow directly into Duck+ pipelines.【F:src/duckplus/connect.py†L173-L206】

.. _odbc-strategies-duckplusodbc:

ODBC strategies (``duckplus.odbc``)
-----------------------------------

Duck+ offers a small strategy framework for managing ODBC connection strings in
concert with :class:`duckplus.secrets.SecretManager`. Each strategy produces a
``SecretDefinition`` via :meth:`definition` or persists credentials directly with
the manager through :meth:`register`. When the secret exists in the registry the
strategy can reconstruct the full ODBC connection string with
:meth:`connection_string`, allowing helpers such as ``attach_nanodbc`` to consume
the resolved value. Driver-oriented helpers share an internal base that anchors
the ``DRIVER`` fragment and key ordering so connections behave consistently
across ecosystems.【F:src/duckplus/odbc.py†L1-L493】

.. admonition:: ``SQLServerStrategy`` — Microsoft SQL Server drivers

   Models SQL Server ODBC definitions. Requires ``SERVER``, ``DATABASE``,
   ``UID``, and ``PWD`` parameters and optionally accepts ``PORT``, ``APP``, or
   ``WSID`` overrides. Version 18 defaults ``ENCRYPT`` to ``yes`` and callers can
   opt into or out of encryption or trusted certificates via constructor
   flags.【F:src/duckplus/odbc.py†L281-L319】

.. admonition:: ``PostgresStrategy`` — PostgreSQL Unicode drivers

   Targets PostgreSQL Unicode drivers with sensible defaults for ``SERVER``,
   ``DATABASE``, ``UID``, and ``PWD`` secrets. Optional parameters capture
   ``PORT``, ``SSLMODE``, ``APPLICATIONNAME``, or ``CLIENTENCODING`` while the
   constructor can fix an ``SSLMODE`` default for consistent security
   posture.【F:src/duckplus/odbc.py†L322-L347】

.. admonition:: ``MySQLStrategy`` — MySQL Unicode or ANSI drivers

   Composes driver strings for MySQL Unicode or ANSI drivers. Expects ``SERVER``,
   ``DATABASE``, ``UID``, and ``PWD`` secrets, allows ``PORT`` or bitwise
   ``OPTION`` flags, and can bake in ``SSLMODE`` or ``CHARSET`` defaults so every
   connection string stays synchronized.【F:src/duckplus/odbc.py†L350-L383】

.. admonition:: ``IBMiAccessStrategy`` — IBM i Access / AS400

   Targets the IBM i Access driver used with AS/400 systems. Expects ``SYSTEM``,
   ``UID``, and ``PWD`` secrets while supporting optional catalog controls like
   ``DATABASE``, ``DBQ``, and ``LIBL``. Convenience arguments populate the
   library list and naming convention without leaking the values into version
   control.【F:src/duckplus/odbc.py†L242-L278】

.. admonition:: ``ExcelStrategy`` — Microsoft Excel files

   Wraps Microsoft's Excel driver with optional ``MAXSCANROWS`` and ``HDR``
   tuning. Secret definitions track workbook paths and optional password
   parameters, keeping automation consistent across scheduled jobs.【F:src/duckplus/odbc.py†L198-L239】

.. admonition:: ``AccessStrategy`` — Microsoft Access databases

   Provides safe defaults for Access connection strings, including support for
   password-protected ``.mdb`` or ``.accdb`` files. Helper flags toggle 64-bit
   driver usage and optional ``PWD`` parameters.【F:src/duckplus/odbc.py†L383-L420】

.. admonition:: ``DuckDBDSNStrategy`` — DuckDB system DSN entries

   Coordinates DuckDB DSN usage for environments that expose central ODBC
   registrations. Pulls connection details from the registry and assembles a
   DSN-aware connection string for ``attach_nanodbc`` integration.【F:src/duckplus/odbc.py†L420-L457】

.. admonition:: ``CustomODBCStrategy`` — Arbitrary driver definitions

   Allows callers to register arbitrary driver fragments while still leveraging
   ``SecretManager`` storage. Validates identifier casing and preserves the
   declared key ordering when reconstructing the final connection string.【F:src/duckplus/odbc.py†L457-L493】

.. _relational-transformations-duckpluscore:

Relational transformations (``duckplus.core``)
----------------------------------------------

DuckRel and its helpers model immutable relational pipelines. Each helper returns
new ``DuckRel`` instances that defer execution until explicitly materialized,
allowing complex flows to remain composable and type-aware.【F:src/duckplus/core.py†L533-L807】

.. admonition:: Join, partition, and ASOF helpers

   Join helpers enforce explicit casing and collision handling rules while
   partition helpers expose common window and bucketing patterns. ASOF joins
   coordinate tolerance windows so incremental data sources can align to
   historical records without full equality matches.【F:src/duckplus/core.py†L533-L807】

.. admonition:: Filter expression helpers

   Filter utilities normalize placeholders and parameter bindings, making it safe
   to combine raw SQL expressions with Python values without leaking identifiers
   or quoting mistakes into the final statement.【F:src/duckplus/core.py†L533-L807】

.. _duckrel:

``DuckRel``
^^^^^^^^^^^

``DuckRel`` wraps DuckDB relations with immutable composition primitives.
Methods such as ``project``, ``order_by``, ``limit``, and ``materialize`` each
return a new ``DuckRel``, preserving the original relation while keeping
transformations type-aware.【F:src/duckplus/core.py†L533-L904】

.. _duckplus-secrets:

Secrets management (``duckplus.secrets``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Secrets helpers integrate with DuckDB's ``secrets`` extension while maintaining a
Python-first registry for configuration storage.【F:src/duckplus/secrets.py†L1-L264】

- ``SecretDefinition(name, engine, parameters)`` validates identifier casing,
  normalizes engine names, and preserves insertion order so exported secrets
  remain stable across environments. Attempting to normalize parameters when a
  name already exists raises, guaranteeing explicit overwrite semantics.【F:src/duckplus/secrets.py†L76-L136】
- ``SecretManager(connection, registry=None, auto_load=True)`` wraps a
  ``DuckConnection``, optionally reusing a shared registry, and lazily attempts to
  load DuckDB's ``secrets`` extension. ``create_secret()`` always writes into the
  registry and mirrors the secret into DuckDB when the extension is available;
  ``drop_secret()`` removes it from both places; ``sync()`` lets callers copy
  cached secrets into DuckDB later. All identifier inputs are validated so they
  remain safe for interpolation into SQL statements.【F:src/duckplus/secrets.py†L138-L226】【F:src/duckplus/secrets.py†L228-L264】

.. _duckplus-html:

HTML preview (``duckplus.html``)
--------------------------------

``to_html(rel, max_rows=100, null_display='', **style)`` renders a lightweight
HTML preview of a relation. Column headers preserve original casing, cell values
are escaped inside DuckDB, and truncated datasets add a ``<tfoot>`` summary.
Optional ``class``/``id`` attributes can be supplied via keyword arguments
(``class_`` to avoid clashing with Python keywords).【F:src/duckplus/html.py†L1-L94】

.. _duckplus-cli:

Command line interface (``duckplus.cli``)
-----------------------------------------

``cli_main`` (exported as ``duckplus.cli_main``) wraps :func:`duckplus.cli.main`.
The CLI offers a read-only SQL runner, schema inspection, and an optional REPL.
Connections are opened in read-only mode when a database path is provided;
errors from DuckDB or the filesystem are surfaced as user-friendly
messages.【F:src/duckplus/cli.py†L1-L120】

.. _public-namespace:

Public namespace
----------------

Importing from ``duckplus`` provides all of the classes and helpers documented
above through the module's ``__all__`` definition, making ``from duckplus import
DuckRel, DuckTable, connect`` the canonical entry point for most
applications.【F:src/duckplus/__init__.py†L64-L94】

.. _demo-walkthroughs:

Demo walkthroughs
-----------------

The following lightweight walkthroughs combine the primitives above so teams can
visualize how the API reference maps onto everyday tasks without relying on any
additional helpers.

Demo: Build a transformation pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example chains a handful of ``DuckRel`` helpers to prepare a transformed
view before materializing it for downstream use.

.. code-block:: python

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

- ``DuckConnection.read_parquet`` and ``DuckConnection.read_csv`` validate paths
  and wrap the resulting relations in ``DuckRel`` for further
  composition.【F:src/duckplus/connect.py†L85-L143】【F:src/duckplus/io.py†L680-L812】
- ``cast_columns``, ``natural_left``, ``filter``, ``order_by``, and ``limit`` each
  return a new ``DuckRel``, ensuring the pipeline stays immutable and
  case-aware.【F:src/duckplus/core.py†L533-L807】
- ``materialize()`` defaults to the Arrow strategy and ensures the resulting
  table can be reused without mutating the original
  relation.【F:src/duckplus/core.py†L844-L904】【F:src/duckplus/materialize.py†L21-L55】

Demo: Append only unseen rows into a fact table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Mutable table helpers complement the immutable pipeline by enforcing explicit
ingestion semantics.

.. code-block:: python

   from duckplus import DuckTable, connect

   with connect("warehouse.duckdb") as conn:
       fact_orders = DuckTable(conn, "analytics.fact_orders")
       staging = conn.read_parquet([Path("/loads/fact_orders_delta.parquet")])

       inserted = fact_orders.insert_antijoin(staging, keys=["order_id"])
       print(f"Inserted {inserted} new rows")

- ``DuckTable`` validates the dotted identifier without changing casing, keeping
  schema ownership explicit.【F:src/duckplus/table.py†L1-L76】
- ``insert_antijoin`` performs a case-aware anti join using the provided keys and
  returns the number of appended rows for
  observability.【F:src/duckplus/table.py†L113-L194】
- ``DuckConnection.read_parquet`` mirrors the same path validation behaviour
  shown earlier, so ingestion always flows through typed
  helpers.【F:src/duckplus/connect.py†L85-L106】【F:src/duckplus/io.py†L680-L787】

Demo: Provision and sync connection secrets
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Secrets management is designed to be connection-aware without relying on global
state.

.. code-block:: python

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

- ``SecretDefinition.normalized()`` guarantees safe identifier casing before the
  secret ever reaches DuckDB.【F:src/duckplus/secrets.py†L1-L74】
- ``SecretManager`` coordinates registry storage with optional extension loading
  and exposes ``create_secret``/``sync`` helpers for deterministic
  mirroring.【F:src/duckplus/secrets.py†L138-L264】
