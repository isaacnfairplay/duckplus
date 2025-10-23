# Runtime overview

DuckPlus 2.0 draws a clear line between connections (which *read* data) and
relations (which are immutable views capable of *writing* or *appending*
content). The split keeps imports fast because the runtime only instantiates the
pieces you touch.

## Connection responsibilities

* ``duckplus.Connection.read_csv`` / ``read_parquet`` / ``read_json`` /
  ``read_excel`` construct immutable relations. The Excel reader automatically
  retries after issuing ``LOAD excel`` if a ``CatalogException`` is raised,
  delivering the DuckDB-style autoload experience without import-time overhead.
* Connections can materialise in-memory relations from arbitrary records via
  ``Connection.from_rows``.

## Relation responsibilities

* ``Relation.write_*`` methods emit new artifacts (CSV, Parquet, JSON).
* ``Relation.append_*`` methods honour anti-join de-duplication, partition
  routing, and rollover policies expressed as Python classes. Parquet appends
  default to directory layouts; opting into single-file updates requires setting
  ``force_single_file=True`` which emits a warning.
* ``Relation.materialize()`` produces a fully materialised snapshot without any
  ``lazy`` flag juggling and caches ``row_count`` / ``estimated_size_bytes``
  properties for light-weight observability.

The runtime exposes these behaviours directly in Python classes, so static
analysers do not depend on dynamic registries.

## Table responsibilities

``duckplus.table.Table`` is a light-weight in-memory catalogue for materialised
relations. ``Table.insert`` accepts either a ``Relation`` or a callable that
returns a ``Relation``. The callable path keeps relation construction lazy while
still forcing explicit ``materialize()`` semantics once the table stores the
rows. Pass ``create=True`` on the first insert to persist the schema and
``overwrite=True`` to replace existing rows.
