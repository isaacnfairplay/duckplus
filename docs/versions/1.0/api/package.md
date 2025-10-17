# ``duckplus`` package

Import :mod:`duckplus` to access the managed connection, relation wrapper, and
schema helpers shipped in the 1.0 release. The package exports a compact surface
area so projects can pin to a single entry point without reaching into internal
modules.

## Public attributes

``DuckPlus`` exposes the following symbols at the package root:

- :class:`DuckCon <duckplus.duckcon.DuckCon>` – context manager for DuckDB
  connections.
- :class:`Relation <duckplus.relation.Relation>` – immutable wrapper around a
  ``DuckDBPyRelation``.
- :class:`Table <duckplus.table.Table>` – lightweight helper for inserting into
  managed tables.
- :mod:`io <duckplus.io>` – file readers that return relations while preserving
  connection metadata.
- :mod:`schema <duckplus.schema>` – schema comparison helpers and supporting
  data structures.

```python
from duckplus import DuckCon, Relation, Table, io, schema
```

The module defers imports to runtime so the package can be imported without
DuckDB installed. Accessing any of the public attributes will raise a
``ModuleNotFoundError`` that explains how to install the ``duckdb`` dependency.

## Lazy import behaviour

When DuckDB is unavailable, the package sets placeholders to ``None`` but
provides attribute access hooks that raise informative errors. The placeholders
are typed as ``Optional`` so static checkers flag missing dependencies early
while keeping runtime error messages friendly.
