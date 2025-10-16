# DuckPlus 1.0

DuckPlus 1.0 introduces a cohesive workflow for managing DuckDB relations with
immutable semantics, typed expression helpers, and curated IO adapters. The API
surface adheres to the open/closed principle: new capabilities are layered on
top of the existing connection, relation, and typed expression primitives
without forcing breaking changes. The reference documentation is divided into
guides that mirror the main developer journeys—from opening a managed connection
to shipping data products.

```{toctree}
:maxdepth: 2
:caption: Start here

getting_started
core/duckcon
core/relations
core/typed_expressions
io/overview
io/file_append
community_extensions
schema_management
practitioner_demos
```

If you are upgrading from an earlier preview, the {doc}`getting_started`
chapter highlights the stable import paths, while the deep-dive guides explain
how each helper composes with DuckDB.
