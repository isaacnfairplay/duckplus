# Migrating from DuckPlus 1.x to 2.0

DuckPlus 2.0 rebuilds the project around a Protocol-first API and a slim runtime
surface. This guide highlights the differences you will encounter when upgrading
existing pipelines.

## Type system and expression helpers

* **Protocol annotations are required.** Expression helpers are still generated
  from the same underlying function catalog, but static analysers now discover
  them through ``typing.Protocol`` interfaces instead of ``.pyi`` stubs. Cast
  expression objects with ``duckplus.typed.as_*_proto`` helpers and annotate
  variables with the matching ``*ExprProto`` types so Pyright and Astral ty see
  the full surface.
* **Metaclass-powered methods.** Runtime methods are attached by the
  ``ExprMeta`` metaclass using static SPEC dictionaries under ``spec/``. You no
  longer import large generated modules at runtime, and import-time cost drops
  dramatically. See :doc:`typed_expressions` for examples.

## Connections, relations, and materialisation

* **Readers live on ``Connection``.** All ``read_*`` helpers (CSV, JSON,
  Parquet, Excel) now belong to ``duckplus.core.Connection``. They return
  immutable ``Relation`` snapshots.
* **Relations own writers and appenders.** Write and append helpers now hang off
  ``Relation`` and require explicit ``materialize()`` calls when you need a
  concrete snapshot. The legacy ``lazy=`` flag is gone; use
  ``Relation.materialize()`` instead.
* **Observability baked in.** Materialised relations cache ``row_count`` and
  ``estimated_size_bytes`` so monitoring code can read metrics without
  re-scanning data.

## Append policy expectations

* **Directory-first Parquet appends.** ``Relation.append_parquet`` defaults to
  directory-based appends. Single-file mutation is rejected unless you pass
  ``force_single_file=True``, which emits a warning explaining the risks.
* **CSV append safeguards.** ``Relation.append_csv`` validates headers,
  delimiter, and quoting before mutating files. Anti-join de-duplication,
  partition routing, and rollover policies are shared between CSV and Parquet
  appenders.
* **Simulation helpers.** ``Relation.simulate_append_csv`` and
  ``Relation.simulate_append_parquet`` return append plans that can be inspected
  without optional dependencies such as ``pyarrow``.

## Table inserts

``Table.insert`` accepts either a ready ``Relation`` or a zero-argument callable
that returns one. The callable path lets you build relations lazily while the
method continues to enforce explicit ``materialize()`` semantics and schema
validation.

## Documentation and tooling

* **Versioned docs.** 1.x guides remain available under ``/docs/v1`` with a
  banner that links back to the 2.0 documentation in ``/docs/v2``.
* **`py.typed` marker.** DuckPlus 2.0 ships the ``py.typed`` marker so type
  checkers can consume the inline annotations directly.
* **Quality gates.** Project automation now runs ``pytest``, ``mypy``, ``uvx ty
  check``, ``uvx pylint``, and a documentation build to enforce the new
  guarantees.
