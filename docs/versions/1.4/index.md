# DuckPlus 1.4.3

DuckPlus 1.4.3 extends the direct-Python helper strategy to DuckDB's scalar
macro surface so decorator-backed modules now publish the `split_part` family,
array helpers, and PostgreSQL catalog shims. The static typed API imports those
modules during package initialisation, ensuring both runtime and typed
namespaces resolve the real helper implementations instead of the generated
function catalog. Patch release 1.4.3 further binds the macro helpers onto their
expression classes, enabling fluent method calls like
``ducktype.Generic("items").array_to_string(", ")`` without dropping down to the
namespace objects. Documentation and package metadata now highlight the macro
migration so contributors can rely on the override modules when exploring the
function surface.

```{tip}
All code snippets in the 1.4 guides run against the ``duckplus`` package
published on PyPI. They assume Python 3.11+ and DuckDB 0.10+; if you are pinned
to an older runtime, consult the {doc}`../typed_api` appendix for migration
notes and breaking-change callouts.
```

## Release highlights

- **Decorator-backed scalar macros** – DuckPlus 1.4 adds
  :mod:`duckplus.functions.scalar` modules for string, list, PostgreSQL catalog,
  privilege, and visibility macros. Import-time decorators register overloads
  on the correct namespaces so IDEs and documentation resolve the Python
  implementations instead of generated metadata.
- **Fluent macro chaining** – Static typed overrides now attach string and
  array macros directly to their corresponding expression classes (for example,
  ``VarcharExpression.split_part`` and ``GenericExpression.array_to_string``),
  so user code can chain helpers without manually referencing the typed
  namespaces.
- **Typed namespace override coverage** – :mod:`duckplus.static_typed` now
  imports the macro override package by default, replacing the generated
  entries with the decorator-backed helpers as soon as the typed namespace
  loads. Regression tests pin helper provenance so future migrations stay
  honest.
- **Documentation alignment for macros** – the Sphinx build and release guides
  describe the new macro buckets and link directly to the override modules so
  contributors can find the canonical implementations without spelunking through
  generated sources.

The sections below walk through the features in increasing depth. Jump straight
to the area you care about or read sequentially to build a mental model of the
entire stack.

```{toctree}
:maxdepth: 2
:caption: Start here

getting_started
core/duckcon
core/relations
core/typed_expressions
core/function_modules
io/overview
io/file_append
community_extensions
schema_management
practitioner_demos
reference/index
```

If you are upgrading from DuckPlus 1.3, the {doc}`getting_started` chapter
highlights the stable import paths while the deep-dive guides explain how each
helper composes with DuckDB. The derived :doc:`practitioner_demos` include a
DuckDB parity checklist that mirrors the interactive demo site, and the
expanded :doc:`core/function_modules` page documents the direct-Python helper
catalog so static tooling and application developers can reason about DuckPlus
without inspecting the generated sources.
