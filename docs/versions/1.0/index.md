# DuckPlus 1.0

DuckPlus 1.0 introduces a cohesive workflow for managing DuckDB relations with
immutable semantics, typed expression helpers, and curated IO adapters. The API
surface adheres to the open/closed principle: new capabilities are layered on
top of the existing connection, relation, and typed expression primitives
without forcing breaking changes. The reference documentation is divided into
guides that mirror the main developer journeys—from opening a managed
connection to shipping data products. Each chapter now calls out guard-rails,
integration tips, and direct links to the underlying implementation so you can
trace behaviour with confidence.

```{tip}
All code snippets in the 1.0 guides run against the ``duckplus`` package
published on PyPI. They assume Python 3.11+ and DuckDB 0.10+; if you are pinned
to an older runtime, consult the {doc}`../typed_api` appendix for migration
notes and breaking-change callouts.
```

## Release highlights

- **Immutable-first relations** keep transformations predictable while exposing
  convenient exporters for Arrow, Pandas, and Polars.
- **Typed expressions** provide static guarantees around column usage and
  integrate directly with the Select builder.
- **IO adapters and extension helpers** surface DuckDB's ecosystem in a
  Pythonic way, offering first-run installation guidance and actionable errors.

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
io/overview
io/file_append
community_extensions
schema_management
practitioner_demos
reference/index
```

If you are upgrading from an earlier preview, the {doc}`getting_started`
chapter highlights the stable import paths, while the deep-dive guides explain
how each helper composes with DuckDB. For a tour of real-world usage,
{doc}`practitioner_demos` showcases language server walkthroughs, notebooks, and
the deterministic ``sales_pipeline`` project that powers many of the examples.
The {doc}`reference/index` reference documents every public class, method, and
function in the 1.0 API surface so static tooling and application developers can
reason about DuckPlus without inspecting the source code.
