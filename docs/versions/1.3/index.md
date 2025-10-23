# DuckPlus 1.3

DuckPlus 1.3 finalises the aggregate helper migration and makes the static typed
API the canonical entrypoint for expression builders. Every DuckDB aggregate now
ships from a decorator-backed Python module, which means IDEs and documentation
resolve the real helper implementations instead of generated namespaces. The
release also aligns the packaging and documentation metadata with the new
default so that the static namespace is front and centre for contributors and
users alike.

```{tip}
All code snippets in the 1.3 guides run against the ``duckplus`` package
published on PyPI. They assume Python 3.11+ and DuckDB 0.10+; if you are pinned
to an older runtime, consult the {doc}`../typed_api` appendix for migration
notes and breaking-change callouts.
```

## Release highlights

- **Decorator-backed aggregates everywhere** – all twenty aggregate helper
  families (from `avg` and `count` to regression, quantiles, and map builders)
  now live in :mod:`duckplus.functions.aggregate` modules with explicit
  registration side effects. Tests assert that helper provenance, docstrings,
  and overload metadata come from these Python modules instead of
  `_generated_function_namespaces`.
- **Static typed namespace as the default** – the package root exports
  :mod:`duckplus.static_typed` by default while the legacy :mod:`duckplus.typed`
  wrapper acts as a compatibility alias with deprecation warnings. Parity tests
  keep the surfaces aligned so migrating code bases can rely on the new
  namespace confidently.
- **Documentation alignment for the release** – the Sphinx build, version
  switcher, and contributor guides now point at the 1.3 documentation tree,
  keeping quick-start and reference material consistent with the shipped
  package.

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
api/index
reference/index
```

If you are upgrading from DuckPlus 1.2, the {doc}`getting_started` chapter
highlights the stable import paths while the deep-dive guides explain how each
helper composes with DuckDB. The derived :doc:`practitioner_demos` include a
DuckDB parity checklist that mirrors the interactive demo site, and the
expanded :doc:`core/function_modules` page documents the direct-Python helper
catalog so static tooling and application developers can reason about DuckPlus
without inspecting the generated sources.
