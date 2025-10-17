# DuckPlus 1.1

DuckPlus 1.1 builds on the 1.0 foundation with richer typed-expression tooling,
curated parity guides for the official DuckDB demos, and hardened
Sphinx-multiversion configuration so documentation for each release remains easy
to navigate. The API surface continues to follow the open/closed principle: new
capabilities extend managed connections, immutable relations, and typed
expressions without forcing breaking changes.

```{tip}
All code snippets in the 1.1 guides run against the ``duckplus`` package
published on PyPI. They assume Python 3.11+ and DuckDB 0.10+; if you are pinned
to an older runtime, consult the {doc}`../typed_api` appendix for migration
notes and breaking-change callouts.
```

## Release highlights

- **DuckDB demo parity guides** map the official engine showcases to DuckPlus
  helpers so teams can translate proof-of-concept SQL into production-ready
  pipelines.
- **Typed function catalogue** documents registered window, scalar, and
  aggregate helpers in a single place for easier discovery.
- **Documentation infrastructure** now defaults the version switcher to a stable
  ``latest`` alias, preventing empty dropdowns on GitHub Pages deployments.

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

If you are upgrading from DuckPlus 1.0, the {doc}`getting_started` chapter
highlights the stable import paths while the deep-dive guides explain how each
helper composes with DuckDB. The derived :doc:`practitioner_demos` now include a
DuckDB parity checklist that mirrors the interactive demo site, and the
{doc}`reference/index` reference documents every public class, method, and function in
the 1.1 API surface so static tooling and application developers can reason
about DuckPlus without inspecting the source code.
