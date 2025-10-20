# DuckPlus 1.2

DuckPlus 1.2 prepares the project for its next release by productising two
long-running initiatives: domain-organised DuckDB function helpers and
automation for auditing bundled extensions. The new modules follow the
direct-Python registration strategy so decorators bind helpers at import time
while the documentation refresh points every quick-start guide at the latest
versioned content.

```{tip}
All code snippets in the 1.2 guides run against the ``duckplus`` package
published on PyPI. They assume Python 3.11+ and DuckDB 0.10+; if you are pinned
to an older runtime, consult the {doc}`../typed_api` appendix for migration
notes and breaking-change callouts.
```

## Release highlights

- **Domain-organised DuckDB functions** land under
  :mod:`duckplus.functions`, starting with approximation aggregates registered
  via side-effect modules so IDEs surface the real call signatures without
  referencing generated registries.
- **Bundled extension audit tooling** ships in :mod:`duckplus.extensions` and
  ``scripts/audit_extensions.py`` so each release can document coverage gaps for
  the DuckDB bundles that ship alongside the engine.
- **Documentation refresh** promotes the 1.2 guides to the default quick-start
  references, ensuring the README and contributor notes link directly to the
  newly published pages.

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

If you are upgrading from DuckPlus 1.1, the {doc}`getting_started` chapter
highlights the stable import paths while the deep-dive guides explain how each
helper composes with DuckDB. The derived :doc:`practitioner_demos` include a
DuckDB parity checklist that mirrors the interactive demo site, and the
new :doc:`core/function_modules` page documents the domain split for DuckDB
function helpers so static tooling and application developers can reason about
DuckPlus without inspecting the source code.
