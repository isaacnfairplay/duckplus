# Contributing to DuckPlus

DuckPlus is moving toward a **direct-Python registration model**. Every helper
that affects the public API must be defined explicitly in Python—classes,
functions, or decorators—and attached at import time. This avoids hidden
registries, keeps IDE completion honest, and lets documentation pull accurate
signatures straight from the objects we ship.

## Design principles

- **Bind helpers through decorators.** Use
  :func:`duckplus.io.duckcon_helper` and
  :func:`duckplus.typed.functions.duckdb_function` (or analogous decorators) to
  attach behaviour when a module imports. Avoid storing callables in module- or
  class-level dictionaries for later registration.
- **Prefer module-level definitions.** Helpers, mixins, and expression
  factories should live in standard Python modules so import order alone
  determines which behaviours are available. Do not rely on runtime discovery
  (``pkg_resources`` entrypoints, JSON manifests, ``**kwargs`` funnels, etc.).
- **Expose real signatures.** Accept keyword arguments explicitly rather than
  accepting ``**kwargs`` and forwarding them downstream. This keeps type
  checkers and editors aligned with DuckDB's options.
- **Guard overwrites intentionally.** Registration helpers already detect name
  collisions. When exposing extension points (e.g. ``DuckCon.register_helper``)
  prefer explicit ``overwrite`` flags over implicit mutation of registries.
- **Keep documentation and tests in sync.** Whenever the fluent API surface
  changes, update the API guides and add regression tests that import the new
  helper directly. The goal is for behaviour to be discoverable by importing the
  module, not by inspecting generated data.

## Workflow expectations

1. **Answer the preflight questions** at the top of ``TODO.md`` before starting
   new work. They ensure the change advances the direct-Python approach.
2. **Update the roadmap.** When you complete a TODO item, mark it finished and
   record any fresh insights so the next task owner inherits the context.
3. **Run the required checks** before pushing a branch:

   ```bash
   pytest
   mypy duckplus
   uvx
   pylint duckplus
   ```

   Add targeted tests whenever behaviour changes.
4. **Document the public surface.** Extend the relevant API guide (I/O,
   relation helpers, typed expressions, etc.) whenever you add or modify helper
   behaviour so the documentation matches the import-time objects.
5. **Favour small, explicit modules.** When a helper suite grows large, split it
   into submodules (e.g. ``duckplus/typed/expressions/string.py``) and use
   decorator-based registration to attach the public helpers. Generated or
   data-driven loaders should be considered temporary compatibility layers.

Following these steps keeps DuckPlus predictable for users while reinforcing the
fluent API ethos across the codebase.
