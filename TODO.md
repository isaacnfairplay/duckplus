# Implementation Roadmap

## Preflight Discovery Questions
Answer these before starting any TODO item to confirm scope and alignment with the direct-Python design constraint.
1. How does the change reinforce the fluent API ethos (method chaining, typed helpers) without introducing runtime-loaded metadata structures?
2. Which concrete modules, classes, or decorators should host the behaviour so that registration happens through Python imports alone?
3. What existing documentation, tests, or historical commits demonstrate the expected ergonomics, and how will they inform a purely Python implementation?
4. Which success criteria and edge cases ensure the new behaviour integrates cleanly with the rest of the API without relying on dynamic registries?
5. Which validation steps (pytest, mypy, uvx, pylint, documentation rebuilds) are required to prove the behaviour matches expectations?

### Active Notes – Replace data-driven registries
1. Binding helpers directly onto `DuckCon` keeps the fluent method surface intact while dropping the per-instance registry dictionary.
2. `duckplus.duckcon` now exposes a `duckcon_helper` decorator so `duckplus.io` can attach helpers at import time without late mutation.
3. `duckplus.typed.functions.duckdb_function` registers DuckDB function helpers during module import so the generated namespaces expose real methods without `_IDENTIFIER_FUNCTIONS`/`_SYMBOLIC_FUNCTIONS` dictionaries.
4. Decimal expression factories now attach via the `_attach_decimal_factories` decorator, eliminating the per-instance cache/loop while still exporting every combination as a class attribute.
5. The type parser maps simple types through `_build_simple_type` conditionals rather than a registry dict, keeping aliases explicit in Python code.
6. Existing docs/tests around `DuckCon.read_*` and the typed expression namespaces confirmed the decorator strategy preserves ergonomics across both APIs.
7. Helper registration still guards overwrites, honours open-connection checks via `require_connection`, and keeps `apply_helper` dispatching to bound methods, while typed helpers now publish symbolic operators via the decorator-managed `symbols` map.
8. mypy, uvx, pylint, pytest, and docs build remain the required validation gates once implementation stabilises.
9. `_StaticFunctionNamespace` preserves legacy `_IDENTIFIER_FUNCTIONS`/`_SYMBOLIC_FUNCTIONS` mappings so older generated modules and caller introspection continue to work while decorator-populated namespaces land.
10. `DuckTypeNamespace` still exposes `_register_decimal_factories` and `_decimal_names` so downstream namespaces can rebuild decimal helpers without depending on the removed cache.

## Fluent Function API Migration
### Goal
Reorient the function exposure strategy so every helper is defined directly in Python modules or classes, removing the need for runtime dict/dataclass registries while keeping the fluent API expressive.

### Tasks
- [ ] Inventory all current registry-style loaders and identify the modules where they live.
- [x] Replace data-driven registries with import-time decorators or explicit class attributes so availability is controlled by Python execution order instead of runtime parsing. *(DuckCon helpers, typed namespaces, decimal factories, and type parser aliases now activate at import time.)*
- [x] Ensure all registration helpers live beside their implementations to preserve discoverability and IDE support. *(I/O helpers now define ``duckcon_helper`` in ``duckplus.io`` so binding lives with the helper implementations while ``duckplus.duckcon`` proxies for compatibility.)*
- [ ] Update tests to reflect the absence of runtime loaders and verify functions remain serialisable/documentable through Python introspection alone.
- [ ] Document the new convention in `docs/contributing.md` and API guides so future work naturally follows the direct-Python model.
- [ ] Schedule retirement of `scripts/generate_function_namespaces.py` once decorator-driven per-function modules fully replace the generated catalog, and document the migration window for contributors.

## DuckDB Function Module Layout
### Goal
Adopt a one-function-per-file pattern for DuckDB wrappers where it improves clarity and reuse, enabling decorators to register each function with compatible types during import.

### Tasks
- [ ] Audit existing DuckDB function wrappers to determine sensible module boundaries (e.g. aggregates, math, string, date/time).
- [ ] Create per-function modules that expose the function implementation and decorate registration with the relevant typed categories.
- [ ] Provide shared base utilities (e.g. in `duckplus/functions/_base.py`) to hold common decorator logic without reintroducing data-driven registries.
- [ ] Update import barrels (such as `duckplus/functions/__init__.py`) to expose the decorated functions while keeping import side effects explicit and testable.
- [ ] Adjust documentation and examples to reference the new module paths.

## Typed Expression Alignment
### Goal
Keep typed expressions first-class while ensuring their helpers follow the same direct-Python registration pattern.

### Tasks
- [x] Refactor any typed expression factories that currently build behaviour from dict definitions to use explicit classes or functions.
- [ ] Where helper suites remain large, split them into submodules (`duckplus/typed/expressions/<category>.py`) so decorators can register variants without data tables.
- [ ] Expand test coverage to assert decorators correctly attach metadata (SQL rendering, dependencies) during import.
- [ ] Synchronise type stubs and documentation with the new module structure.

## Relation API Refinement
### Goal
Review relation helpers to ensure no lingering runtime loading remains and that new module structure integrates cleanly.

### Tasks
- [ ] Inspect relation helper factories for indirect registries (e.g. transform/aggregate helper lists) and rewrite them as explicit method definitions or mixins.
- [ ] Where cross-cutting behaviours exist, create mixin classes that can be inherited and composed rather than populated via runtime loops.
- [ ] Update tests to ensure relation helpers remain chainable and maintain immutability guarantees after refactors.

## Documentation Refresh
### Goal
Communicate the direct-Python design principle across contributor and user-facing materials.

### Tasks
- [ ] Update contributor docs to describe the decorator-based registration workflow and rationale against runtime loaders.
- [ ] Provide migration notes highlighting the benefits (import-time safety, IDE compatibility, easier debugging).
- [ ] Revise API reference pages to point to the new module layout and ensure Sphinx autodoc picks up the decorated functions.

## Quality Gates
The following checks remain mandatory before merging any work:
- mypy
- uvx
- pylint
- pytest (full suite)
- Documentation build if relevant changes are made
