# Implementation Roadmap

## Preflight Discovery Questions
Answer these before starting any TODO item to confirm scope and alignment with the direct-Python design constraint.
1. How does the change reinforce the fluent API ethos (method chaining, typed helpers) without introducing runtime-loaded metadata structures?
2. Which concrete modules, classes, or decorators should host the behaviour so that registration happens through Python imports alone?
3. What existing documentation, tests, or historical commits demonstrate the expected ergonomics, and how will they inform a purely Python implementation?
4. Which success criteria and edge cases ensure the new behaviour integrates cleanly with the rest of the API without relying on dynamic registries?
5. Which validation steps (pytest, mypy, uvx, pylint, documentation rebuilds) are required to prove the behaviour matches expectations?

### Discovery Log – Test introspection coverage (2024-05-09)
1. Exercising helper metadata through unit tests guarantees the fluent API stays discoverable via ``DuckCon`` methods rather than hidden registries, keeping chaining ergonomics intact.
2. Assertions will live in ``tests/test_duckcon.py`` and ``tests/test_typed_function_namespace.py`` so they directly target helpers defined in ``duckplus/io`` and ``duckplus/typed/_generated_function_namespaces`` where decorators bind behaviour at import time.
3. Existing I/O helper documentation and typed API guides (``docs/io.md`` and ``docs/typed_api.md``) already showcase introspectable examples, and the new tests mirror those usage patterns through direct module references.
4. Success looks like verifying docstrings, signatures, and pickling of helpers/methods without opening connections, proving everything needed for documentation and serialization flows from the Python objects themselves.
5. Run ``pytest``, ``mypy duckplus``, ``uvx``, and ``pylint duckplus`` to confirm behavioural, typing, policy, and style gates stay green.

### Discovery Log – DuckDB function module audit (2024-05-15)
1. Grouping helpers into per-domain modules keeps every wrapper defined directly in Python while preserving the fluent chaining ergonomics outlined for typed namespaces.【F:docs/duckdb_function_module_audit.md†L42-L74】
2. Aggregate families will live in `duckplus/functions/aggregate/` (statistics, quantiles, approximation, arg-extrema), while scalar helpers split across `duckplus/functions/scalar/` subpackages for math, datetime, strings, JSON, and collection tooling so decorators still register behaviour at import time.【F:docs/duckdb_function_module_audit.md†L76-L101】
3. Existing typed API docs and the generator retirement plan already steer contributors toward decorator-backed modules, providing the reference ergonomics for the split.【F:docs/typed_api.md†L11-L16】【F:docs/function_namespace_generator_retirement.md†L3-L55】
4. Success hinges on moving large helper clusters (e.g. ICU collations, quantiles, regression suites) into those modules without breaking signatures or docstrings; the audit enumerates these hotspots so tests can target them explicitly.【F:docs/duckdb_function_module_audit.md†L24-L74】
5. Validation continues to require `pytest`, `mypy duckplus`, `uvx`, `pylint duckplus`, and rebuilding docs when the module layout or narrative guidance changes.【F:TODO.md†L96-L110】

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
11. Contributor docs and API guides now describe the decorator-based registration pattern so new helpers default to direct Python attachments.
12. The deprecation schedule for `scripts/generate_function_namespaces.py` is published in `docs/function_namespace_generator_retirement.md`; review it whenever per-function modules land to keep the migration window accurate.

## Fluent Function API Migration
### Registry Inventory
- `duckplus.duckcon.DuckCon.register_helper` dynamically binds helper callables onto the class and proxies through `apply_helper`, keeping a runtime extension point for connection-scoped helpers. Default helpers continue to arrive import-time via `duckplus.io.duckcon_helper`, and both paths are exercised by tests and docs that rely on `DuckCon.read_*` and `apply_helper` behaving interchangeably.【F:duckplus/duckcon.py†L120-L148】【F:duckplus/io/__init__.py†L32-L36】【F:tests/test_duckcon.py†L26-L66】【F:docs/io.md†L9-L35】
- `duckplus.io.duckcon_helper` attaches each file reader directly to `DuckCon` at import time rather than stashing them in a registry dictionary. The decorator still mutates the class, so future refactors should confirm helpers remain import-side effects while keeping overwrite checks centralised in `DuckCon.register_helper`. Behaviour is validated by explicit reader tests across CSV/Parquet/JSON helpers and by documentation examples that expect helpers to be pre-bound.【F:duckplus/io/__init__.py†L32-L520】【F:tests/test_io_helpers.py†L32-L134】【F:docs/versions/1.1/io/overview.md†L19-L105】
- `duckplus.typed.functions._StaticFunctionNamespace` keeps `_IDENTIFIER_FUNCTIONS`/`_SYMBOLIC_FUNCTIONS` dicts as compatibility shims while the `duckdb_function` decorator marks methods for registration during class creation. Refactors should converge on decorator-only attachment once generated modules migrate. Tests assert the legacy dicts still populate, and docs already describe the decorator workflow that new code should follow.【F:duckplus/typed/functions.py†L83-L195】【F:tests/test_typed_function_namespace.py†L1-L47】【F:docs/typed_api.md†L220-L229】
- `duckplus.typed._generated_function_namespaces` remains dictionary-driven: each namespace class hard-codes `_IDENTIFIER_FUNCTIONS` mappings and instantiates aggregate/ scalar/window registries at module import. Until the generator emits decorator-decorated methods, any rewrite must preserve the literal mappings consumed by current tests and namespace consumers.【F:duckplus/typed/_generated_function_namespaces.py†L1334-L1380】【F:duckplus/typed/_generated_function_namespaces.py†L45667-L45686】【F:tests/test_typed_function_namespace.py†L33-L47】
- `duckplus.typed.expression.DuckTypeNamespace` decorates the class with `_attach_decimal_factories` to plant every decimal helper as an attribute, then runs `_register_decimal_factories` per instance to rebuild the legacy `_decimal_names` list. Tests and docs depend on `decimal_factory_names` and manual re-registration staying available during the transition.【F:duckplus/typed/expression.py†L79-L142】【F:tests/test_typed_function_namespace.py†L49-L65】【F:docs/typed_api.md†L41-L56】
- `duckplus.typed.__init__` and `duckplus.typed.ducktype` still loop through `ducktype.decimal_factory_names` to inject the dynamically attached decimal helpers into their module globals. This preserves import ergonomics for callers (`from duckplus import Decimal_18_2`) while the underlying factories move toward decorator-driven attachment.【F:duckplus/typed/__init__.py†L52-L100】【F:duckplus/typed/ducktype.py†L9-L41】
- Full snapshot recorded in `docs/registry_inventory.md` for reference.【F:docs/registry_inventory.md†L1-L41】

### Goal
Reorient the function exposure strategy so every helper is defined directly in Python modules or classes, removing the need for runtime dict/dataclass registries while keeping the fluent API expressive.

### Tasks
- [x] Inventory all current registry-style loaders and identify the modules where they live. *(Documented in `docs/registry_inventory.md`.)*
- [x] Replace data-driven registries with import-time decorators or explicit class attributes so availability is controlled by Python execution order instead of runtime parsing. *(DuckCon helpers, typed namespaces, decimal factories, and type parser aliases now activate at import time.)*
- [x] Ensure all registration helpers live beside their implementations to preserve discoverability and IDE support. *(I/O helpers now define ``duckcon_helper`` in ``duckplus.io`` so binding lives with the helper implementations while ``duckplus.duckcon`` proxies for compatibility.)*
- [x] Update tests to reflect the absence of runtime loaders and verify functions remain serialisable/documentable through Python introspection alone. *(New assertions cover docstrings, signatures, and picklability for import-time helper attachments.)*
- [x] Document the new convention in `docs/contributing.md` and API guides so future work naturally follows the direct-Python model.
- [x] Schedule retirement of `scripts/generate_function_namespaces.py` once decorator-driven per-function modules fully replace the generated catalog, and document the migration window for contributors. *(Documented in `docs/function_namespace_generator_retirement.md` with staged release targets.)*

## DuckDB Function Module Layout
### Goal
Adopt a one-function-per-file pattern for DuckDB wrappers where it improves clarity and reuse, enabling decorators to register each function with compatible types during import.

### Tasks
- [x] Audit existing DuckDB function wrappers to determine sensible module boundaries (e.g. aggregates, math, string, date/time). *(Captured in `docs/duckdb_function_module_audit.md`.)*
- [x] Track window-function coverage; generated namespaces are currently empty and need verification once DuckDB exposes wrappers downstream.【F:docs/duckdb_function_module_audit.md†L18-L26】
- [x] Create per-function modules that expose the function implementation and decorate registration with the relevant typed categories.
- [x] Migrate the remaining approximation helpers (e.g. ``approx_quantile``, ``histogram``) into ``duckplus/functions/aggregate`` modules following the new registration pattern.
- [x] Provide shared base utilities (e.g. in `duckplus/functions/_base.py`) to hold common decorator logic without reintroducing data-driven registries.
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
