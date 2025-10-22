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
6. Boolean aggregates (`bool_and`, `bool_or`) now live in `duckplus/functions/aggregate/boolean.py`, overriding the generated namespace at import time with decorator-backed helpers validated by new introspection tests.【F:duckplus/functions/aggregate/boolean.py†L1-L173】【F:tests/test_typed_function_namespace.py†L109-L122】
7. Core counting aggregates (`count`, `count_if`, `count_star`, and legacy `countif`) now reside in `duckplus/functions/aggregate/counting.py`, keeping aliases in sync with decorator-backed helpers and barrel exports validated by updated introspection and import tests.【F:duckplus/functions/aggregate/counting.py†L1-L354】【F:duckplus/functions/aggregate/__init__.py†L1-L81】【F:tests/test_typed_function_namespace.py†L179-L195】【F:tests/test_function_import_barrels.py†L8-L55】
8. Generic aggregate helper `any_value` (and its FILTER form) now lives in `duckplus/functions/aggregate/generic.py`, overriding the generated namespace with decorator-backed helpers validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/generic.py†L1-L125】【F:tests/test_typed_function_namespace.py†L197-L212】【F:tests/test_function_import_barrels.py†L8-L47】
9. Statistical skewness aggregates now live in `duckplus/functions/aggregate/statistics.py`, keeping docstrings aligned with the generated descriptions while tests pin their provenance and the aggregate barrel re-exports.【F:duckplus/functions/aggregate/statistics.py†L1-L129】【F:tests/test_typed_function_namespace.py†L169-L194】【F:tests/test_function_import_barrels.py†L8-L55】
10. Bitwise aggregate helpers (`bit_and`, `bit_or`, `bit_xor`) now live in `duckplus/functions/aggregate/bitwise.py`, replacing the generated namespace entries with decorator-backed helpers validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/bitwise.py†L1-L245】【F:tests/test_typed_function_namespace.py†L161-L205】【F:tests/test_function_import_barrels.py†L6-L58】
11. Ordering aggregate helper `first` (and its `FILTER` variant) now lives in `duckplus/functions/aggregate/ordering.py`, replacing the generated namespace entry with decorator-backed helpers validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/ordering.py†L1-L116】【F:tests/test_typed_function_namespace.py†L169-L204】【F:tests/test_function_import_barrels.py†L6-L58】
12. Central tendency aggregate helper `mode` (and its `FILTER` variant) now lives in `duckplus/functions/aggregate/mode.py`, replacing the generated namespace entry with decorator-backed helpers validated by updated barrel and namespace tests.【F:duckplus/functions/aggregate/mode.py†L1-L132】【F:tests/test_function_import_barrels.py†L1-L62】【F:tests/test_typed_function_namespace.py†L161-L216】
13. Arg-extrema helpers across blob, varchar, numeric, and generic aggregates now ship from `duckplus.functions.aggregate.arg_extrema`, which registers the decorator-backed overload metadata—including the `ANY[]` variants—onto every namespace while namespace tests pin the provenance.【F:duckplus/functions/aggregate/arg_extrema.py†L1-L438】【F:tests/test_typed_function_namespace.py†L248-L296】
14. Median aggregate helpers now live in `duckplus/functions/aggregate/median.py`, registering ``median`` and ``median_filter`` directly onto the generic namespace with decorator-backed docstrings verified by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/median.py†L1-L118】【F:tests/test_function_import_barrels.py†L8-L73】【F:tests/test_typed_function_namespace.py†L225-L258】
15. Quantile aggregate helpers now live in `duckplus/functions/aggregate/quantiles.py`, registering ``quantile``/``quantile_disc``/``quantile_cont`` (and FILTER variants) onto the generic namespace with decorator-backed docstrings validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/quantiles.py†L1-L223】【F:tests/test_function_import_barrels.py†L6-L120】【F:tests/test_typed_function_namespace.py†L212-L264】
16. String aggregation helpers now live in `duckplus/functions/aggregate/string.py`, registering ``string_agg`` and ``string_agg_filter`` onto the varchar aggregate namespace with decorator-backed docstrings validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/string.py†L1-L136】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:duckplus/functions/__init__.py†L5-L44】【F:tests/test_function_import_barrels.py†L6-L130】【F:tests/test_typed_function_namespace.py†L213-L245】
17. Bitstring aggregation helpers now live in `duckplus/functions/aggregate/bitstring.py`, registering ``bitstring_agg`` and ``bitstring_agg_filter`` onto the generic namespace with decorator-backed signatures validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/bitstring.py†L1-L126】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:duckplus/functions/__init__.py†L5-L44】【F:tests/test_function_import_barrels.py†L6-L130】【F:tests/test_typed_function_namespace.py†L201-L245】
18. Map aggregation helper ``map`` now lives in `duckplus/functions/aggregate/map.py`, registering the decorator-backed helper onto the generic aggregate namespace with barrel and namespace tests asserting its provenance.【F:duckplus/functions/aggregate/map.py†L1-L86】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:tests/test_function_import_barrels.py†L6-L140】【F:tests/test_typed_function_namespace.py†L200-L256】
19. Extremum-by-value aggregates now live in `duckplus/functions/aggregate/extremum_by_value.py`, registering ``max_by``/``min_by`` (and their ``FILTER`` variants) across blob, varchar, numeric, and generic namespaces with decorator-backed overloads validated by the updated barrel and namespace tests.【F:duckplus/functions/aggregate/extremum_by_value.py†L1-L272】【F:tests/test_function_import_barrels.py†L1-L84】【F:tests/test_typed_function_namespace.py†L248-L356】
20. Basic extrema helpers now live in `duckplus/functions/aggregate/extrema.py`, registering ``max``/``min`` (and FILTER variants) across boolean, blob, varchar, numeric, and generic namespaces with decorator-backed overloads validated by the barrel re-export assertions and namespace provenance tests.【F:duckplus/functions/aggregate/extrema.py†L1-L302】【F:duckplus/functions/aggregate/__init__.py†L1-L148】【F:duckplus/functions/__init__.py†L5-L72】【F:tests/test_function_import_barrels.py†L1-L110】【F:tests/test_typed_function_namespace.py†L150-L360】
21. Regression and covariance helpers now live in `duckplus/functions/aggregate/regression.py`, registering ``covar_*`` and ``regr_*`` aggregates directly on the numeric namespace while tests pin the decorator-backed docstrings and barrel provenance.【F:duckplus/functions/aggregate/regression.py†L1-L390】【F:duckplus/functions/aggregate/__init__.py†L1-L190】【F:duckplus/functions/__init__.py†L5-L92】【F:tests/test_function_import_barrels.py†L1-L120】【F:tests/test_typed_function_namespace.py†L200-L320】

### Discovery Log – Function module documentation (2024-05-20)
1. Publishing domain documentation for :mod:`duckplus.functions` gives contributors a concrete reference for how side-effect modules and helper re-exports should behave as more DuckDB functions migrate away from generated registries.【F:docs/versions/1.3/core/function_modules.md†L1-L60】
2. The README now points directly at the new modules so the quick-start guidance stays aligned with the package exports while reiterating the decorator-based registration strategy.【F:README.md†L10-L23】
3. Versioned guides promote the 1.3 documentation tree, keeping quick references (I/O, typed API, schema) pointed at the latest release when future work lands.【F:docs/index.md†L9-L14】【F:docs/io.md†L5-L7】

### Discovery Log – Scalar macro migration (2024-05-26)
1. Introduced :mod:`duckplus.functions.scalar.string` so ``split_part``, ``array_to_string``, and ``array_to_string_comma_default`` live in decorator-backed helpers that register onto the varchar namespace at import time.【F:duckplus/functions/scalar/string.py†L1-L136】
2. The scalar barrel mirrors the aggregate package, publishing ``SIDE_EFFECT_MODULES`` so tests can assert the registration surface while top-level imports re-export the helpers.【F:duckplus/functions/scalar/__init__.py†L1-L34】【F:duckplus/functions/__init__.py†L1-L118】
3. Importing :mod:`duckplus.functions.scalar.string` from :mod:`duckplus.static_typed` guarantees the scalar string macros attach to the typed namespace without relying on prior ``duckplus.functions`` imports, covering the first static-definition bucket.【F:duckplus/static_typed/__init__.py†L5-L54】
4. Regression tests pin the new module provenance for both the typed namespace and the import barrels, keeping introspection aligned with the Python implementations.【F:tests/test_typed_function_namespace.py†L1-L420】【F:tests/test_function_import_barrels.py†L1-L208】
5. PostgreSQL privilege macros now live in :mod:`duckplus.functions.scalar.postgres_privilege`, and the boolean overrides expose them through the runtime barrel and typed namespace without relying on generated registries.【F:duckplus/functions/scalar/postgres_privilege.py†L1-L726】【F:duckplus/static_typed/function_overrides/scalar_postgres_privilege.py†L1-L322】【F:tests/test_function_import_barrels.py†L1-L340】【F:tests/test_typed_function_namespace.py†L360-L468】
6. PostgreSQL visibility macros now ship from :mod:`duckplus.functions.scalar.postgres_visibility`, replacing the generated boolean namespace entries for ``pg_*_is_visible`` and ``pg_has_role`` across both runtime and typed barrels.【F:duckplus/functions/scalar/postgres_visibility.py†L1-L276】【F:duckplus/static_typed/function_overrides/scalar_postgres_visibility.py†L1-L276】【F:tests/test_function_import_barrels.py†L1-L420】【F:tests/test_typed_function_namespace.py†L360-L540】
- **Progress:** Scalar macro buckets now cover 47 of 130 helpers (~36%) with decorator-backed modules.

### Discovery Log – Static typed graduation (2024-05-24)
1. `duckplus.static_typed` now houses the production factories, aggregates, and decimal helpers directly—no proxy modules—so the static namespace is the single source of truth.【F:duckplus/static_typed/__init__.py†L1-L111】【F:duckplus/static_typed/expression.py†L1-L275】
2. The package root exposes that namespace by default, while the deprecated :mod:`duckplus.typed` wrapper simply redirects to it with a deprecation warning for callers that have not migrated yet.【F:duckplus/__init__.py†L7-L68】【F:duckplus/typed/__init__.py†L1-L60】
3. Compatibility tests guard the aliasing behaviour so future changes continue to emit the warning and share implementations.【F:tests/test_static_typed_parity.py†L1-L44】

### Active Notes – Replace data-driven registries
1. Binding helpers directly onto `DuckCon` keeps the fluent method surface intact while dropping the per-instance registry dictionary.
2. `duckplus.duckcon` now exposes a `duckcon_helper` decorator so `duckplus.io` can attach helpers at import time without late mutation.
3. `duckplus.static_typed.functions.duckdb_function` registers DuckDB function helpers during module import so the generated namespaces expose real methods without `_IDENTIFIER_FUNCTIONS`/`_SYMBOLIC_FUNCTIONS` dictionaries.
4. Decimal expression factories now attach via `duckplus.static_typed.expressions.decimal.register_decimal_factories`, eliminating per-instance caches while exporting every combination as a module attribute.
5. The type parser maps simple types through `_build_simple_type` conditionals rather than a registry dict, keeping aliases explicit in Python code.
6. Existing docs/tests around `DuckCon.read_*` and the typed expression namespaces confirmed the decorator strategy preserves ergonomics across both APIs.
7. Helper registration still guards overwrites, honours open-connection checks via `require_connection`, and keeps `apply_helper` dispatching to bound methods, while typed helpers now publish symbolic operators via the decorator-managed `symbols` map.
8. mypy, uvx, pylint, pytest, and docs build remain the required validation gates once implementation stabilises.
9. `_StaticFunctionNamespace` preserves legacy `_IDENTIFIER_FUNCTIONS`/`_SYMBOLIC_FUNCTIONS` mappings so older generated modules and caller introspection continue to work while decorator-populated namespaces land.
10. `DuckTypeNamespace.decimal_factory_names` reads from the decorator-populated tuple defined in `duckplus.static_typed.expressions.decimal`, keeping legacy lookups stable without any rebuild shim.
11. Contributor docs and API guides now describe the decorator-based registration pattern so new helpers default to direct Python attachments.
12. The deprecation schedule for `scripts/generate_function_namespaces.py` is published in `docs/function_namespace_generator_retirement.md`; review it whenever per-function modules land to keep the migration window accurate.

## Fluent Function API Migration
### Registry Inventory
- `duckplus.duckcon.DuckCon.register_helper` dynamically binds helper callables onto the class and proxies through `apply_helper`, keeping a runtime extension point for connection-scoped helpers. Default helpers continue to arrive import-time via `duckplus.io.duckcon_helper`, and both paths are exercised by tests and docs that rely on `DuckCon.read_*` and `apply_helper` behaving interchangeably.【F:duckplus/duckcon.py†L120-L148】【F:duckplus/io/__init__.py†L32-L36】【F:tests/test_duckcon.py†L26-L66】【F:docs/io.md†L9-L35】
- `duckplus.io.duckcon_helper` attaches each file reader directly to `DuckCon` at import time rather than stashing them in a registry dictionary. The decorator still mutates the class, so future refactors should confirm helpers remain import-side effects while keeping overwrite checks centralised in `DuckCon.register_helper`. Behaviour is validated by explicit reader tests across CSV/Parquet/JSON helpers and by documentation examples that expect helpers to be pre-bound.【F:duckplus/io/__init__.py†L32-L520】【F:tests/test_io_helpers.py†L32-L134】【F:docs/versions/1.1/io/overview.md†L19-L105】
- `duckplus.static_typed.functions._StaticFunctionNamespace` keeps `_IDENTIFIER_FUNCTIONS`/`_SYMBOLIC_FUNCTIONS` dicts as compatibility shims while the `duckdb_function` decorator marks methods for registration during class creation. Refactors should converge on decorator-only attachment once generated modules migrate. Tests assert the legacy dicts still populate, and docs already describe the decorator workflow that new code should follow.【F:duckplus/static_typed/functions.py†L83-L195】【F:tests/test_typed_function_namespace.py†L1-L47】【F:docs/typed_api.md†L220-L229】
- `duckplus.static_typed._generated_function_namespaces` remains dictionary-driven: each namespace class hard-codes `_IDENTIFIER_FUNCTIONS` mappings and instantiates aggregate/ scalar/window registries at module import. Until the generator emits decorator-decorated methods, any rewrite must preserve the literal mappings consumed by current tests and namespace consumers.【F:duckplus/static_typed/_generated_function_namespaces.py†L1334-L1380】【F:duckplus/static_typed/_generated_function_namespaces.py†L45667-L45686】【F:tests/test_typed_function_namespace.py†L33-L47】
- `duckplus.static_typed.expressions.decimal.register_decimal_factories` decorates `DuckTypeNamespace` so every decimal helper binds at class definition, and the module exports the factories directly for downstream imports. Tests and docs ensure `decimal_factory_names` mirrors the shared tuple without relying on re-registration shims.【F:duckplus/static_typed/expression.py†L1-L150】【F:duckplus/tests/test_typed_function_namespace.py†L1-L77】【F:docs/typed_api.md†L1-L120】
- `duckplus.static_typed.__init__` and `duckplus.static_typed.ducktype` still loop through `ducktype.decimal_factory_names` to inject the dynamically attached decimal helpers into their module globals. This preserves import ergonomics for callers (`from duckplus import Decimal_18_2`) while the underlying factories move toward decorator-driven attachment.【F:duckplus/static_typed/__init__.py†L52-L100】【F:duckplus/static_typed/ducktype.py†L9-L41】
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

#### Aggregate helper migration tracker
- [x] Approximation sketches (`approx_count_distinct`, `approx_top_k`, `histogram`)
- [x] Boolean conjunction/disjunction (`bool_and`, `bool_or`)
- [x] Counting helpers (`count`, `count_if`, `count_star`, `countif`)
- [x] Generic passthrough (`any_value`)
- [x] Bitwise aggregates (`bit_and`, `bit_or`, `bit_xor`)
- [x] Ordering helpers (`first`)
- [x] Central tendency (mode)
- [x] Central tendency (median)
- [x] Arg-extrema suite (`arg_{min,max}`)
- [x] Statistical skewness (`skewness`)
- [x] String concatenation (`string_agg`)
- [x] Bitstring aggregation (`bitstring_agg`)
- [x] List aggregation (`list`/`array` accumulation helpers)
- [x] Extremum by value (`max_by`, `min_by`)
- [x] Map/struct aggregation (`map` builders)
- [x] Quantile suite (`quantile`, `quantile_cont`, `quantile_disc`)
- [x] Regression & covariance (`regr_*`, `covar_*`)
- [x] Basic extrema (`min`, `max`)
- [x] Summation & product (`sum`, `product`)
- [x] Averages (`avg`, `mean`)

- **Progress:** 20 of 20 aggregate helper families migrated (~100%).

#### Scalar macro migration tracker
- [x] Ensure scalar string macros load with :mod:`duckplus.static_typed` so they override the generated namespace automatically.【F:duckplus/static_typed/__init__.py†L5-L54】【F:duckplus/functions/scalar/string.py†L1-L136】
- [x] Migrate scalar list macros (``array_append``, ``array_prepend``, ``array_push_back``, ``array_push_front``, ``array_pop_back``, ``array_pop_front``, ``array_intersect``, ``array_reverse``) onto decorator-backed helpers that register with the generic namespace at import time.【F:duckplus/functions/scalar/list.py†L1-L249】【F:duckplus/static_typed/function_overrides/scalar_generic.py†L1-L249】
- [x] Migrate catalog/session macros (``current_*``, ``session_user``, ``pg_get_*``, ``pg_size_pretty``, ``pg_typeof``) onto decorator-backed helpers that register with the varchar namespace and preserve DuckDB provenance for typed overrides.【F:duckplus/functions/scalar/system.py†L1-L366】【F:duckplus/static_typed/function_overrides/scalar_system.py†L1-L366】【F:tests/test_function_import_barrels.py†L1-L210】【F:tests/test_typed_function_namespace.py†L360-L440】
- [x] Migrate PostgreSQL privilege macros (``has_*_privilege``) onto decorator-backed helpers that register with the boolean namespace and mirror typed overrides.【F:duckplus/functions/scalar/postgres_privilege.py†L1-L726】【F:duckplus/static_typed/function_overrides/scalar_postgres_privilege.py†L1-L322】【F:duckplus/functions/scalar/__init__.py†L1-L220】【F:duckplus/functions/__init__.py†L1-L220】【F:tests/test_function_import_barrels.py†L1-L420】【F:tests/test_typed_function_namespace.py†L360-L520】
- [x] Migrate PostgreSQL visibility macros (``pg_*_is_visible``, ``pg_has_role``) onto decorator-backed helpers that register with the boolean namespace and mirror typed overrides.【F:duckplus/functions/scalar/postgres_visibility.py†L1-L276】【F:duckplus/static_typed/function_overrides/scalar_postgres_visibility.py†L1-L276】【F:duckplus/functions/scalar/__init__.py†L1-L220】【F:duckplus/functions/__init__.py†L1-L220】【F:tests/test_function_import_barrels.py†L1-L420】【F:tests/test_typed_function_namespace.py†L360-L540】

- **Progress:** 5 of 5 scalar macro buckets migrated (~100%).

### Discovery Log – Release version alignment (2025-10-21)
1. Update the Sphinx configuration to advertise DuckPlus 1.4.3 so doc builds label the upcoming release correctly.【F:docs/conf.py†L19-L20】
2. Refresh the version switcher JSON so published sites offer the 1.4.3 guides alongside prior releases and the rolling ``latest`` channel.【F:docs/_static/version_switcher.json†L1-L27】
3. These adjustments keep the documentation navigation ready for the release without changing the existing quick-start toctrees.【F:docs/index.md†L1-L20】

## DuckDB Function Module Layout
### Goal
Adopt a one-function-per-file pattern for DuckDB wrappers where it improves clarity and reuse, enabling decorators to register each function with compatible types during import.

### Tasks
- [x] Audit existing DuckDB function wrappers to determine sensible module boundaries (e.g. aggregates, math, string, date/time). *(Captured in `docs/duckdb_function_module_audit.md`.)*
- [x] Track window-function coverage; generated namespaces are currently empty and need verification once DuckDB exposes wrappers downstream.【F:docs/duckdb_function_module_audit.md†L18-L26】
- [x] Create per-function modules that expose the function implementation and decorate registration with the relevant typed categories.
- [x] Migrate the remaining approximation helpers (e.g. ``approx_quantile``, ``histogram``) into ``duckplus/functions/aggregate`` modules following the new registration pattern.
- [x] Provide shared base utilities (e.g. in `duckplus/functions/_base.py`) to hold common decorator logic without reintroducing data-driven registries.
- [x] Update import barrels (such as `duckplus/functions/__init__.py`) to expose the decorated functions while keeping import side effects explicit and testable.
- [x] Adjust documentation and examples to reference the new module paths. *(Documented in `docs/versions/1.3/core/function_modules.md` and the README highlights.)*

Aggregate helpers currently ship from ``duckplus/functions/aggregate/`` so contributors can browse approximation wrappers directly without relying on generated dictionaries.【F:duckplus/functions/aggregate/__init__.py†L1-L46】【F:duckplus/functions/aggregate/approximation.py†L1-L200】

## Typed Expression Alignment
### Goal
Keep typed expressions first-class while ensuring their helpers follow the same direct-Python registration pattern.

### Tasks
- [x] Refactor any typed expression factories that currently build behaviour from dict definitions to use explicit classes or functions.
- [ ] Where helper suites remain large, split them into submodules (`duckplus/typed/expressions/<category>.py`) so decorators can register variants without data tables.
- [ ] Expand test coverage to assert decorators correctly attach metadata (SQL rendering, dependencies) during import.
- [ ] Synchronise type stubs and documentation with the new module structure.
- [x] Documented the decimal decorator module and removed shim references across guides so contributors rely on ``duckplus.typed.expressions.decimal`` directly.【F:docs/typed_api.md†L41-L63】【F:docs/versions/1.1/core/typed_expressions.md†L25-L53】【F:docs/versions/1.0/core/typed_expressions.md†L13-L31】

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
- [x] Update contributor docs to describe the decorator-based registration workflow and rationale against runtime loaders. *(Expanded the design principles with a domain-module example in `docs/contributing.md`.)*
- [ ] Provide migration notes highlighting the benefits (import-time safety, IDE compatibility, easier debugging).
- [ ] Revise API reference pages to point to the new module layout and ensure Sphinx autodoc picks up the decorated functions.

## Quality Gates
The following checks remain mandatory before merging any work:
- mypy
- uvx
- pylint
- pytest (full suite)
- Documentation build if relevant changes are made
