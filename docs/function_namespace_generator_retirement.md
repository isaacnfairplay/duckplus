# Function namespace generator retirement plan

`scripts/generate_function_namespaces.py` still emits the massive
`duckplus.typed._generated_function_namespaces` module so that static helper
classes can mirror DuckDB's catalog. Now that helpers are registered through
import-time decorators, we can retire the generator once the per-function
modules provide complete coverage.

## Why the generator is being retired

- Decorator-based registration already binds helpers directly to
  `duckplus.duckcon.DuckCon` and the typed expression namespaces. Keeping a
  generator that writes dictionary literals works against the direct-Python
  design constraint.
- The generated module is difficult to audit or review. Per-function modules keep
  signatures and documentation close to the implementation so tooling can
  inspect them at import time.
- Removing the generator eliminates a DuckDB connection requirement from the
  build toolchain and simplifies contributor setup.

## Readiness criteria

Before the generator can be removed, all of the following must be true:

1. Every helper emitted today has a hand-authored module that registers the same
   behaviour through decorators during import.
2. Tests in `tests/test_typed_function_namespace.py` cover the decorator-backed
   namespaces without importing `_generated_function_namespaces`.
3. Documentation in `docs/typed_api.md` and release notes reference the new
   modules directly so contributors know where helpers live.
4. `duckplus/typed/functions.py` and other compatibility shims emit clear
   deprecation warnings when the generated catalog is imported, nudging
   downstream code to adopt the new modules.
5. The release branch that removes the generator keeps the old module in the git
   history so tags can rebuild past releases if required.

## Migration timeline

| Release | Status |
|---------|--------|
| 1.2.0   | Ship decorator-backed modules alongside the generated catalog and mark the generator as deprecated in release notes. |
| 1.3.0   | Stop importing `_generated_function_namespaces` by default. Tests should fail if the generator is required. |
| 1.4.0   | Delete `scripts/generate_function_namespaces.py` and the generated module entirely. |

If the decorator-backed modules slip, push the retirement by one minor release
while keeping the deprecation notice active.

## Contributor expectations

- Move helpers into `duckplus/typed/functions/` (or a similar per-function
  directory) and register them with the existing decorators.
- Update or add tests as helpers graduate from the generated catalog so we can
  detect gaps early.
- Mention the migration window in pull requests that touch typed functions so
  reviewers can keep the roadmap in view.

## Communication plan

- Reference this plan from `docs/registry_inventory.md` so contributors can find
  the retirement schedule while auditing registries.
- Summarise progress and remaining blockers in `TODO.md` under the "Active
  Notes" section whenever the status changes.
- When the generator is finally removed, record the breaking change in the
  changelog and update prior release documentation so the removal history stays
  discoverable.
