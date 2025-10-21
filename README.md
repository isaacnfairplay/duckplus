# DuckPlus README

A tightly scoped field guide for building reliable DuckDB workloads with DuckPlus.
It captures the core patterns, design opinions, and guardrails encoded in the
codebase so an AI or human teammate can adopt the same defaults quickly.

## TL;DR
- Manage DuckDB connections with `DuckCon`; it installs optional extensions,
  binds helper functions via decorators, and guarantees cleanup through the context manager
  protocol.【F:duckplus/duckcon.py†L35-L181】
- Wrap every transformation in the immutable `Relation` layer so column names,
  types, and dependencies stay validated as you compose pipelines.【F:duckplus/relation.py†L36-L142】【F:duckplus/relation.py†L466-L520】
- Use `ducktype` factories for any SQL expression; they encode dependencies,
  support aggregates, and drive the `select()` builder without raw strings. The
  namespace now resolves to the static implementation by default; importing
  ``duckplus.typed`` still works but emits a deprecation warning that points to
  :mod:`duckplus.static_typed`.【F:duckplus/static_typed/__init__.py†L1-L111】【F:duckplus/typed/__init__.py†L1-L60】
- Reach for the domain-organised helpers in :mod:`duckplus.functions` when you
  need typed wrappers for DuckDB's approximation aggregates. Modules such as
  ``duckplus.functions.aggregate.approximation`` register their helpers via
  decorators at import time so IDEs surface concrete call signatures without
  relying on generated registries.【F:duckplus/functions/__init__.py†L5-L40】【F:duckplus/functions/aggregate/__init__.py†L1-L40】
- Prefer the I/O, schema, and table helpers exposed from the package root for
  consistent error handling and identifier quoting. Built-in readers such as
  ``DuckCon.read_csv``, ``DuckCon.read_parquet``, and ``DuckCon.read_excel``
  ship pre-registered via :func:`duckplus.io.duckcon_helper`, so you can call
  them directly from the manager.【F:duckplus/__init__.py†L7-L44】【F:duckplus/duckcon.py†L73-L181】【F:duckplus/io/__init__.py†L1-L661】【F:duckplus/schema.py†L1-L118】【F:duckplus/table.py†L1-L64】

## Repository Layout
| Area | Purpose | Highlights |
| --- | --- | --- |
| `duckplus/duckcon.py` | Connection lifecycle | Extension discovery, decorator-driven helper binding, strict context usage.【F:duckplus/duckcon.py†L35-L214】 |
| `duckplus/relation.py` | Immutable relational DSL | Column/type caching, dependency validation, pandas/arrow/polars sampling.【F:duckplus/relation.py†L36-L231】 |
| `duckplus/static_typed/` | Primary typed expressions | Statically defined typed expressions backed by the production factories, aggregate helpers, and SELECT builder.【F:duckplus/static_typed/__init__.py†L1-L111】【F:duckplus/static_typed/expression.py†L1-L275】 |
| `duckplus/typed/` | Deprecated alias | Thin wrapper that re-exports :mod:`duckplus.static_typed` while emitting a deprecation warning for downstream callers.【F:duckplus/typed/__init__.py†L1-L60】 |
| `duckplus/functions/aggregate/` | DuckDB aggregate helpers | Decorator-registered wrappers with side-effect imports that expose approximation suites directly from Python modules.【F:duckplus/functions/aggregate/__init__.py†L1-L46】【F:duckplus/functions/aggregate/approximation.py†L1-L200】 |
| `duckplus/io/` | File readers | Normalised options, identifier quoting, connection guards for CSV/Parquet/JSON.【F:duckplus/io/__init__.py†L1-L194】 |
| `duckplus/schema.py` | Schema diff tooling | Drift detection with warnings plus file-based comparisons.【F:duckplus/schema.py†L1-L140】 |
| `duckplus/table.py` | Managed inserts | Cross-relation guardrails, automatic create/overwrite handling.【F:duckplus/table.py†L1-L64】 |

## Core Concepts & Opinions
1. **Connection Safety First** – Always open `DuckCon` via `with DuckCon(...) as con:`.
   The class guards extension installation, shared helper registration, and
   enforces teardown even when errors surface.【F:duckplus/duckcon.py†L35-L114】
2. **Immutable Relations** – Every `Relation` method returns a new wrapper so you
   can stage transformations without side effects. The wrapper caches column
   metadata and refuses to run if the originating `DuckCon` is closed, keeping
   pipelines deterministic.【F:duckplus/relation.py†L36-L161】【F:duckplus/relation.py†L466-L520】
3. **Typed Expressions over Strings** – Build SQL fragments through `ducktype`
   factories. They know their upstream column dependencies, which lets helpers
   detect missing columns and surface precise errors at composition time instead
   of deferring to DuckDB runtime failures. Importing :mod:`duckplus.static_typed`
   yields the canonical namespace, and `duckplus.typed` redirects there with a
   deprecation warning.【F:duckplus/relation.py†L466-L520】【F:duckplus/typed/__init__.py†L1-L60】
4. **Helper Registration Beats Raw SQL** – Use `DuckCon.register_helper` for I/O
   or custom routines so callers can invoke them through `apply_helper` without
   re-plumbing connections. Core helpers like `read_csv`, `read_parquet`, and
   `read_json` bind at import time via :func:`duckplus.io.duckcon_helper`, so
   every manager exposes the common file readers. Override them with
   `overwrite=True` when custom behaviour is required.【F:duckplus/duckcon.py†L73-L170】【F:duckplus/io/__init__.py†L1-L661】
5. **Schema Drift is a First-Class Signal** – Rely on `schema.diff_relations`
   and `schema.diff_files` to compare datasets and emit warnings when types
   change unexpectedly, instead of building ad-hoc checks.【F:duckplus/schema.py†L1-L140】

## Building Pipelines
1. **Establish the connection**:
   ```python
   from duckplus import DuckCon

   with DuckCon(extra_extensions=("nanodbc",)) as con:
       relation = con.sql("SELECT * FROM events")  # regular DuckDB relation
   ```
2. **Wrap into DuckPlus**:
   ```python
   from duckplus import Relation

   events = Relation.from_relation(DuckCon(database=":memory:"), relation)
   ```
   Prefer retrieving relations directly through `DuckCon` helpers (see below)
   so the wrapper captures the active connection automatically.【F:duckplus/relation.py†L210-L231】
3. **Compose with typed expressions**:
   ```python
   from duckplus import ducktype

   enriched = (
       events
       .add(
           (ducktype.Numeric("value") * 2).alias("double_value"),
           newer=ducktype.Boolean("is_new").coalesce(False),
       )
       .transform(updated_at="updated_at::TIMESTAMP")
   )
   ```
   `Relation.add` rejects unknown dependencies, duplicate aliases, and closed
   connections before dispatching SQL, giving immediate feedback.【F:duckplus/relation.py†L466-L520】
4. **Aggregate with builders**:
   ```python
   summary = (
       enriched
       .aggregate()
       .agg(ducktype.Numeric.Aggregate.sum("double_value"), alias="total")
       .agg(ducktype.Numeric.Aggregate.avg("value"), alias="avg_value")
       .all()
   )
   ```
   Aggregate factories encapsulate valid numeric operations and return typed
   expressions ready for `Relation.aggregate()` builders.【F:duckplus/typed/expressions/numeric.py†L158-L235】
5. **Export responsibly**:
   ```python
   manager = DuckCon()
   with manager:
       snapshot = manager.read_parquet(
           "data/events.parquet",
           columns=["event_id", "value"],
       )
   ```
   I/O wrappers normalise option aliases, quote identifiers, and keep parity with
   DuckDB keyword arguments while still requiring an open connection. The helpers
   register on every `DuckCon` instance, so the import stays optional.【F:duckplus/duckcon.py†L73-L159】【F:duckplus/io/__init__.py†L1-L194】

## Tables & Persistence
- Use `duckplus.Table` when inserting into managed DuckDB tables; it validates
  that the source relation originates from the same `DuckCon` and optionally
  creates or overwrites the destination in one call.【F:duckplus/table.py†L1-L64】
- Prefer `Table.insert_relation` for raw DuckDB relations (e.g., produced by
  bulk loaders) to reuse the same guardrails without wrapping first.【F:duckplus/table.py†L33-L64】

## Schema Verification Loop
1. Profile candidate datasets via `Relation.null_ratios()` and
   `Relation.row_count()` for quick health checks.【F:duckplus/relation.py†L70-L125】
2. Use `schema.diff_relations(baseline, candidate)` to compare columns and types.
   Enable `warn=True` (default) so type drift emits actionable warnings.【F:duckplus/schema.py†L1-L118】
3. When working with files, delegate to `schema.diff_files` so DuckPlus handles
   reader selection and option normalisation internally.【F:duckplus/schema.py†L118-L140】

## Extension & Helper Patterns
- Pass `extra_extensions` when creating `DuckCon` to auto-install community
  extensions like `nanodbc` and `excel`; the class deduplicates requests and
  orchestrates installation safely.【F:duckplus/duckcon.py†L47-L84】【F:duckplus/duckcon.py†L120-L181】
- Call `duckcon.extensions()` to inspect installation status, including version
  metadata, aliases, and source location for debugging environments.【F:duckplus/duckcon.py†L147-L206】
- Audit bundled DuckDB extensions with :func:`duckplus.extensions.collect_bundled_extension_audit` to confirm
  helper coverage and regenerate the published report via ``scripts/audit_extensions.py`` when releases ship.【F:duckplus/extensions.py†L1-L109】【F:scripts/audit_extensions.py†L1-L82】
- Register connection-aware helpers with `DuckCon.register_helper` then invoke
  them through `apply_helper(name, ...)` so you never pass around raw
  connections.【F:duckplus/duckcon.py†L88-L147】

## Testing & Quality Gates
- Run `pytest` for behavioural coverage, `mypy duckplus` for typing, `uvx` for
  repository-specific validation (mirrors local policy), and `pylint duckplus`
  for style enforcement before shipping changes.【F:AGENTS.md†L1-L6】
- Documentation lives in `docs/` and is published via GitHub Pages; update it in
  tandem with new features to keep the public site aligned.【F:docs/index.md†L1-L20】

## Further Reading
- Browse the published guides at
  [isaacnfairplay.github.io/duckplus/latest](https://isaacnfairplay.github.io/duckplus/latest/).
- Review `TODO.md` for roadmap context and preflight discovery questions that
  inform future feature design.
