# Typed relational API architecture

This note captures how Duck+ tracks column metadata today, lessons from other
typed SQL DSLs, and a proposal for introducing a schema-centric abstraction
that unlocks richer static typing and more reliable chaining semantics.

## Current DuckRel metadata flow

### Relation construction

* `DuckRel.__init__` accepts parallel sequences of column names, raw DuckDB type
  strings, and optional `DuckType` markers. It normalizes column casing, asserts
  matching lengths, and stores the metadata as tuples so that every relation
  carries a complete schema snapshot.【F:src/duckplus/duckrel.py†L256-L303】
* Helper accessors (e.g., `_column_index`, `_column_marker`,
  `_metadata_from_expression`) resolve column names and annotate expressions with
  the recorded marker and Python annotation, validating that declared builders
  still match the underlying DuckDB schema.【F:src/duckplus/duckrel.py†L318-L413】

### Column expression builders

* Column builder utilities in `filters.py` retain the requested column name,
  optional `DuckType` marker, and cached Python annotation. They expose metadata
  so that projection helpers can read the declared types without reconstructing
  them from SQL text.【F:src/duckplus/filters.py†L95-L207】

### Projection and star modifiers

* `project_columns`, `drop`, and `project` rebuild `DuckRel` instances with
  sliced names, DuckDB types, and propagated markers, defaulting to `Unknown`
  when a raw SQL string omits typing metadata.【F:src/duckplus/duckrel.py†L457-L555】
* Star modifiers (`rename_columns`, `transform_columns`, `add_columns`) share
  `_apply_star_projection`, which rewrites column lists, injects new expressions,
  and updates the parallel metadata tuples before instantiating a new
  `DuckRel`.【F:src/duckplus/duckrel.py†L592-L666】【F:src/duckplus/duckrel.py†L1410-L1479】

### Aggregations

* `aggregate` coordinates grouping keys, aggregate builders, and projection
  aliases. It resolves markers from expressions, collects the resulting DuckDB
  types, and constructs a new relation whose metadata reflects both grouping and
  aggregated columns.【F:src/duckplus/duckrel.py†L694-L855】

### Joins and casts

* Join helpers compile explicit select lists that detect name collisions, apply
  suffixes, and interleave left/right metadata to describe the combined
  schema.【F:src/duckplus/duckrel.py†L1481-L1586】
* Casting helpers rebuild select lists with `CAST`/`TRY_CAST` SQL and update the
  stored markers by mapping requested DuckDB types through the `DuckType`
  registry.【F:src/duckplus/duckrel.py†L1364-L1408】

### Observed pain points

* Metadata is duplicated across multiple tuples, making it easy for future
  changes to forget updating one of the synchronized structures.
* Marker validation logic is scattered between the relation constructor and
  helper utilities, complicating efforts to add richer typing semantics.
* Callers cannot easily introspect the current schema beyond bespoke accessors,
  limiting how we expose typed column objects to IDEs or static analyzers.

## Lessons from typed SQL DSLs

### Ibis

* Uses `Schema` objects that bind column names to logical types and value types,
  enabling expression trees to derive result schemas automatically.
* Column objects expose both backend and frontend typing information, allowing
  static type checkers to reason about expressions composed with selectors.
* Type declarations live alongside table definitions (`ibis.schema`), avoiding
  duplication between introspection and expression construction.

### SQLAlchemy 2.0

* Declarative mappings treat `Column` objects as the single source of truth for
  database types, ORM field definitions, and `Annotated` Python typing.
* Core expression objects carry `TypeEngine` metadata that propagates through
  selects, joins, and functions, with 2.0’s typing overhaul making expressions
  generic over their Python value types.
* Table metadata exposes schema-bound column accessors (`table.c.<name>`), so
  chained SQL retains both logical names and typing information without
  redeclaration.

### dbt semantic models

* Semantic models centralize typed definitions for entities, measures, and
  dimensions. Downstream queries reference those names, keeping field semantics
  consistent across transformations.
* Type information is declared once in YAML and consumed by the compiler, docs,
  and lineage tooling—highlighting the ecosystem benefits of a unified metadata
  source.

## Proposed DuckSchema architecture

### Core objects

* Introduce a `DuckSchema` value object that owns an ordered mapping of column
  names to `ColumnDefinition` instances.
* `ColumnDefinition` fields:
  * `name`: canonical column identifier (normalized casing).
  * `duck_type`: `type[DuckType]` marker describing logical capabilities.
  * `duckdb_type`: raw DuckDB type string for round-tripping with the engine.
  * `python_type`: cached Python annotation derived from the marker.
  * Optional `origin`: structured metadata (e.g., source relation alias,
    expression description) for debugging and docs generation.
* `DuckSchema` responsibilities:
  * Normalize column casing and maintain deterministic order.
  * Provide case-insensitive lookups and helper methods for deriving new schemas
    (`select`, `rename`, `append`, `concat`, `replace`).
  * Centralize marker validation (`_ensure_declared_marker`) and expose typed
    iterators for integration with static typing utilities.

### DuckRel integration

* `DuckRel` stores a single `DuckSchema` instead of parallel tuples. The
  constructor delegates to `DuckSchema.from_relation(...)` to consolidate
  normalization, validation, and caching.
* Expression helpers (column builders, `AggregateExpression`, star modifiers)
  request services from the schema (e.g., `schema.resolve(name)`,
  `schema.marker(name)`) instead of duplicating lookup logic.
* Schema-deriving operations:
  * `project_columns` / `drop`: call `schema.select(resolved_columns)` to get a
    narrowed schema.
  * `project`: combine rendered expressions with metadata from expressions or
    defaults (`Unknown`) and call `schema.replace(columns)` to produce new
    definitions.
  * Star modifiers: `schema.rename`, `schema.replace`, and `schema.add` update
    definitions while preserving ordering and suffix logic.
  * `aggregate`: `schema.grouped(...)` builds grouped definitions, optionally
    tracking grouping keys separately for `.col()` ergonomics. Builders should
    infer whether each expression is an aggregate or grouping key based on
    whether any aggregate functions were applied, so callers do not need to
    pre-classify arguments. The helper should also encourage callers to reuse
    the relation instance that is being aggregated (e.g., passing expressions
    that reference `rel.col("name")`) so column metadata always originates from
    the same schema snapshot, rather than from chained intermediate objects.
  * `join`: compose schemas via `DuckSchema.join(left, right, projection_config)`
    returning merged definitions and collision suffix metadata.
  * `cast_columns`: update definitions by mapping requested DuckDB types through
    `DuckType.lookup` once, avoiding duplicate loops.
* Expose typed column accessors (`DuckRel.schema.column("name")`) that return a
  `ColumnDefinition`, enabling `.col()` builders to reuse canonical metadata.

### Static typing surface

* Generate `TypedDict`/`Protocol` stubs from `DuckSchema` for IDEs, powering
  `DuckRel.fetch_typed()` or similar helpers with auto-derived tuple
  annotations.
* Provide opt-in factories (`typed_select`) that accept callables producing
  `ColumnExpression[T]` objects so chained methods retain precise Python value
  types.
* Explore emitting `.pyi` files or a plugin (akin to SQLAlchemy’s mypy plugin)
  that reads `DuckSchema` definitions and generates static typing hints.

## Workflow validation

1. **Initial typed select**
   ```python
   rel = conn.table("orders").select(
       order_id=col("order_id", duck_type=ducktypes.BigInt),
       order_ts=col("order_ts", duck_type=ducktypes.TimestampTz),
       total=col("total", duck_type=ducktypes.Decimal),
   )
   ```
   * `DuckSchema` validates markers once and records the canonical schema.
2. **Column operations and aggregation**
   ```python
   rel = rel.col("total").rename("total_amount")

   aggregated = rel.aggregate(
       rel.col("order_ts").date().alias("order_date"),
       daily_total=rel.col("total_amount").sum(),
   )

   rel = aggregated.order_by(aggregated.col("order_date"))
   ```
   * Each method derives a validated schema for the new relation and updates
     column definitions and annotations accordingly. Aggregation helpers detect
     that `daily_total` uses an aggregate function and treat `order_date` as a
     grouping column without extra flags. Storing the relation before the
     aggregation keeps typed column accessors (`rel.col(...)`, `rel.schema`) in
     scope so we can build expressions with the exact relation instance being
     aggregated.

3. **Typed materialization**
   * Downstream `.col()` calls always resolve against the schema, so renames,
     suffixes, and aggregates return updated definitions without re-declaring
     types.
   * Static type checkers infer the tuple shape of `rel.fetch_typed()` (or
     similar) via the schema metadata.

### Chaining ergonomics and relation self-reference

* `.aggregate` and similar helpers should steer developers away from chaining a
  new relation just before an aggregation call, because doing so makes it
  harder to reference the relation instance that owns the schema when building
  typed column expressions. Documentation and helper signatures should nudge
  users to grab the relation into a local (as shown above) or to pass a callable
  that receives the relation (`rel.aggregate(lambda r: {...})`).
* Enforcing that expressions originate from the relation under transformation
  lets us validate that `ColumnDefinition` objects share identity with the
  schema being mutated. We can surface linting or runtime guards that reject
  expressions constructed from other relations, reinforcing the “self-aware”
  workflow the chaining proposal targets.

## Migration and compatibility considerations

* Preserve `Unknown` as the default marker so untyped call sites behave exactly
  as today; `DuckSchema` simply wraps the existing metadata.
* Continue accepting raw `duck_types` sequences in the constructor and convert
  them into a schema internally to avoid breaking advanced users.
* Offer transitional accessors (`DuckRel.column_type_markers`,
  `DuckRel.column_python_annotations`) that proxy to the schema, maintaining
  compatibility with any downstream tooling that relies on those methods.
* Introduce `DuckSchema` privately first, add targeted regression tests to prove
  parity with current metadata outputs, and publish migration guides explaining
  how `.project()`, joins, and star modifiers map onto the new schema helpers.
* Provide feature flags (e.g., `DuckRel(select_schema=True)`) for early adopters
  while we iterate on the public API, enabling opt-in feedback without forcing
  a breaking change.

## Open questions and next steps

* How should we represent computed columns (e.g., expressions without direct
  source columns) in the `origin` metadata so that documentation and lineage
  tools can trace them accurately?
* Should we eagerly compute Python annotations for every column, or lazily cache
  them to avoid overhead when users operate in a purely dynamic mode?
* What is the right user-facing API for generating typed stubs—auto-emitted
  `.pyi` files during `uv build`, a mypy plugin, or an explicit CLI command?
* Can we align the schema object with DuckDB’s native introspection APIs to
  avoid discrepancies between runtime results and declared metadata?
