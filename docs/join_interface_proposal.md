# DuckRel Join Interface Proposal

## Overview
DuckRel already enforces explicit projection and strict column resolution. The join surface must match that clarity so call sites
announce how keys line up, how collisions are resolved, and how time-aware joins behave. This revision adopts the feedback from
our design review to present a unified, ergonomic API that keeps DuckRel's strict defaults while covering equality, predicate,
semi/anti, and ASOF joins without parallel method families.

The guiding principle is that the call signature should make column flow obvious. Developers reading call sites should
immediately see (a) which columns match by name, (b) which columns require aliasing, (c) whether additional predicates
participate, and (d) how projections and suffixes are handled. All of this composes with today's strict-missing behavior and
projection rules.

## Design Goals
- Preserve the default behavior where joins on shared column names require no additional configuration.
- Allow callers to opt in to non-shared equality keys via keyword arguments (`left_column="right_column"`) while keeping the
  interface type-checkable.
- Provide a single method per join flavor and make "natural" behavior an option rather than a separate family.
- Surface semi/anti joins so DuckRel retains parity with the existing helper set.
- Introduce a structured way to express non-equality predicates (e.g., `<`, `<=`, expressions with bound parameters) without
  falling back to raw SQL strings.
- Provide a dedicated ASOF join entry point that makes ordering semantics, tolerance requirements, and projection behavior
  explicit.
- Keep the API discoverable: a compact set of methods that hang directly off `DuckRel` and share the same keyword vocabulary.

## Unified Method Surface
Every equality-based join shares the same signature shape. We expose one method per join flavor:

| Method | Description |
| --- | --- |
| `DuckRel.inner_join(...)` | Symmetric inner join. |
| `DuckRel.left_join(...)` | Left outer join. |
| `DuckRel.right_join(...)` | Right outer join. |
| `DuckRel.full_join(...)` | Full outer join. |
| `DuckRel.semi_join(...)` | Semi join (filter left by existence on right). |
| `DuckRel.anti_join(...)` | Anti join (filter left by non-existence on right). |
| `DuckRel.cross_join(...)` | Cross join (explicit, no inferred keys). |
| `DuckRel.asof_join(...)` | ASOF join with ordered keys. |

All non-ASOF joins adopt the following keyword-only parameters:

```python
DuckRel.inner_join(
    other: DuckRel,
    /,
    *,
    use_shared: bool = True,
    on: Sequence[tuple[str, str]] | None = None,
    exclude_shared: Sequence[str] = (),
    predicates: Sequence[JoinPredicate] = (),
    project: JoinProjection | None = None,
    strict: bool = True,
    suffixes: tuple[str, str] = ("", "_r"),
    **key_aliases: str,
) -> DuckRel
```

Key rules:
- **Shared keys by default.** When `use_shared=True`, DuckRel collects all shared column names from both relations, removes any
  `exclude_shared`, and uses them as equality keys.
- **Explicit `on` keys.** `on` supplies `(left, right)` pairs. If provided alongside `use_shared`, the final key set is
  `(shared - exclude_shared) ∪ on`. Duplicate pairs collapse by name. Callers can provide the same mapping via
  `key_aliases` (e.g., `customer_id="customer_identifier"`); DuckRel normalizes the keywords to the left relation and raises on
  unknown names so Python surfaces typos early.
- **Cross protection.** If the resolved key set is empty and the caller did not explicitly request a cross join, we raise an
  error (guard against accidental Cartesian products). `cross_join` is the only helper that allows zero keys by default.
- **Strict resolution.** With `strict=True`, both sides must contain every referenced column. `strict=False` demotes missing keys
  to runtime filters (mirroring today's strictness opt-out).
- **Projection defaults.** By default we drop duplicate right-side equality keys after the join. Callers can override via the
  `project` helper (see **Projection Policy** below).
- **Suffixes.** Name collisions on non-key columns raise unless the caller supplies alternative `suffixes` or enables
  `project.allow_collisions`.

This unified shape eliminates the confusing `natural_*` vs `left_inner/left_right` split while keeping natural behavior readily
available via `use_shared=True`.

## Join Predicate Types
To describe non-equality comparisons we introduce dataclasses in `duckplus.core`:

```python
from dataclasses import dataclass
from typing import Literal, Sequence

@dataclass(frozen=True)
class ColumnPredicate:
    left: str
    operator: Literal["=", "!=", "<", "<=", ">", ">="]
    right: str

@dataclass(frozen=True)
class ExpressionPredicate:
    expression: str
    args: Sequence[object] = ()

JoinPredicate = ColumnPredicate | ExpressionPredicate
```

`ExpressionPredicate` mirrors the parameter binding style from `DuckRel.filter`: callers must use positional placeholders in
`expression` (e.g., `"left_col > right_col + ?"`) and provide values through `args`. The join builder binds these arguments when
rendering SQL so arbitrary fragments never concatenate unescaped literals. This also keeps IDEs and type checkers aware of
predicate usage.

All predicates run through validation that ensures identifiers map to either the left or right relation, operators are supported,
and no expression references ambiguous columns.

## ASOF Join Semantics
`DuckRel.asof_join` extends the equality signature with ordering requirements:

```python
DuckRel.asof_join(
    other: DuckRel,
    /,
    *,
    order: AsofOrder,  # AsofOrder(left="event_ts", right="quote_ts")
    direction: Literal["backward", "forward", "nearest"] = "backward",
    tolerance: str | None = None,
    nearest_tie: Literal["left", "right"] = "left",
    use_shared: bool = True,
    on: Sequence[tuple[str, str]] | None = None,
    exclude_shared: Sequence[str] = (),
    predicates: Sequence[JoinPredicate] = (),
    project: JoinProjection | None = None,
    strict: bool = True,
    suffixes: tuple[str, str] = ("", "_r"),
    keep_right_order_col: bool = True,
    **key_aliases: str,
) -> DuckRel
```

Rules enforced by the implementation:
- `order` supplies distinct left/right column names. Both are retained in the projection when
  `keep_right_order_col=True`; callers may drop the right column via `project` if they choose. `key_aliases` mirror the equality
  alias syntax from other joins and receive the same validation.
- `direction` selects the DuckDB ASOF mode. When `direction == "nearest"`, `tolerance` is **required**; omitting it raises a
  `ValueError`.
- `nearest_tie` controls tie breaking when two right rows are equidistant. Defaulting to `"left"` preserves the existing
  relation's value; DuckRel maps this to the corresponding DuckDB clause or a deterministic `ORDER BY` + `LIMIT 1` fallback.
- Ordering: the planner ensures DuckDB receives the relation sorted on the left order column (raising a warning if metadata shows
  it is unsorted). We document that callers should pre-sort when the plan cannot guarantee ordering.
- ASOF always renders `ON` clauses (never `USING`) to avoid DuckDB collapsing the order columns. Equality keys follow the same
  resolution rules as other joins, and additional predicates are appended to the `ON` conjuncts.

## Projection Policy
Projection control remains explicit:
- Equality joins drop right-side duplicates of key columns after the join. Callers can override via
  `project = JoinProjection(drop_right_keys=False)` or similar helpers.
- Non-equality/ASOF joins keep both order columns by default. Additional columns are subject to the same collision policy and
  suffix handling as equality joins.
- `JoinProjection` centralizes suffix, drop, and collision flags so we do not balloon keyword arguments.

Default suffixes remain `("", "_r")`, matching DuckDB's `_r` convention instead of numeric suffixes. When collisions persist the
join raises unless `project.allow_collisions` is set.

## Natural Subset Control
`exclude_shared` lets callers omit specific shared columns from automatic key inference. Combined with `on`, this lets pipelines
restrict large schemas to a targeted key set without abandoning natural inference entirely. For example:

```python
orders.left_join(
    customers,
    exclude_shared=("updated_at",),
    on=[("customer_id", "id")],
)
```

## Partitioned Join Orchestration
Legacy helpers offered an optional partitioned orchestration API with correctness bugs on LEFT/FULL joins. For this release we
**omit** partitioned execution entirely. The proposal documents that callers must pre-partition upstream (e.g., by materializing
filtered relations) and we revisit a corrected orchestrator in a future milestone once the strict join surface ships.

## Migration Mapping
To keep the proposal grounded in the current code:
- `DuckRel.join` (current helper) → `DuckRel.inner_join` with defaults (`use_shared=True`).
- `DuckRel.left_join` → `DuckRel.left_join` (same name, new parameters). Existing call sites translate their tuple-based keys into
  `on=[(...)]` and rely on shared columns otherwise.
- `DuckRel.join_asof` → `DuckRel.asof_join`. The helper `AsofSpec.from_current_api` adapts the existing keyword arguments to the
  new dataclasses for a smoother upgrade path.
- Tuple-based helpers migrate via `JoinSpec.from_tuples(...)` described below.

We ship deprecation shims that forward legacy signatures to the new methods while emitting `DeprecationWarning` for at least one
release cycle.

## Specification Helpers
Structured specifications remain valuable for reuse and serialization. We retain `JoinSpec`/`AsofSpec` while aligning them with
the new parameter sets:

```python
@dataclass(frozen=True)
class JoinSpec:
    equal_keys: Sequence[tuple[str, str]]
    predicates: Sequence[JoinPredicate] = ()
    exclude_shared: Sequence[str] = ()

    @classmethod
    def from_tuples(
        cls,
        *keys: tuple[str, str],
        predicates: Sequence[JoinPredicate] = (),
        exclude_shared: Sequence[str] = (),
    ) -> "JoinSpec":
        """Adapter for tuple-based joins in today's DuckRel helpers."""
```

```python
@dataclass(frozen=True)
class AsofOrder:
    left: str
    right: str

@dataclass(frozen=True)
class AsofSpec(JoinSpec):
    order: AsofOrder
    direction: Literal["backward", "forward", "nearest"] = "backward"
    tolerance: str | None = None
    nearest_tie: Literal["left", "right"] = "left"
    keep_right_order_col: bool = True

    @classmethod
    def from_current_api(
        cls,
        *,
        keys: Mapping[str, str] | None = None,
        order: AsofOrder,
        direction: Literal["backward", "forward", "nearest"] = "backward",
        tolerance: str | None = None,
    ) -> "AsofSpec":
        """Adapter for today's `DuckRel.join_asof` helper."""
```

These specs feed the same builders as the direct methods so pipelines can predefine join plans, serialize them, or share them
across processes without re-describing each keyword.

## Implementation Notes
- Equality key resolution uses DuckRel's identifier utilities to keep casing and strictness consistent.
- SQL generation prefers `USING` when all equality keys share identical names; otherwise it renders `ON` with explicit qualified
  identifiers. ASOF always renders `ON`.
- Predicate rendering binds `ExpressionPredicate.args` to avoid SQL injection.
- The planner drops right-side key columns after projection unless the projection helper overrides the behavior.
- Semi/anti joins reuse the same key resolution and predicate pipeline but compile to `EXISTS` / `NOT EXISTS` subqueries.
- `cross_join` bypasses key resolution and always errors if callers provide `use_shared` or `on` arguments.

## Test Plan
- **Key resolution**: shared-only, `on`-only, mixed, `exclude_shared`, missing identifiers under `strict=True`/`False`.
- **Projection**: verify right key suppression, suffix application, stability of column order, and `keep_right_order_col` for
  ASOF.
- **Predicate safety**: ensure parameter binding works, unsupported operators raise, and ambiguous identifiers fail validation.
- **ASOF semantics**: backward/forward/nearest (with required tolerance), tie-breaking behavior, unsorted inputs (warning path),
  differing order column names, and `keep_right_order_col=False` override.
- **Semi/anti joins**: confirm existence filtering semantics and interaction with predicates.
- **Regression coverage**: replicate outputs from the current `join`, `left_join`, and `join_asof` helpers under default
  scenarios.

## Documentation Plan
- Update the README join section with examples using the unified signature (shared keys, aliases via `on`, predicate usage).
- Add docstrings to each new method referencing `JoinPredicate`, `JoinSpec`, and `AsofSpec` helpers.
- Provide a cookbook entry demonstrating ASOF joins, including the tolerance requirement for `"nearest"` and tie-breaking.

---
This proposal keeps join usage declarative and self-documenting while addressing the naming, safety, and behavioral gaps raised in
the design review. The resulting API is compact, explicit, and aligned with DuckRel's strict ethos.
