# DuckRel Join Interface Proposal

## Overview
DuckRel already enforces explicit projection and strict column resolution. Join semantics need the same level of clarity so that
callers can describe how keys line up without falling back to DuckDB defaults. This proposal introduces a structured join API that
uses keyword arguments for aliasing non-shared columns, distinguishes between "natural" (shared column) joins and explicitly
keyed joins, and adds a dedicated surface for ASOF logic.

The guiding principle is that the call signature should make column flow obvious. Developers reading call sites should immediately
see (a) which columns match by name, (b) which columns require aliasing, and (c) whether additional comparison predicates (>, <,
>=, etc.) participate in the join. All of this must compose cleanly with the existing strict-missing behavior and projection rules.

## Design Principles
- **Column intent stays local.** Every join method exposes non-shared equality keys as ``**kwargs`` so call sites document aliasing
  inline. Structured predicate objects are used when aliasing alone is insufficient.
- **Method names advertise behavior.** Join flavors are grouped by natural (shared-column) joins, directed joins (explicit specs),
  and ASOF joins to avoid conflating DuckRel semantics with SQL keywords.
- **Strict-by-default continues.** Missing identifiers, casing mismatches, and projection collisions raise unless callers opt
  into relaxed behavior through explicit keyword-only flags.
- **Composable without guesswork.** The same specification types power explicit joins, natural joins with mixed predicates, and
  ASOF joins so that advanced usage does not require bespoke helpers.

## Goals
- Preserve the default behavior where joins on shared column names require no additional configuration.
- Allow callers to opt-in to non-shared equality keys via ``**kwargs`` using the pattern ``left_column=right_column``.
- Surface clear method names that describe the join flavor without mirroring SQL tokens exactly.
- Introduce a structured way to express non-equality predicates (e.g., ``<=`` for ASOF, range bounds) without falling back to raw
  SQL strings.
- Provide a dedicated ASOF join entry point that makes time-order semantics explicit and testable.
- Keep the API discoverable: a small set of methods that hang directly off ``DuckRel``.

## Method Families
### Natural joins (shared column defaults)
Natural joins remain the entry point for "shared column" behavior. Each method shares the same signature shape:

```python
DuckRel.natural_inner(
    other: DuckRel,
    /,
    *,
    strict: bool = True,
    project: JoinProjection | None = None,
    predicates: Sequence[JoinPredicate] | None = None,
    **key_aliases: str,
)
```

``natural_left``, ``natural_right``, ``natural_full``, and ``natural_asof`` follow the same keyword ordering. The positional ``other``
parameter keeps parity with existing DuckRel transforms. ``strict`` defaults to ``True`` to preserve the error-on-missing contract;
callers may opt out for pipelines that need best-effort matching. ``project`` exposes suffix handling, explicit keep/drop toggles,
and other projection-time controls in a single object rather than multiple ad-hoc kwargs.

**Keyword aliasing**: Passing ``customer_id="customer_identifier"`` means "match ``left.customer_id`` to ``right.customer_identifier``".
If the right-hand column does not exist, the usual identifier resolution error is raised (respecting ``strict``). Alias kwargs are
optional; omitting them reverts to purely shared-column behavior. Because the aliases are true keyword arguments, linters and type
checkers surface typos at the call site when ``from __future__ import annotations`` is active.

**Trailing predicate support**: When a caller needs to mix aliasing with additional predicates, the method accepts an optional
``predicates`` keyword-only parameter (``Sequence[JoinPredicate] | None``). This keeps the ergonomics of ``**kwargs`` while making
the presence of non-equality logic explicit and statically validated.

### Directed joins (explicit key sets)
When callers want to control all join keys instead of relying on shared names, we expose explicit methods that accept a
``JoinSpec`` instance. These methods deliberately avoid kwargs aliasing to keep them orthogonal to the natural variants.

| Method | Description |
| --- | --- |
| ``DuckRel.left_inner(other, spec: JoinSpec, /, *, project: JoinProjection | None = None)`` | Specifies exact columns (left and right) plus optional non-equality predicates. |
| ``DuckRel.left_outer(...)`` | Left outer join (same semantics, different null-handling). |
| ``DuckRel.left_right(...)`` | Right join with explicit keys. |
| ``DuckRel.inner_join(...)`` | Symmetric inner join using the same ``JoinSpec``. |
| ``DuckRel.outer_join(...)`` | Full outer join. |

The ``JoinSpec`` type replaces ad-hoc tuple sequences. It captures both equality keys and advanced predicates in a structured form
(see **Join Specification Objects** below). These methods never infer keys; the caller must spell out the relationship. The explicit
specs also act as a shared transport format for serialization (e.g., storing join plans) or cross-process RPC.

### ASOF joins
ASOF logic requires a hybrid of equality keys and an ordered comparison. We expose two methods:

1. ``DuckRel.natural_asof(other, /, *, order: AsofOrder, direction: AsofDirection = "backward", tolerance: str | None = None, **key_aliases: str)``
2. ``DuckRel.asof_join(other, spec: AsofSpec, /, *, project: JoinProjection | None = None)``

``natural_asof`` mirrors the natural join behavior—shared columns are used automatically and kwargs provide aliases. The caller must
supply an ``AsofOrder`` instance describing the time ordering (e.g., ``AsofOrder(left="event_ts", right="snapshot_ts")``). Optional
``direction`` values (``"backward"``, ``"forward"``, ``"nearest"``) and a DuckDB-style ``tolerance`` string control the match window.
Passing ``predicates=...`` works the same way as with the natural join family so callers can supply additional range filters or
disallow ties.

``asof_join`` lets callers provide a full ``AsofSpec`` (equality keys, order pair, direction, optional range predicates). Both variants
return a ``DuckRel`` that maintains the usual projection guarantees. ``AsofSpec`` additionally supports ``range_predicates`` so callers
can constrain either side with relative offsets (e.g., "only match if measurement is within the last hour").

## Validation & Error Handling
- ``strict=True`` keeps the existing error-on-missing behavior. For natural joins this applies both to inferred shared columns and to
  alias kwargs. For explicit specs we validate each identifier during construction so invalid specs fail fast.
- ``JoinPredicate`` objects run through a validator that rejects unsupported operators, ambiguous references (e.g., ``table.col`` when
  only ``col`` is allowed), and non-deterministic expressions. ``ExpressionPredicate`` additionally runs a whitelist-based parser to
  guard against injection.
- ``AsofSpec`` enforces monotonic ordering by checking that the ``order`` columns exist and, when possible, verifying that the left
  relation already carries an ordering marker (DuckRel tracks ``order_by`` metadata). When metadata is absent, DuckRel issues a
  ``UserWarning`` so callers can decide whether to sort explicitly before joining.
- ``range_predicates`` must reference the same columns as the ``order`` pair or equality keys. This avoids surprising "cross-key"
  comparisons that DuckDB would otherwise accept but that complicate mental models.

## Join Specification Objects
### Equality keys via kwargs
``**key_aliases`` always map left-column identifiers (as keyword names) to right-column identifiers (as values). Internally we resolve
the keyword names using the same identifier-normalization utilities used elsewhere, so casing and quoting rules stay consistent.
Because the kwargs live at the call site, the codebase advertises the column relationships directly:

```python
orders.natural_left(customers, order_customer_id="customer_id")
```

### Structured predicates
To describe non-equality comparisons (both for regular joins and ASOF joins), we introduce the following dataclasses in
``duckplus.core``:

```python
@dataclass(frozen=True)
class ColumnPredicate:
    left: str
    operator: Literal["=", "!=", "<", "<=", ">", ">="]
    right: str

@dataclass(frozen=True)
class ExpressionPredicate:
    expression: str  # validated SQL fragment scoped to the join

JoinPredicate = ColumnPredicate | ExpressionPredicate
```

``JoinSpec`` collects equality keys, kwargs aliases, and optional ``JoinPredicate`` items:

```python
@dataclass(frozen=True)
class JoinSpec:
    equal_keys: Sequence[tuple[str, str]]
    predicates: Sequence[JoinPredicate] = ()

    @classmethod
    def from_tuples(
        cls,
        *keys: tuple[str, str],
        predicates: Sequence[JoinPredicate] | None = None,
    ) -> "JoinSpec":
        """Helper for migrating tuple-based join definitions."""
```

``AsofSpec`` extends this with an ``order`` pair and ASOF-specific parameters:

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
    range_predicates: Sequence[JoinPredicate] = ()

    @classmethod
    def from_current_api(
        cls,
        *,
        keys: Mapping[str, str] | None = None,
        order: AsofOrder,
        direction: Literal["backward", "forward", "nearest"] = "backward",
        tolerance: str | None = None,
    ) -> "AsofSpec":
        """Adapter for the ``DuckRel.join_asof`` helper that ships today."""
```

> ``Mapping`` and ``Sequence`` refer to ``collections.abc`` / ``typing`` imports; the implementation uses ``collections.abc`` to
> avoid runtime typing overhead.

Developers who prefer ``**kwargs`` only can stick to the ``natural_*`` methods; those who need advanced logic can build specs
explicitly. ``ExpressionPredicate`` stays as an escape hatch for complex predicates while still enforcing projection control.
``JoinSpec``/``AsofSpec`` also double as documentation artifacts: a pipeline can log or serialize them so reviewers know exactly
which predicates applied.

### Why structured predicates instead of trailing SQL strings?
- **Static analysis**: dataclasses integrate with type checkers and IDEs, preventing subtle bugs that raw SQL strings might hide.
- **Consistent validation**: the implementation can reuse DuckRel's identifier resolution and quoting rules, guaranteeing
  case-insensitive matching without manual quoting.
- **Composable**: Specs can be programmatically merged, filtered, or transformed (e.g., injecting audit predicates) before running
  the join—difficult to do with free-form strings.
- **Escape hatch still available**: ``ExpressionPredicate`` holds a validated fragment for scenarios that genuinely require
  arbitrary expressions. Trailing SQL strings would force us to pick between permissive-but-unsafe or restrictive-but-brittle.

### Mapping to previous join helpers
- ``DuckRel.join`` (current helper) → ``DuckRel.natural_inner`` or ``DuckRel.inner_join`` depending on whether callers relied on shared
  columns or explicit tuples.
- ``DuckRel.join_asof`` (current helper) → ``DuckRel.natural_asof`` with identical kwargs plus ``predicates`` for custom ranges.
- ``DuckRel.left_join`` → ``DuckRel.natural_left`` (shared columns) or ``DuckRel.left_outer`` (explicit spec). We maintain shim
  methods that delegate and emit deprecation warnings for at least one release.
- ``DuckRel.join_on`` (tuple-based) → ``JoinSpec.from_tuples(...)`` to ease migration. The helper returns a ``JoinSpec`` so callers
  can pass it to any of the explicit join methods.

## API Examples
- **Natural left join with alias**
  ```python
  rel = orders.natural_left(customers, order_customer_id="customer_id")
  ```

- **Natural join with suffix override**
  ```python
  rel = orders.natural_inner(customers, suffixes=("", "_customer"), status="status_code")
  ```

- **Natural join with additional predicates**
  ```python
  rel = orders.natural_left(
      customers,
      status="status_code",
      predicates=[ColumnPredicate("order_date", ">=", "customer_since")],
  )
  ```

- **Explicit inner join with predicates**
  ```python
  spec = JoinSpec(equal_keys=[("order_id", "id")], predicates=[ColumnPredicate("order_date", ">=", "customer_since")])
  rel = orders.inner_join(customers, spec)
  ```

- **ASOF join**
  ```python
  rel = ticks.natural_asof(quotes, order=AsofOrder(left="event_ts", right="quote_ts"), symbol="symbol")
  ```

  or using the explicit form:

  ```python
  spec = AsofSpec(
      equal_keys=[("symbol", "symbol")],
      order=AsofOrder(left="event_ts", right="quote_ts"),
      direction="nearest",
      tolerance="5 seconds",
      range_predicates=[ColumnPredicate("event_ts", ">=", "quote_ts")],
  )
  rel = ticks.asof_join(quotes, spec)
  ```

## Implementation Notes
- ``DuckRel`` gains new methods matching the tables above; existing ``inner_join``/``left_join`` helpers can be refactored to use
  the ``JoinSpec`` machinery internally for backwards compatibility.
- Keyword aliasing reuses ``util.resolve_columns`` so identifier casing is consistent.
- ``JoinSpec`` and ``AsofSpec`` live in ``duckplus.core`` (or ``duckplus.core.join`` if we factor joins into a helper module) to avoid
  circular imports.
- ``AsofSpec`` compiles to DuckDB's ``ASOF JOIN`` syntax with ``ORDER BY`` and optional ``RANGE`` clauses, and gracefully degrades to
  an error when DuckDB returns ``NOT IMPLEMENTED`` for unsupported combinations.
- Tests cover:
  - Natural join behavior with and without kwargs aliases, including multiple aliases and case-folded identifiers.
  - ``predicates`` plumbing on natural joins to ensure order and alias resolution remain deterministic.
  - Error paths when alias columns are missing or collide.
  - ``JoinSpec`` conversions, including ``ColumnPredicate`` rendering and ``ExpressionPredicate`` validation.
  - ASOF join correctness (backward, forward, nearest) using small in-memory fixtures with deterministic timestamps.
  - Projection guarantees (right-side key suppression, suffix handling).
  - Serialization round-trips of ``JoinSpec``/``AsofSpec`` to ensure reproducible plans.

## Migration Strategy
1. Add the ``JoinSpec``/``AsofSpec`` types and helper functions (e.g., ``build_join_spec``) without removing existing methods.
2. Implement the new ``natural_*`` and explicit join methods, delegating to existing join builders where possible.
3. Deprecate old method names via ``DeprecationWarning`` while updating internal call sites and tests to the new surface. We ship an
   ``explain_join`` helper that prints the structured spec so downstream teams can map old method names to new ones.
4. Provide ``JoinSpec.from_tuples`` and ``AsofSpec.from_current_api`` helpers during the migration window so callers can move piecemeal.
5. Remove deprecated names after a release cycle once documentation and integrations have migrated.

## Documentation Plan
- Update the README join section with examples mirroring those above.
- Add docstrings to each new method referencing ``JoinSpec`` and ``AsofSpec`` usage.
- Provide a cookbook entry demonstrating ASOF joins in both natural and explicit forms.

## Open Questions
- Should ``ExpressionPredicate`` accept parameter binding to prevent SQL injection in user-provided fragments? If so, we can mirror
  the ``DuckRel.filter`` API by accepting ``args`` alongside the expression.
- Do we need an ergonomic helper for building ``JoinSpec`` from ``**kwargs`` when consumers want to mix aliasing with additional
  predicates outside the natural join family?
- What default tolerance should ASOF joins assume when ``direction="nearest"``? DuckDB requires explicit tolerance for "nearest";
  we may enforce that in the ``AsofSpec`` constructor.
- Should ``JoinSpec`` expose a ``from_kwargs`` constructor that mirrors the natural join signature for users who prefer symmetric
  building blocks?

---
This proposal aims to keep join usage declarative and self-documenting while unlocking advanced time-aware operations without
sacrificing Duck+'s strict defaults.
