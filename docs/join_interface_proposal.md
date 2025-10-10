# DuckRel Join Interface Proposal

## Overview
DuckRel already enforces explicit projection and strict column resolution. Join semantics need the same level of clarity so that
callers can describe how keys line up without falling back to DuckDB defaults. This proposal introduces a structured join API that
uses keyword arguments for aliasing non-shared columns, distinguishes between "natural" (shared column) joins and explicitly
keyed joins, and adds a dedicated surface for ASOF logic.

The guiding principle is that the call signature should make column flow obvious. Developers reading call sites should immediately
see (a) which columns match by name, (b) which columns require aliasing, and (c) whether additional comparison predicates (>, <,
>=, etc.) participate in the join. All of this must compose cleanly with the existing strict-missing behavior and projection rules.

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
These methods leverage shared column names as the baseline keys. Keyword arguments let callers map additional or non-shared
columns without removing the natural behavior. All methods return a new ``DuckRel`` and retain the existing projection rules
(first relation drives column order, right-side key columns are dropped by default).

| Method | Description |
| --- | --- |
| ``DuckRel.natural_inner(other, /, *, strict: bool = True, **key_aliases: str)`` | Inner join on shared column names, optionally mapping additional pairs via ``**key_aliases``. ``strict`` preserves the error-on-missing behavior. |
| ``DuckRel.natural_left(...)`` | Left join counterpart. |
| ``DuckRel.natural_right(...)`` | Right join counterpart (discouraged in pipelines but available). |
| ``DuckRel.natural_full(...)`` | Full outer join. |
| ``DuckRel.natural_asof(...)`` | Time-aware join (see ASOF section). |

**Keyword aliasing**: Passing ``customer_id="customer_identifier"`` means "match ``left.customer_id`` to ``right.customer_identifier``".
If the right-hand column does not exist, the usual identifier resolution error is raised (respecting ``strict``). Alias kwargs are
optional; omitting them reverts to purely shared-column behavior.

**Additional configuration**: We keep suffix handling and duplicate column rules wired through the existing ``JoinProjection`` helper
(the implementation already enforces suffix-based collision avoidance). Those knobs live behind optional keyword-only arguments that
appear before ``**key_aliases`` so they remain discoverable, e.g. ``suffixes`` or ``allow_collisions``.

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
(see **Join Specification Objects** below). These methods never infer keys; the caller must spell out the relationship.

### ASOF joins
ASOF logic requires a hybrid of equality keys and an ordered comparison. We expose two methods:

1. ``DuckRel.natural_asof(other, /, *, order: AsofOrder, direction: AsofDirection = "backward", tolerance: str | None = None, **key_aliases: str)``
2. ``DuckRel.asof_join(other, spec: AsofSpec, /, *, project: JoinProjection | None = None)``

``natural_asof`` mirrors the natural join behavior—shared columns are used automatically and kwargs provide aliases. The caller must
supply an ``AsofOrder`` instance describing the time ordering (e.g., ``AsofOrder(left="event_ts", right="snapshot_ts")``). Optional
``direction`` values (``"backward"``, ``"forward"``, ``"nearest"``) and a DuckDB-style ``tolerance`` string control the match window.

``asof_join`` lets callers provide a full ``AsofSpec`` (equality keys, order pair, direction, optional range predicates). Both variants
return a ``DuckRel`` that maintains the usual projection guarantees.

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
```

Developers who prefer ``**kwargs`` only can stick to the ``natural_*`` methods; those who need advanced logic can build specs
explicitly. ``ExpressionPredicate`` stays as an escape hatch for complex predicates while still enforcing projection control.

## API Examples
- **Natural left join with alias**
  ```python
  rel = orders.natural_left(customers, order_customer_id="customer_id")
  ```

- **Natural join with suffix override**
  ```python
  rel = orders.natural_inner(customers, suffixes=("", "_customer"), status="status_code")
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
  )
  rel = ticks.asof_join(quotes, spec)
  ```

## Implementation Notes
- ``DuckRel`` gains new methods matching the tables above; existing ``inner_join``/``left_join`` helpers can be refactored to use
the ``JoinSpec`` machinery internally for backwards compatibility.
- Keyword aliasing reuses ``util.resolve_columns`` so identifier casing is consistent.
- ``JoinSpec`` and ``AsofSpec`` live in ``duckplus.core`` (or ``duckplus.core.join`` if we factor joins into a helper module) to avoid
  circular imports.
- ``AsofSpec`` compiles to DuckDB's ``ASOF JOIN`` syntax with ``ORDER BY`` and optional ``RANGE`` clauses.
- Tests cover:
  - Natural join behavior with and without kwargs aliases.
  - Error paths when alias columns are missing or collide.
  - ``JoinSpec`` conversions, including ``ColumnPredicate`` rendering.
  - ASOF join correctness (backward, forward, nearest) using small in-memory fixtures.
  - Projection guarantees (right-side key suppression, suffix handling).

## Migration Strategy
1. Add the ``JoinSpec``/``AsofSpec`` types and helper functions (e.g., ``build_join_spec``) without removing existing methods.
2. Implement the new ``natural_*`` and explicit join methods, delegating to existing join builders where possible.
3. Deprecate old method names via ``DeprecationWarning`` while updating internal call sites and tests to the new surface.
4. Remove deprecated names after a release cycle once documentation and integrations have migrated.

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

---
This proposal aims to keep join usage declarative and self-documenting while unlocking advanced time-aware operations without
sacrificing Duck+'s strict defaults.
