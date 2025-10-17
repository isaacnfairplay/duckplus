# Typed expression API

DuckPlus' typed expression DSL keeps SQL construction readable while preserving
DuckDB semantics. Expressions carry dependency metadata so relation helpers can
validate column references early. Unlike plain strings, typed expressions know
their DuckDB logical type, whether they are nullable, and which columns they
depend on. That context powers validation across the rest of the package.

## Creating typed expressions

Use the :mod:`duckplus.typed.ducktype` factory to construct expression objects
based on DuckDB types:

```python
from duckplus.typed import ducktype

amount = ducktype.Numeric("amount")
customer = ducktype.Varchar("customer")
```

Expressions know which columns they depend on. Combining expressions merges
those dependencies so downstream helpers can ensure referenced columns exist.
You can inspect dependencies directly via
:attr:`duckplus.typed.expressions.base.TypedExpression.dependencies` to build
documentation or runtime audits.

## Arithmetic and comparison helpers

Typed expressions implement Python operators. Comparisons return boolean typed
expressions that can be fed into :meth:`Relation.filter
<duckplus.relation.Relation.filter>`.

```python
high_value = amount > 100
promotions = (customer == "prime") | (customer == "enterprise")
discounted = ducktype.Boolean("is_discounted")

eligible = promotions & ~discounted
```

## Aggregation helpers

Aggregation helpers live on the typed expression itself:

```python
summary = base.aggregate(
    ("customer",),
    total_sales=amount.sum(),
    average_sale=amount.avg(),
    largest_sale=amount.max(),
)
```

Aggregations can be filtered using :meth:`duckplus.typed.expressions.base.TypedExpression.filter` and
composed with window functions via :meth:`duckplus.typed.expressions.base.TypedExpression.over`.

## Window functions

Window helpers mirror DuckDB's ``OVER`` clause while keeping dependencies
explicit:

```python
running_total = amount.sum().over(
    partition_by=("customer",),
    order_by=(("order_date", "ASC"),),
)
```

Typed expressions expose ``rows`` and ``range`` framing helpers, as well as
``order`` direction keywords so the DSL stays expressive.

## Conditional expressions and aliases

Use :meth:`duckplus.typed.expressions.base.TypedExpression.alias` to assign an
output name without immediately materialising the expression. Pair it with the
:class:`~duckplus.typed.CaseExpressionBuilder` returned by the varchar factory
to model conditional logic. Remember to call ``end()`` once the branches are
defined:

```python
status = (
    ducktype.Varchar.case()
    .when(ducktype.Boolean("is_returned"), ducktype.Varchar.literal("returned"))
    .else_(ducktype.Varchar.literal("fulfilled"))
    .end()
    .alias("order_status")
)

case = (
    ducktype.Varchar.case()
    .when(amount > 500, ducktype.Varchar.literal("enterprise"))
    .when(amount > 100, ducktype.Varchar.literal("growth"))
    .else_(ducktype.Varchar.literal("starter"))
    .end()
    .alias("segment")
)
```

Both helpers keep dependencies precise, ensuring that relation operations flag a
missing ``amount`` column before executing SQL.

## Function catalogue

DuckPlus exposes curated metadata for DuckDB functions via
:mod:`duckplus.typed.functions`. Each namespace lists the scalar, aggregate, and
window functions the DSL understands, which is useful for editor integrations or
dynamic form builders:

```python
from duckplus.typed import functions

print(functions.SCALAR_FUNCTIONS["lower"].parameters)
```

When the DSL lacks a direct helper for a DuckDB built-in, fall back to the
``raw`` constructor on the appropriate factory to inject SQL while still
benefiting from dependency tracking. For example,
``ducktype.Numeric.raw("great_circle_distance(lat, lon)")`` yields a typed
expression with no dependencies.

## Select builder

The :mod:`duckplus.typed.select` module provides a chainable builder for SELECT
statements. The builder keeps projections, filters, ordering, and window clauses
immutable while validating dependencies just like the relation helpers.

```python
from duckplus.typed import select

query = (
    select.from_relation(base)
    .with_columns(amount, amount.sum().over(partition_by=("customer",)))
    .order_by(("customer", "ASC"))
)
result = query.execute()
```

When combined with :class:`DuckCon`, the builder makes it straightforward to
construct reusable query templates while honouring the open/closed principle:
extending the API involves adding new expression helpers rather than rewriting
existing ones.

## Debugging typed expressions

Every typed expression renders to SQL via ``str(expression)``. Use this to debug
unexpected behaviour or to embed expressions in logs during development. Inspect
``expression.dependencies`` to confirm the referenced columns—values are stored
as :class:`~duckplus.typed.dependencies.ExpressionDependency` objects matching
the checks performed by :class:`Relation`. Keeping the inspection surface area
rich makes it easier to reason about complex queries without leaving Python.
