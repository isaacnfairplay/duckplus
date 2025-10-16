# Typed expression API

DuckPlus' typed expression DSL keeps SQL construction readable while preserving
DuckDB semantics. Expressions carry dependency metadata so relation helpers can
validate column references early.

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

## Arithmetic and comparison helpers

Typed expressions implement Python operators. Comparisons return boolean typed
expressions that can be fed into :meth:`Relation.filter
<duckplus.relation.Relation.filter>`.

```python
high_value = amount > 100
promotions = (customer == "prime") | (customer == "enterprise")
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
