# Typed expression quick reference

This page summarises the Protocols, factories, and helper casts that ship with
DuckPlus 2.0. Use it as a checklist when annotating expressions so static
analysers understand the runtime surface installed by ``ExprMeta``.

## Protocols

| Protocol | Description | Key methods |
| --- | --- | --- |
| ``StringExprProto`` | String-focused helpers attached to ``VarcharExpr`` and compatible types. | ``strip``, ``lower``, ``upper``, ``split_part``, ``replace`` |
| ``NumericExprProto`` | Numeric calculations and aggregates exposed on ``IntegerExpr`` values. | ``abs``, ``round``, ``floor``, ``ceil`` |
| ``BooleanExprProto`` | Logical combinators that return boolean predicates. | ``and_``, ``or_``, ``not_``, ``if_else`` |
| ``TemporalExprProto`` | Timestamp/date helpers for truncation, extraction, and formatting. | ``date_trunc``, ``extract``, ``strftime`` |

Each Protocol returns Protocol-compatible types so chained expressions continue
to type check. Annotate variables directly or intersect concrete expression
classes with the Protocol if you need both behaviours.

## Factories and casts

| Helper | Purpose | Example |
| --- | --- | --- |
| ``duckplus.typed.varchar(name: str)`` | Build a ``VarcharExpr`` referencing a column. | ``varchar("customer_name")`` |
| ``duckplus.typed.integer(name: str)`` | Build an integer expression. | ``integer("age")`` |
| ``duckplus.typed.timestamp(name: str)`` | Build a temporal expression. | ``timestamp("created_at")`` |
| ``duckplus.typed.predicate(name: str)`` | Reference a boolean predicate column. | ``predicate("is_active")`` |
| ``duckplus.typed.as_string_proto(expr)`` | View any compatible value as ``StringExprProto``. | ``as_string_proto(varchar("sku"))`` |
| ``duckplus.typed.as_numeric_proto(expr)`` | View a numeric value as ``NumericExprProto``. | ``as_numeric_proto(integer("count"))`` |
| ``duckplus.typed.as_boolean_proto(expr)`` | View a predicate as ``BooleanExprProto``. | ``as_boolean_proto(predicate("ready"))`` |
| ``duckplus.typed.as_temporal_proto(expr)`` | View a temporal value as ``TemporalExprProto``. | ``as_temporal_proto(timestamp("created_at"))`` |

### Putting it together

```python
from duckplus.typed import (
    as_boolean_proto,
    as_numeric_proto,
    as_string_proto,
    as_temporal_proto,
    integer,
    predicate,
    timestamp,
    varchar,
)
from duckplus.typed.protocols import (
    BooleanExprProto,
    NumericExprProto,
    StringExprProto,
    TemporalExprProto,
)

name: StringExprProto = as_string_proto(varchar("name"))
age: NumericExprProto = as_numeric_proto(integer("age"))
created: TemporalExprProto = as_temporal_proto(timestamp("created_at"))
active: BooleanExprProto = as_boolean_proto(predicate("active"))

# Chained helpers stay fully typed
cutoff: TemporalExprProto = created.date_trunc("day")
name_length = name.length()
active_and_recent = active.and_(cutoff.greater(timestamp("2024-01-01")))
```

Refer back to :doc:`typed_expressions` for a deeper discussion of how Protocols
and the metaclass interact.
