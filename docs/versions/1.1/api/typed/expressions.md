# Expressions and builders

Typed expressions pair SQL fragments with DuckDB type metadata and dependency
tracking. The classes live under :mod:`duckplus.typed.expressions` and are
re-exported from :mod:`duckplus.typed`.

## ``TypedExpression``

Base class that stores ``render()`` SQL output, a ``duck_type`` instance, and a
``dependencies`` frozenset. Key methods:

- ``alias(alias)`` – return an :class:`AliasedExpression` wrapping the expression.
- ``clone_with_sql(sql, *, dependencies)`` – produce a new expression of the same
  class.
- Comparison operators (``==``, ``!=``) – coerce other expressions or literals
  into :class:`BooleanExpression` instances.
- ``over(partition_by=None, order_by=None, frame=None)`` – render a window clause
  while merging dependencies.

## ``AliasedExpression``

Adapter that preserves metadata while rendering ``<expression> AS <alias>``. The
``over`` and comparison helpers delegate to the underlying expression so chaining
continues to work.

## ``BooleanExpression``

Represents boolean SQL fragments.

- Logical operators ``&``/``|``/``~`` compose expressions with ``AND``, ``OR``,
  and ``NOT``.
- ``column(name, *, table=None)`` – reference a boolean column with dependency
  tracking.
- ``literal(value)`` – produce ``TRUE`` or ``FALSE`` literals.
- ``raw(sql, *, dependencies=())`` – wrap raw SQL while providing optional
  dependencies.

## ``GenericExpression``

Covers SQL fragments whose concrete type is unknown. Provides ``column`` and
``raw`` constructors plus ``max_by(order)`` for aggregate selection.

## ``NumericExpression``

Represents numeric expressions and overloads arithmetic operators (``+``, ``-``,
``*``, ``/``, ``%``, ``**``). Additional helpers:

- ``column(name, *, table=None)`` and ``literal(value)`` – reference numeric
  inputs with type inference for literals.
- ``raw(sql, *, dependencies=(), duck_type=None)`` – wrap custom SQL.
- ``sum()`` / ``avg()`` – delegate to DuckDB aggregate functions.

``NumericFactory`` builds numeric expressions and exposes:

- ``__call__(column, *, table=None)`` – column accessor.
- ``literal(value)`` / ``raw(sql, ...)`` – factory variants.
- ``coerce(operand)`` – convert column names, tuples, or numeric literals into
  expressions.
- ``case()`` – return a :class:`CaseExpressionBuilder` preconfigured for numeric
  results.
- ``Aggregate`` – property exposing ``NumericAggregateFactory`` for function
  lookups.

``NumericAggregateFactory`` bridges to generated DuckDB function registries. When
DuckDB metadata is available, attributes such as ``.sum`` or ``.avg`` call the
corresponding overloads.

## Text and binary expressions

- ``VarcharExpression`` mirrors the numeric helpers for string data. Use
  ``VarcharFactory`` to construct columns, literals, raw expressions, and CASE
  builders.
- ``BlobExpression`` and ``BlobFactory`` cover binary data.
- ``GenericFactory`` creates ``GenericExpression`` instances when type information
  is unavailable.
- ``BooleanFactory`` produces ``BooleanExpression`` instances and offers
  ``coerce`` helpers for CASE builders.

## ``CaseExpressionBuilder``

Fluent builder with ``when(condition, result)``, ``else_(result)``, and ``end()``.
Conditions and result operands are coerced through the factories supplied by the
calling namespace (for example ``ducktype.Numeric.case()``). The builder prevents
multiple ``ELSE`` clauses and disallows reuse after ``end()``.

## ``SelectStatementBuilder``

Constructs ``SELECT`` lists without manual string formatting.

- ``column(expression, *, alias=None, if_exists=False)`` – append a column or
  expression. ``if_exists`` requires typed expressions with dependency metadata
  and skips the column when prerequisites are absent.
- ``star(*, exclude=None, replace=None, exclude_if_exists=None, replace_if_exists=None)`` –
  render ``*`` with optional ``REPLACE``/``EXCLUDE`` clauses. ``*_if_exists``
  variants include clauses only when the required columns are present.
- ``from_(source)`` – set the ``FROM`` clause when building full statements.
- ``build(*, available_columns=None)`` – render a complete ``SELECT`` statement,
  including the ``FROM`` clause when defined.
- ``build_select_list(*, available_columns=None)`` – render only the ``SELECT``
  list for use with :meth:`duckplus.relation.Relation.project`.

The builder raises ``RuntimeError`` when mutated after finalisation to prevent
accidental reuse.

## ``DuckTypeNamespace``

``ducktype`` provides a convenient namespace:

- ``ducktype.Numeric("column")`` / ``ducktype.Boolean(...)`` / ``ducktype.Varchar(...)``
  – factory shortcuts.
- ``ducktype.Functions`` – instance of :class:`DuckDBFunctionNamespace` exposing
  scalar, aggregate, and window function registries.
- ``ducktype.select()`` – return a fresh :class:`SelectStatementBuilder`.
