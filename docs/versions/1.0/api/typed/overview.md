# Overview

Importing :mod:`duckplus.typed` exposes the following symbols:

- ``TypedExpression`` – base class for typed SQL fragments.
- ``BooleanExpression`` / ``NumericExpression`` / ``VarcharExpression`` /
  ``BlobExpression`` / ``GenericExpression`` – concrete expression categories with
  convenience operators and casting helpers.
- ``AliasedExpression`` – wrapper that pairs an expression with a column alias.
- ``CaseExpressionBuilder`` – fluent ``CASE`` builder with ``when`` and ``else_``
  clauses that return typed expressions.
- ``SelectStatementBuilder`` – helper for constructing ``SELECT`` lists that
  respect typed expressions.
- ``ducktype`` – namespace for expression factories (e.g. ``ducktype.Numeric("column")``).
- ``ExpressionDependency`` – frozen dataclass for dependency metadata with
  ``column(name, *, table=None)`` and ``table(name)`` constructors.
- ``DuckDBFunctionNamespace`` / ``DuckDBFunctionSignature`` – descriptors used to
  document DuckDB scalar, aggregate, and window functions.
- ``SCALAR_FUNCTIONS`` / ``AGGREGATE_FUNCTIONS`` / ``WINDOW_FUNCTIONS`` – mappings
  of DuckDB function names to their signatures grouped by namespace.

The following pages dive into expression methods, function registries, and the
DuckDB type system that powers expression validation.
