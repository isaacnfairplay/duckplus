# Function registry

DuckPlus introspects DuckDB's built-in functions and exposes them through typed
wrappers.

## ``DuckDBFunctionDefinition`` and ``DuckDBFunctionSignature``

- ``DuckDBFunctionDefinition`` records the schema name, function name, function
  type (scalar/aggregate/window), return type, parameter types, and optional
  varargs type. ``matches_arity(argument_count)`` verifies that an overload accepts
  a given number of arguments.
- ``DuckDBFunctionSignature`` provides a developer-friendly representation of a
  specific overload. ``call_syntax()`` renders ``NAME(arg_type, ...)`` and
  ``return_annotation()`` renders the DuckDB return type.

## Callable wrappers

``_DuckDBFunctionCall`` (exposed via namespaces) selects the appropriate overload
for supplied operands. It coercively converts arguments into typed expressions,
merges dependencies, and returns the correctly typed result. The ``signatures``
property lists available overloads.

## Namespaces and constants

- ``DuckDBFunctionNamespace`` – root object with ``Scalar``, ``Aggregate``, and
  ``Window`` attributes. Each attribute exposes callables grouped by return
  category. Namespaces support ``dir()`` for discoverability.
- ``SCALAR_FUNCTIONS`` / ``AGGREGATE_FUNCTIONS`` / ``WINDOW_FUNCTIONS`` – module
  level singletons mirroring the namespace attributes. Use them when a lightweight
  registry is preferred over constructing ``DuckDBFunctionNamespace``.

Function callables derive their docstrings from DuckDB metadata, making them easy
to inspect from interactive shells or language servers.
