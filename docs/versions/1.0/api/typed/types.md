# Type system

DuckPlus models DuckDB types as lightweight Python objects. Each type renders its
SQL representation and supports structural comparison.

## Core class

- ``DuckDBType`` – abstract base with ``render()``, ``describe()``, ``key()``, and
  ``accepts(candidate)``. Subclasses override these methods to encode type
  semantics. Instances are hashable and printable for debugging.

## Scalar types

- ``GenericType(name)`` – catch-all type that accepts any candidate.
- ``BooleanType(name="BOOLEAN")`` – boolean family; equality depends on the type
  name.
- ``VarcharType(name="VARCHAR")`` – string data.
- ``BlobType(name="BLOB")`` – binary payloads.
- ``NumericType(name)`` – numeric family with relaxed ``accepts`` rules for
  ``NUMERIC``.
- ``IntegerType(name)`` – numeric subtype that accepts other integer widths and
  ``NUMERIC`` when compatible.
- ``IntervalType(name="INTERVAL")`` – interval data.
- ``TemporalType(name)`` – date/time families (``DATE``, ``TIMESTAMP``, etc.).
- ``IdentifierType(name)`` – identifier-like data such as ``UUID``.
- ``DecimalType(precision, scale)`` – renders ``DECIMAL(p, s)`` and stores
  precision metadata.
- ``UnknownType()`` – placeholder when metadata is unavailable; accepts all
  candidates.

## Collection types

- ``ListType(element_type)`` – ``LIST(<type>)`` wrappers.
- ``ArrayType(element_type, length=None)`` – ``ARRAY`` with optional fixed length.
- ``MapType(key_type, value_type)`` – ``MAP`` of key/value types.
- ``UnionType(options)`` – ``UNION`` of alternative types.
- ``StructField(name, field_type)`` – field descriptor used by ``StructType``.
- ``StructType(fields)`` – ``STRUCT`` composed of ``StructField`` entries.
- ``EnumType(values)`` – ``ENUM`` with explicit string members.

Each collection type implements ``render()``, ``describe()``, and ``key()`` so they
can participate in equality comparisons and ``accepts`` checks.

## Parsing and inference helpers

- ``parse_type(text)`` – parse a DuckDB type string into the corresponding
  ``DuckDBType`` instance.
- ``infer_numeric_literal_type(value)`` – inspect Python numeric literals to choose
  an appropriate DuckDB numeric type.

Use these utilities to convert metadata returned by DuckDB into structured
objects that integrate with typed expressions and dependency tracking.
