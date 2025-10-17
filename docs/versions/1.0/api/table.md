# ``duckplus.table``

The :mod:`duckplus.table` module provides a :class:`Table` dataclass that binds a
DuckDB table name to a :class:`duckplus.duckcon.DuckCon`. It offers thin wrappers
around DuckDB's insert primitives while enforcing that data originates from the
same managed connection.

## ``Table``

``Table(duckcon, name)`` stores the owning manager and table identifier.

- ``insert(relation, *, target_columns=None, create=False, overwrite=False)`` –
  insert rows from a :class:`duckplus.relation.Relation`. DuckPlus validates that
  the relation was produced by the same ``DuckCon`` and optionally restricts the
  target column list. ``create=True`` allows creating the table on the fly and
  ``overwrite=True`` truncates existing data before inserting.
- ``insert_relation(relation, *, target_columns=None, create=False, overwrite=False)`` –
  variant that accepts a raw ``duckdb.DuckDBPyRelation`` instead of a DuckPlus
  relation.

Both helpers rely on :mod:`duckplus._table_utils` for column normalisation and
share consistent error messages for missing connections or column mismatches.
