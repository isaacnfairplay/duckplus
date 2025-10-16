# File append helpers

DuckPlus wraps DuckDB's file-writer APIs so append workflows stay safe and
predictable.

## CSV appends

:meth:`duckplus.relation.Relation.append_csv` appends rows to an existing CSV
file. The helper enforces schema compatibility and offers opt-in deduplication:

```python
relation.append_csv(
    "reports/output.csv",
    mode="append",
    match_all_columns=True,
)
```

Enabling ``match_all_columns`` ensures duplicate rows are removed based on all
columns before writing. When ``mode="overwrite"`` the helper truncates the file
before writing new rows.

## Parquet appends

Parquet writes require rewriting files safely. DuckPlus handles this by writing
to a temporary file and replacing the target atomically.

```python
relation.append_parquet(
    "warehouse/sales.parquet",
    mode="append",
    unique_id_column="id",
)
```

The helper supports both ``append`` and ``overwrite`` modes, validates columns
up front, and surfaces helpful errors when the relation originates from a closed
connection.

## Table utilities

Managed tables share the same API guarantees. See {mod}`duckplus.table` for
helpers that create or overwrite DuckDB tables while verifying schema metadata.
These utilities lean on the same immutability principles, keeping the API open
for future extension without breaking callers.
