# File append helpers

DuckPlus wraps DuckDB's file-writer APIs so append workflows stay safe and
predictable. Because the helpers operate on immutable relations, you can rehearse
the exact rows that will be written, validate schemas, and even dry-run the
transforms using the sampling utilities described in
{doc}`../core/relations`.

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
before writing new rows. Pair the helper with
:meth:`Relation.null_ratios <duckplus.relation.Relation.null_ratios>` to detect
unexpected null patterns prior to exporting.

```python
if relation.null_ratios()["total"] > 0:
    raise ValueError("Totals cannot contain nulls before export")
```

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
connection. When ``unique_id_column`` is provided, DuckPlus performs an anti-join
against the target file to skip rows that already exist—ideal when you are
reprocessing incremental extracts. Set ``match_all_columns=True`` to compare the
entire row structure instead.

## Table utilities

Managed tables share the same API guarantees. See {mod}`duckplus.table` for
helpers that create or overwrite DuckDB tables while verifying schema metadata.
These utilities lean on the same immutability principles, keeping the API open
for future extension without breaking callers.

### Incremental refresh pattern

Combine the file append helpers with :class:`duckplus.table.Table` to create an
end-to-end incremental refresh pipeline:

1. Ingest raw files with {mod}`duckplus.io` readers.
2. Apply typed-expression transforms and validations.
3. Write the curated slice to Parquet with ``append_parquet``.
4. Insert the same rows into a DuckDB table using
   :meth:`duckplus.table.Table.insert`, enabling fast local analytics.

Because each step works with immutable relations, you can add assertions after
every operation without worrying about accidental mutation.
