# I/O Helpers

The `duckplus.io` module provides thin wrappers around DuckDB's file readers
that integrate with :class:`duckplus.DuckCon`. Each helper expects an open
`DuckCon` context and returns an immutable :class:`duckplus.Relation` that keeps
a reference to the managing connection.

```python
from duckplus import DuckCon, io

manager = DuckCon()
with manager:
    relation = io.read_csv(manager, "data.csv")
    print(relation.columns)
```

## CSV

```
io.read_csv(
    duckcon,
    source,
    *,
    header=None,
    delimiter=",",
    quotechar='"',
    escapechar=None,
    sample_size=None,
    auto_detect=None,
    columns=None,
    dtype=None,
    names=None,
    na_values=None,
    null_padding=None,
    force_not_null=None,
    files_to_sniff=None,
    decimal=None,
    date_format=None,
    timestamp_format=None,
    encoding=None,
    compression=None,
    hive_types_autocast=None,
    all_varchar=None,
    hive_partitioning=None,
    comment=None,
    max_line_size=None,
    store_rejects=None,
    rejects_table=None,
    rejects_limit=None,
    rejects_scan=None,
    union_by_name=None,
    filename=None,
    normalize_names=None,
    ignore_errors=None,
    allow_quoted_nulls=None,
    auto_type_candidates=None,
    parallel=None,
    skiprows=None,
)
```

* **source** – String or `PathLike` pointing at a CSV file or buffer.
* **header** – Treat the first row as column names.
* **delimiter** – Single-character field separator (aliases: `delim`).
* **quotechar** / **escapechar** – Configure quoting behaviour (aliases: `quote`, `escape`).
* **columns** / **dtype** – Optional mappings describing column types.
* **names** / **na_values** – Override column names or provide strings considered nulls.
* **filename** – Append the originating filename (as an absolute path) as an additional column when `True`.
* Remaining keywords mirror DuckDB's `read_csv` table function and are forwarded
  explicitly so IDEs surface the available options. Aliases raise a descriptive
  `ValueError` when conflicting values are supplied.

## Parquet

```
io.read_parquet(
    duckcon,
    source,
    *,
    binary_as_string=None,
    file_row_number=None,
    filename=None,
    hive_partitioning=None,
    columns=None,
)
```

These options map directly to DuckDB's [`read_parquet`](https://duckdb.org/docs/data/parquet)
table function. Only explicitly provided keyword arguments are forwarded so
callers can rely on IDE completions.

## JSON

```
io.read_json(
    duckcon,
    source,
    *,
    columns=None,
    sample_size=None,
    maximum_depth=None,
    records=None,
    format=None,
    date_format=None,
    timestamp_format=None,
    compression=None,
    maximum_object_size=None,
    ignore_errors=None,
    convert_strings_to_integers=None,
    field_appearance_threshold=None,
    map_inference_threshold=None,
    maximum_sample_files=None,
    filename=None,
    hive_partitioning=None,
    union_by_name=None,
    hive_types=None,
    hive_types_autocast=None,
)
```

JSON helpers are flexible enough to cover both JSON Lines and nested JSON
inputs. As with the CSV and Parquet helpers, only provided keyword arguments are
forwarded to DuckDB to avoid masking typos.

## CSV appenders

```
io.append_csv(
    duckcon,
    table,
    source,
    *,
    target_columns=None,
    create=False,
    overwrite=False,
    header=None,
    delimiter=",",
    quotechar='"',
    escapechar=None,
    sample_size=None,
    auto_detect=None,
    columns=None,
    dtype=None,
    names=None,
    na_values=None,
    null_padding=None,
    force_not_null=None,
    files_to_sniff=None,
    decimal=None,
    date_format=None,
    timestamp_format=None,
    encoding=None,
    compression=None,
    hive_types_autocast=None,
    all_varchar=None,
    hive_partitioning=None,
    comment=None,
    max_line_size=None,
    store_rejects=None,
    rejects_table=None,
    rejects_limit=None,
    rejects_scan=None,
    union_by_name=None,
    filename=None,
    normalize_names=None,
    ignore_errors=None,
    allow_quoted_nulls=None,
    auto_type_candidates=None,
    parallel=None,
    skiprows=None,
)
```

* **table** – Target DuckDB table (optionally schema-qualified) to receive rows.
* **target_columns** – Optional list of column names to insert into, allowing
  tables with defaults to be populated without explicitly projecting every
  field.
* **create** / **overwrite** – Control whether the helper should create the
  table when it does not exist or replace its current contents before
  inserting.
* Remaining keyword arguments mirror :func:`io.read_csv`, including the alias
  handling rules described above.

The helper registers a temporary view for the source relation so the same
connection can execute the necessary ``INSERT`` or ``CREATE TABLE AS SELECT``
statement safely.

## NDJSON appenders

```
io.append_ndjson(
    duckcon,
    table,
    source,
    *,
    target_columns=None,
    create=False,
    overwrite=False,
    columns=None,
    sample_size=None,
    maximum_depth=None,
    records="auto",
    format=None,
    date_format=None,
    timestamp_format=None,
    compression=None,
    maximum_object_size=None,
    ignore_errors=None,
    convert_strings_to_integers=None,
    field_appearance_threshold=None,
    map_inference_threshold=None,
    maximum_sample_files=None,
    filename=None,
    hive_partitioning=None,
    union_by_name=None,
    hive_types=None,
    hive_types_autocast=None,
)
```

By default the NDJSON helper processes one JSON object per line (``records="auto"``)
before delegating to the same insertion pipeline as the CSV variant. All
keyword arguments are forwarded explicitly to DuckDB's ``read_json`` table
function so schema hints and parsing options remain discoverable in IDEs.
