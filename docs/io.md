# I/O Helpers

The `duckplus.io` module provides thin wrappers around DuckDB's file readers
that integrate with :class:`duckplus.DuckCon`. Each helper expects an open
`DuckCon` context and returns an immutable :class:`duckplus.Relation` that keeps
a reference to the managing connection.

Every reader exposes its full keyword signature directly in Python so editors
and type checkers surface the supported options. The ``duckcon`` manager and
``source`` parameters can also be supplied by keyword to make call sites more
descriptive when desired.

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

```python
manager = DuckCon()
with manager:
    relation = io.read_csv(
        manager,
        "data.csv",
        header=True,
        delimiter=",",
        na_values=["NA", ""],
    )
```
All arguments are keyword-only apart from the connection and source, which may
also be passed by keyword. The explicit signature keeps IDE completions in sync
with the implementation and provides quick access to aliases such as
``delim``/``delimiter``.

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
    union_by_name=None,
    compression=None,
)
```

These options map directly to DuckDB's [`read_parquet`](https://duckdb.org/docs/data/parquet)
table function. The ``source`` may be a single path or a sequence of paths/globs,
and explicit keyword-only arguments ensure unexpected options are surfaced at
call time instead of being silently ignored. The positional ``duckcon`` and
``source`` parameters can similarly be passed by keyword if preferred.
Only explicitly provided keyword arguments are forwarded so callers can rely on
IDE completions.

```python
manager = DuckCon()
with manager:
    relation = io.read_parquet(
        manager,
        ["part-*.parquet"],
        union_by_name=True,
        filename=True,
    )
```

* **binary_as_string** – Cast DuckDB's binary columns to strings when `True`.
* **file_row_number** – Include a running row number for each file when set.
* **filename** – Append the source filename (absolute path) when enabled.
* **hive_partitioning** – Interpret partitioned directory structures.
* **union_by_name** – Align schemas with non-matching column order.
* **compression** – Override DuckDB's decompression behaviour.

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
inputs. ``columns`` accepts the same mappings and sequences that DuckDB's
``read_json`` table function understands and is normalised to built-in
containers before forwarding. As with the CSV and Parquet helpers, only provided
keyword arguments are forwarded to DuckDB to avoid masking typos.
Passing ``duckcon``/``source`` by keyword is also supported so pipelines can
highlight the data source being loaded inline.

```python
manager = DuckCon()
with manager:
    relation = io.read_json(
        manager,
        "events.json",
        columns={"payload": "STRUCT"},
        records=True,
        maximum_depth=5,
    )
```

* **records** – Control whether the input is JSON Lines (`True`) or arrays/objects.
* **format** – Explicitly declare a format variant DuckDB should expect.
* **convert_strings_to_integers** – Enable automatic integer coercion.
* **maximum_* options** – Tune sampling depth, object size, and file counts.
* **hive_types** / **hive_types_autocast** – Normalise Hive-partitioned datasets.

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
