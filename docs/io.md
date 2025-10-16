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
    header=True,
    delimiter=",",
    quotechar='"',
    escapechar=None,
    sample_size=None,
    auto_detect=True,
    columns=None,
)
```

* **source** – String or `PathLike` pointing at a CSV file or buffer.
* **header** – Treat the first row as column names.
* **delimiter** – Single-character field separator.
* **quotechar** / **escapechar** – Configure quoting behaviour.
* **sample_size** – Rows DuckDB should sample when inferring types.
* **auto_detect** – Whether DuckDB should attempt automatic schema detection.
* **columns** – Optional mapping of column names to DuckDB type strings.

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
