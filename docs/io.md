# I/O Helpers

> **Note**
> The actively maintained documentation now lives under
> {doc}`versions/1.3/io/overview <versions/1.3/io/overview>` and
> {doc}`versions/1.3/io/file_append <versions/1.3/io/file_append>` in the versioned
> Sphinx site. This file remains for quick reference while older links migrate.

The `duckplus.io` module provides thin wrappers around DuckDB's file readers
that integrate with :class:`duckplus.DuckCon`. Each helper expects an open
`DuckCon` context and returns an immutable :class:`duckplus.Relation` that keeps
a reference to the managing connection.

```{note}
DuckPlus helpers register through direct Python imports. Decorating a function
with :func:`duckplus.io.duckcon_helper` attaches it to
``duckplus.duckcon.DuckCon`` when the module imports, so the fluent API stays in
sync with the shipped Python objects—no runtime registry lookups or
``**kwargs`` funnels.
```

Because helpers are attached at import time, calling
``manager.read_csv(...)`` or ``manager.apply_helper("read_excel", ...)`` works
without importing ``duckplus.io`` explicitly.

Every reader exposes its full keyword signature directly in Python so editors
and type checkers surface the supported options. The ``duckcon`` manager and
``source`` parameters can also be supplied by keyword to make call sites more
descriptive when desired.

```python
from pathlib import Path

from duckplus import DuckCon

manager = DuckCon()
with manager:
    relation = manager.read_csv(Path("data.csv"))
    print(relation.columns)
```

Because the helpers register automatically, persisting results is equally
straightforward via the relation-level file writers:

```python
with manager:
    relation = manager.read_parquet(Path("data.parquet"))
    relation.append_csv(Path("report.csv"))
    relation.write_parquet_dataset(
        Path("dataset"),
        partition_column="country",
    )
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

* **source** – ``pathlib.Path`` instance or sequence of ``Path`` objects pointing at CSV files or buffers.
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

**Example**

```python
relation = io.read_csv(
    manager,
    Path("transactions.csv"),
    delimiter="|",
    header=True,
    na_values=["NA", ""],
    filename=True,
)
```

Passing the alias keywords (`delim`, `quote`, `escape`, etc.) behaves identically
to their primary counterparts so long as only one spelling is used for each
option. Attempting to provide both will raise an error, making it obvious when a
typo slipped through autocomplete suggestions.

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
    directory=False,
    partition_id_column=None,
    partition_glob="*.parquet",
)
```

These options map directly to DuckDB's [`read_parquet`](https://duckdb.org/docs/data/parquet)
table function. The ``source`` may be a single ``Path`` or a sequence of ``Path``
objects/globs. Enabling ``directory=True`` treats ``source`` as a folder and
loads every file matching ``partition_glob`` (``"*.parquet"`` by default).
Only explicitly provided keyword arguments are forwarded so callers can rely on
IDE completions.

**Example**

```python
relation = io.read_parquet(
    manager,
    [Path("/data/sales_2024.parquet"), Path("/data/sales_2025.parquet")],
    filename=True,
    union_by_name=True,
)
```

Because the signature is spelled out explicitly, editors can suggest the
available options—such as `binary_as_string` for forcing binary columns into
`VARCHAR`—without resorting to `**kwargs` guesswork.
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
* **directory** – When `True`, scan a directory instead of a single file and
  apply ``partition_glob`` to collect matching inputs.
* **partition_id_column** – Add a derived column populated with each file's
  stem. The helper automatically enables ``filename=True`` so provenance
  remains accessible alongside the partition key.
* **partition_glob** – Glob (or sequence of globs) used to select files when
  ``directory=True``.

```python
manager = DuckCon()
with manager:
    relation = io.read_parquet(
        manager,
        Path("/data/events"),
        directory=True,
        partition_id_column="batch_id",
    )

assert set(relation.relation.project("batch_id").distinct().fetchall()) == {
    ("2024_01",),
    ("2024_02",),
}
```

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
inputs. ``source`` accepts either a single ``Path`` or any ``collections.abc.Sequence``
of ``Path`` objects, mirroring DuckDB's flexibility. ``columns`` accepts the same
mappings and sequences that DuckDB's
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

**Example**

```python
relation = io.read_json(
    manager,
    Path("events.ndjson"),
    records=True,
    maximum_depth=4,
    hive_types_autocast=True,
)
```

The helper surfaces the full keyword list so features like
`convert_strings_to_integers` are discoverable directly from IDE tooltips.

## File appenders

Previous releases exposed table-oriented append helpers under :mod:`duckplus.io`.
Those helpers have been replaced by :meth:`Relation.append_csv` and
:meth:`Relation.append_parquet`, which treat appends as direct file operations.
See :doc:`relation` for usage examples covering mutation behaviour, duplicate
avoidance, and Parquet's temporary rewrite workflow.
