# File and table readers

DuckPlus mirrors DuckDB's file readers while integrating with
:class:`~duckplus.duckcon.DuckCon`. Each helper expects an open manager and
returns an immutable :class:`~duckplus.relation.Relation` with cached metadata.
The functions—covering file readers and extension-backed connectors—live in
:mod:`duckplus.io` and register automatically on every ``DuckCon`` instance, so
you can call them directly from the connection manager without importing the
module. They intentionally avoid ``**kwargs`` so editor completions surface
every DuckDB option.

```python
from pathlib import Path

from duckplus import DuckCon

manager = DuckCon()
with manager:
    relation = manager.read_csv(Path("data.csv"), header=True)
    print(relation.columns)
```

Because the helpers register automatically, persisting results is just as easy
when chaining to the relation-level writers:

```python
with manager:
    relation = manager.read_parquet(Path("data.parquet"))
    relation.append_csv(Path("report.csv"))
    relation.write_parquet_dataset(
        Path("dataset"),
        partition_column="country",
    )
```

## CSV reader

:meth:`duckplus.io.read_csv` exposes DuckDB's table-function keywords without
using ``**kwargs`` so IDEs surface every option. Aliases such as ``delim`` and
``quote`` match DuckDB's own names, and DuckPlus raises a descriptive error when
both the canonical and alias form are supplied for the same argument.

Key options include:

- ``columns`` and ``dtype`` for explicit column typing.
- ``names`` and ``na_values`` to override column names and null sentinels.
- ``filename=True`` to append the absolute path of each input file.

```python
with manager:
    relation = manager.read_csv(
        Path("transactions.csv"),
        delimiter="|",
        header=True,
        na_values=["NA", ""],
        filename=True,
    )
```

Pass ``lazy=True`` to stream large CSVs lazily—DuckPlus will propagate the
parameter to DuckDB, allowing you to chain transformations before triggering
materialisation.

## Parquet reader

:meth:`duckplus.io.read_parquet` mirrors DuckDB's keyword arguments, including
``union_by_name``, ``filename``, and ``hive_partitioning``. Passing a directory
with ``directory=True`` loads all ``*.parquet`` files by default.

```python
with manager:
    relation = manager.read_parquet(
        [
            Path("/data/sales_2024.parquet"),
            Path("/data/sales_2025.parquet"),
        ],
        union_by_name=True,
        filename=True,
    )
```

## JSON, Arrow, and database connectors

The IO module extends to DuckDB's JSON readers, Arrow integration, and
community-extension backed connectors like Excel and nano-ODBC. Each helper keeps
parameters explicit so scripts remain self-documenting. Highlights include:

- :func:`duckplus.io.read_json` for line-delimited JSON, with ``maximum_depth``
  and ``format`` options mirroring DuckDB's table function.
- :func:`duckplus.io.read_arrow` for zero-copy reads from Arrow datasets or
  ``pyarrow.dataset.Dataset`` objects.
- :func:`duckplus.io.read_odbc_query` and :func:`duckplus.io.read_odbc_table`
  for nano-ODBC queries and scans.
- :func:`duckplus.io.read_excel` for Excel workbooks, which will automatically
  install the ``excel`` extension when missing.

Consult the docstrings in :mod:`duckplus.io` for the full argument lists. When an
extension is required, DuckPlus will attempt to install it automatically or
raise an actionable message if the environment is offline. Call
``manager.apply_helper("read_csv", ...)`` to route through the bound helper
directly, or pass
``overwrite=True`` to :meth:`DuckCon.register_helper
<duckplus.duckcon.DuckCon.register_helper>` if you need to replace the defaults
with a custom implementation.

## Composing with custom helpers

:class:`DuckCon <duckplus.duckcon.DuckCon>` exposes
:meth:`~duckplus.duckcon.DuckCon.register_helper` and
:meth:`~duckplus.duckcon.DuckCon.apply_helper` so you can wrap bespoke data
sources. Register a callable that accepts the open connection, then return a
DuckPlus relation to remain within the immutable flow:

```python
def read_yaml(connection, path):
    return connection.sql("SELECT * FROM read_json(? ::VARCHAR)", [str(path)])

manager.register_helper("read_yaml", read_yaml)

with manager:
    relation = manager.apply_helper("read_yaml", Path("data.yaml"))
```

The returned relation captures metadata like any built-in reader, so downstream
validation and schema utilities continue to work.
