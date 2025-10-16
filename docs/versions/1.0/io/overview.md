# File and table readers

DuckPlus mirrors DuckDB's file readers while integrating with
:class:`~duckplus.duckcon.DuckCon`. Each helper expects an open manager and
returns an immutable :class:`~duckplus.relation.Relation` with cached metadata.

```python
from pathlib import Path

from duckplus import DuckCon, io

manager = DuckCon()
with manager:
    relation = io.read_csv(manager, Path("data.csv"), header=True)
    print(relation.columns)
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
    relation = io.read_csv(
        manager,
        Path("transactions.csv"),
        delimiter="|",
        header=True,
        na_values=["NA", ""],
        filename=True,
    )
```

## Parquet reader

:meth:`duckplus.io.read_parquet` mirrors DuckDB's keyword arguments, including
``union_by_name``, ``filename``, and ``hive_partitioning``. Passing a directory
with ``directory=True`` loads all ``*.parquet`` files by default.

```python
relation = io.read_parquet(
    manager,
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
parameters explicit so scripts remain self-documenting.

Consult the docstrings in :mod:`duckplus.io` for the full argument lists. When an
extension is required, DuckPlus will attempt to install it automatically or
raise an actionable message if the environment is offline.
