# Working with JSON and nested data

DuckDB's demo parses JSON payloads directly into relational columns. DuckPlus
exposes the same ``read_json`` helper while keeping the typed expression API
available for schema validation and column derivations.

```python
from pathlib import Path

from duckplus import DuckCon
from duckplus import io as duckio
from duckplus.typed import ducktype

manager = DuckCon()
with manager:
    events = duckio.read_json(
        manager,
        Path("data/events.json"),
        format="auto",  # aligns with DuckDB's default demo behaviour
        records=True,
        columns={
            "n": "INTEGER",
            "metrics": "STRUCT(likes INTEGER, shares INTEGER)",
        },
    )
    metrics = events.add(
        total_engagement=ducktype.Numeric.raw(
            "metrics.likes + metrics.shares",
            dependencies=["metrics"],
        )
    )
    print(metrics.relation.fetchall())
```

DuckPlus forwards advanced options such as ``records``, ``lines``,
``maximum_depth``, and ``ignore_errors`` exactly as DuckDB documents them. Use
the typed DSL to project nested elements without losing column dependency
tracking.
