# Append policy toolkit

Append flows coordinate multiple concerns so analysts can chain behaviour without
shelling out to custom scripts.

## De-duplication

Use ``duckplus.io.AppendDedupe`` to drop rows already present in the target
artifact. Anti-join de-duplication defaults to using all columns but can be
restricted to specific keys.

```python
from duckplus.io import AppendDedupe

policy = AppendDedupe(mode="anti_join", keys=["id"])
```

## Partition routing

``duckplus.io.Partition`` writes records into per-partition directories (column or
hash routing) before handing them to the storage backend.

## Rollover

``duckplus.io.Rollover`` keeps directory-based appends manageable by rolling over
once a file reaches a row or size threshold. The helper simply chooses the next
available filename so no metadata registries are required.

CSV appenders persist a sidecar ``.duckplus-meta`` file recording the delimiter
and quoting configuration. Header validation reads that metadata before checking
existing files so custom delimiters (for example ``;`` or ``\t``) continue to
round-trip safely during subsequent appends.

The relation methods apply these helpers uniformly across CSV, JSON, and Parquet
appenders. Parquet append defaults to directory layouts; setting
``force_single_file=True`` enables single-file appends and logs a warning.

## Simulation helpers

Use ``Relation.simulate_append_csv`` and
``Relation.simulate_append_parquet`` to inspect the append plan without touching
the filesystem. The simulator mirrors partition routing and rollover decisions
and records planned paths, created files, and whether de-duplication trimmed any
rows.

```python
from duckplus.core import Connection
from duckplus.io import AppendDedupe, Rollover

conn = Connection()
relation = conn.from_rows(["id", "name"], [{"id": 1, "name": "Ada"}])

plan = relation.simulate_append_parquet(
    "warehouse", dedupe=AppendDedupe(mode="anti_join"), rollover=Rollover(max_rows=100)
)
for action in plan.actions:
    print(action.path, action.rows_to_append)
```

Simulations operate entirely with the standard library. Parquet de-duplication
requires ``pyarrow`` at runtime, so the simulation records a note when the
existing-row scan is skipped.
