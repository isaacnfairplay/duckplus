# Connection management with ``DuckCon``

`DuckCon` centralises DuckDB connection handling so resource management, extension
loading, and configuration changes are explicit. This guide walks through the
core features that power application backends.

## Lifecycle management

`DuckCon` can be used either as a context manager or by calling
:meth:`~duckplus.duckcon.DuckCon.connect`/:meth:`~duckplus.duckcon.DuckCon.close`
explicitly. In both cases the class maintains a single live connection at a
time, guarding against concurrent reuse.

```python
from duckplus import DuckCon

manager = DuckCon(config={"default_null_order": "nulls_last"})
with manager as con:
    assert con.sql("SELECT 1").fetchone() == (1,)

assert manager.connection is None  # closed automatically
```

Attempting to enter the context manager twice raises a `RuntimeError`, keeping
connection ownership clear.

## Extension loading

Many DuckDB features live behind installable extensions. DuckPlus accepts the
`extra_extensions` argument to install and load them eagerly. This is
idempotent—extensions that are already installed are simply loaded.

```python
manager = DuckCon(extra_extensions=("httpfs", "excel"))
with manager:
    http_enabled = manager.extensions()["httpfs"].installed
```

Call :meth:`~duckplus.duckcon.DuckCon.load_extensions` at runtime to install
additional extensions. The helper surfaces actionable errors when a download is
not possible (for example, in offline environments), allowing callers to provide
fallback paths.

## Connection configuration

The optional ``config`` dictionary lets you set DuckDB configuration parameters
before the connection opens. DuckPlus normalises keyword arguments and defers to
DuckDB's validation logic so the API stays future-proof.

```python
manager = DuckCon(config={"allow_unsigned_extensions": True})
with manager as con:
    con.execute("PRAGMA verify_parallelism")
```

## Relationship with relations

Instances of :class:`~duckplus.relation.Relation` always reference the
`DuckCon` that produced them. Helpers such as
:meth:`duckplus.relation.Relation.append_parquet` validate that the originating
connection is still open before issuing writes, preventing confusing runtime
errors.

Because the relation wrapper stores the manager reference, you can share
connections across helpers without dealing with raw DuckDB handles. See the
[immutable relation helper guide](relations.md) for deeper coverage of the
immutable relation APIs.
