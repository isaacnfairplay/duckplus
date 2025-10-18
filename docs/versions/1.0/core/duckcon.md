# Connection management with ``DuckCon``

`DuckCon` centralises DuckDB connection handling so resource management,
extension loading, and configuration changes are explicit. This guide walks
through the core features that power application backends and highlights
patterns the DuckPlus team uses in services, notebooks, and CLI utilities.

## Lifecycle management

`DuckCon` can be used either as a context manager or by calling
:meth:`~duckplus.duckcon.DuckCon.connect`/:meth:`~duckplus.duckcon.DuckCon.close`
explicitly. In both cases the class maintains a single live connection at a
time, guarding against concurrent reuse. The ``is_open`` property and explicit
error messages make it trivial to write defensive wrappers.

```python
from duckplus import DuckCon

manager = DuckCon(database="warehouse.duckdb", read_only=True)
with manager as con:
    assert con.sql("SELECT 1").fetchone() == (1,)

assert manager.connection is None  # closed automatically

try:
    with manager:
        pass
    with manager:
        pass
except RuntimeError:
    print("Double entry prevented")
```

Attempting to enter the context manager twice raises a `RuntimeError`, keeping
connection ownership clear. For CLI tools, wrap the ``with`` block in
``click.Context`` or similar frameworks so teardown happens even when commands
raise exceptions.

## Extension loading

Many DuckDB features live behind installable extensions. DuckPlus accepts the
`extra_extensions` argument to install and load them eagerly. This is
idempotent—extensions that are already installed are simply loaded.

```python
manager = DuckCon(extra_extensions=("excel",))
with manager:
    extensions = {info.name: info for info in manager.extensions()}
    excel_available = extensions.get("excel")
    print(excel_available.loaded)
```

To add an extension after the connection is open, call
``manager.connection.install_extension(...)`` directly and then re-run your
reader. DuckPlus will surface actionable errors when a download is not possible
(for example, in offline environments), allowing callers to provide fallback
paths. When you need to introspect the current state, use
:meth:`DuckCon.extensions <duckplus.duckcon.DuckCon.extensions>` to retrieve a
tuple of :class:`~duckplus.duckcon.ExtensionInfo` records with version, alias,
and installation metadata:

```python
with manager:
    for info in manager.extensions():
        print(info.name, info.installed, info.loaded)
```

Because extension installation happens inside the ``with`` block, the metadata
always reflects the current connection.

## Connection configuration

The optional ``config`` dictionary lets you set DuckDB configuration parameters
before the connection opens. DuckPlus normalises keyword arguments and defers to
DuckDB's validation logic so the API stays future-proof. Use it to toggle
settings such as ``default_null_order`` or `threads` when running analytical
workloads.

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

## Registering custom helpers

Every :class:`DuckCon` instance keeps a registry of lazily bound helper
functions. Use :meth:`DuckCon.register_helper
<duckplus.duckcon.DuckCon.register_helper>` to attach a callable that receives
the active ``DuckDBPyConnection``. DuckPlus pre-registers the packaged file
readers (``read_csv``, ``read_parquet``, ``read_json``) and extension-backed
connectors (``read_odbc_query``, ``read_odbc_table``, ``read_excel``) so you can
call them as methods on the manager or via :meth:`DuckCon.apply_helper
<duckplus.duckcon.DuckCon.apply_helper>` without importing :mod:`duckplus.io`.
Pass ``overwrite=True`` to replace these defaults with a project-specific
implementation. This registry is a handy escape hatch for environment-specific
functionality such as registering a ``parquet_scan`` macro or enabling custom
pragmas.

```python
def _apply_pragmas(connection, pragmas):
    for key, value in pragmas.items():
        connection.execute(f"PRAGMA {key}={value}")

manager.register_helper("set_pragmas", _apply_pragmas)

with manager:
    manager.apply_helper("set_pragmas", {"threads": 4, "memory_limit": "4GB"})
```

Helpers are namespaced per manager; calling :meth:`DuckCon.apply_helper
<duckplus.duckcon.DuckCon.apply_helper>` before the connection opens will raise a
clear ``RuntimeError`` pointing back to ``DuckCon.connection``.

## Testing patterns

The immutable design makes it straightforward to test connection-heavy code. In
pytest fixtures, construct a ``DuckCon`` once and yield the object rather than
the raw connection—relations created during the test will retain access to the
fixture and keep assertions honest. When you need to simulate offline behaviour,
set ``extra_extensions=()`` and monkeypatch ``connection.install_extension`` to
raise, then assert that your application emits the fallback messaging you
expect.
