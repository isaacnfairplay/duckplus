# Community extension coverage

DuckPlus ships helpers for high-impact DuckDB community extensions while keeping
the API extensible. The goal is to make ``extra_extensions`` opt-ins feel as
first-class as built-in readers—installations happen automatically, metadata is
queryable, and errors point to actionable next steps.

## Nano-ODBC

`DuckCon.load_nano_odbc()` installs the nano-ODBC bundle so you can query remote
databases through DuckDB's connector. Relation constructors mirror DuckDB's
``from_odbc`` helpers and validate connection state. Use
:meth:`duckplus.relation.Relation.from_odbc_query` for ad-hoc SQL or
:meth:`duckplus.relation.Relation.from_odbc_table` for table scans—the helpers
accept parameter sequences and provide deterministic error messages when the
extension is missing.

```python
from duckplus import DuckCon, Relation

manager = DuckCon(extra_extensions=("nanodbc",))
with manager:
    customers = Relation.from_odbc_table(
        manager,
        "Driver={SQLite3};Database=/data/crm.sqlite",
        "customers",
    )
```

## Excel

:meth:`Relation.from_excel <duckplus.relation.Relation.from_excel>` wraps the
Excel extension, exposing keyword arguments such as ``sheet``, ``names``, and
``dtype``. DuckPlus installs the extension on demand and surfaces clear errors
when the environment is offline. Because the helper understands alias pairs like
``skip``/``skiprows``, you receive immediate feedback if conflicting arguments
are supplied.

## Bundled extensions

`DuckCon.extensions()` lists installed bundles along with their state so you can
confirm availability before executing queries. Helpers in :mod:`duckplus.io`
leverage this to load HTTPFS, JSON, and other bundled features transparently.
Combine it with ``extra_extensions`` when constructing :class:`DuckCon` to pin
the extension set used in production versus local development.

## Roadmap

The {file}`docs/community_extension_targets.md` file tracks upcoming wrappers for
ZIP archives, YAML ingestion, and more. Contributions that add new helpers should
prefer extending existing modules rather than rewriting them, following the
open/closed principle baked into the package design. If an extension fails to
install at runtime, DuckPlus raises a ``RuntimeError`` with the name of the
extension, the attempted install path, and a reminder to pre-install it in
air-gapped environments.
