# DuckPlus 2.0

DuckPlus 2.0 rebuilds the project around a Protocol-first type system and a
lightweight runtime that favours fast imports. Connections own *readers* while
relations are immutable objects that expose *writers* and *appenders*.

* ✅ **Static typing without stubs** – expression helpers use Python Protocols so
  Pyright and Astral ty see the full method surface without separate ``.pyi``
  files.
* ✅ **Immutable relations** – every reader returns an immutable snapshot with
  explicit ``materialize()`` semantics and cached ``row_count`` /
  ``estimated_size_bytes`` metrics instead of ``lazy=`` flags.
* ✅ **Append policy toolkit** – CSV and Parquet appenders understand anti-join
  de-duplication, partition routing, and rollover policies out of the box.
* ✅ **Simulation helpers** – ``Relation.simulate_append_*`` returns append plans
  without touching storage so environments without optional dependencies can
  validate policies.
* ✅ **Extension autoload** – calling ``Connection.read_excel`` mirrors DuckDB’s
  autoload behaviour by retrying after ``LOAD excel`` when necessary.

```python
from pathlib import Path

from duckplus import Connection
from duckplus.io import AppendDedupe, Rollover
from duckplus.typed import (
    as_boolean_proto,
    as_numeric_proto,
    as_string_proto,
    as_temporal_proto,
    integer,
    predicate,
    timestamp,
    varchar,
)
from duckplus.typed.protocols import (
    BooleanExprProto,
    NumericExprProto,
    StringExprProto,
    TemporalExprProto,
)

conn = Connection()
people = conn.read_csv(Path("people.csv"))
name: StringExprProto = as_string_proto(varchar("name"))
is_active: BooleanExprProto = as_boolean_proto(predicate("active"))
created_at: TemporalExprProto = as_temporal_proto(timestamp("created_at"))
total_spend: NumericExprProto = as_numeric_proto(integer("lifetime_spend"))
cleaned = people.materialize()
cleaned.append_csv(
    "warehouse/people.csv",
    dedupe=AppendDedupe(mode="anti_join", keys=["id"]),
)
plan = cleaned.simulate_append_parquet(
    Path("warehouse"),
    rollover=Rollover(max_rows=1000),
)
# Plan highlights partition targets without requiring pyarrow
assert plan.backend == "parquet"
# Protocols expose boolean helpers too
assert is_active.and_(name.like("Jo%"))
# Temporal helpers flow into numeric/string Protocols
assert created_at.date_trunc("day").strftime("%Y-%m-%d")
# Numeric helpers stay visible too
assert total_spend.abs().round()
```

CSV appenders store a ``.duckplus-meta`` file with delimiter and quoting details
so future appends validate headers using the same configuration. This keeps
non-default delimiters (``;``, ``\t``) safe without guessing parser settings.

## Working with tables

``Table.insert`` accepts either a materialised ``Relation`` or a callable that
returns one. The callable path lets you defer relation construction until the
table is ready to persist rows while keeping explicit ``materialize()``
semantics:

```python
from duckplus.table import Table

table = Table("people")
table.insert(lambda: conn.from_rows(["id"], [{"id": 1}]), create=True)
table.insert(lambda: conn.from_rows(["id"], [{"id": 2}]), overwrite=True)
assert [row["id"] for row in table] == [2]
```

## Package layout

| Module | Purpose |
| --- | --- |
| ``duckplus.core`` | Connection + relation primitives, append policies, extension autoload |
| ``duckplus.io`` | Append helper classes and simulation dataclasses |
| ``duckplus.table`` | In-memory ``Table`` abstraction for inserts and relation snapshots |
| ``duckplus.typed`` | Metaclass-powered typed expressions with matching Protocols |
| ``spec`` | Frozen method specifications consumed by the metaclass |

## Docs & typing

Documentation lives under ``docs/`` with ``v1`` (archived) and ``v2`` (current)
subtrees.

* Start with ``docs/v2/runtime.md`` for the runtime layout.
* Consult ``docs/v2/migration.md`` when upgrading existing DuckPlus projects.
* Keep ``docs/v2/typed_reference.md`` nearby for a quick list of Protocols and
  factories.
* ``docs/v2/typed_expressions.md`` explains how Protocols and the metaclass
  interact so IDEs and type checkers understand runtime helpers.

The project ships ``py.typed`` so type checkers consume inline annotations. Run
``mypy duckplus`` or ``uvx ty check duckplus spec`` to verify the static surface.

## Tests

Use ``pytest`` to exercise the runtime. File-format appenders rely on optional
packages (``pyarrow``/``pandas``); tests skip gracefully when those dependencies
are missing.
