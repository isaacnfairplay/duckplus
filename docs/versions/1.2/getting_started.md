# Getting started

DuckPlus wraps DuckDB with two ergonomic primitives:

- {class}`~duckplus.duckcon.DuckCon`, a context manager that owns the DuckDB
  connection lifecycle and provides extension loading utilities.
- {class}`~duckplus.relation.Relation`, an immutable wrapper around
  ``DuckDBPyRelation`` that records column metadata so validation stays strict.

Install the package directly from PyPI (recommended) or your own fork:

```bash
pip install duckplus
# or: pip install git+https://github.com/isaacnfairplay/duckplus.git
```

```{note}
DuckPlus requires Python 3.11 or newer. The package pins DuckDB 0.10.0 or newer
so typed expressions and relation helpers share the same semantics as the
underlying database.
```

### Verifying your environment

Immediately after installation, open a Python shell and run:

```python
import duckplus

with duckplus.DuckCon() as con:
    print(con.sql("SELECT 'duckplus' AS package_name, 42 AS answer").fetchall())
```

The query confirms the DuckDB bindings are discoverable and that the default
configuration can execute simple SQL. If an import fails, double-check that
DuckDB and ``duckplus`` were installed into the same virtual environment. The
package is tested on CPython 3.11–3.12 across Linux, macOS, and Windows runners.

## Opening a connection

`DuckCon` keeps connection lifecycles predictable. Use it as a context manager
when you want to rely on automatic cleanup, or call ``connect``/``close``
explicitly when integrating with existing resource managers. The class exposes
the ``is_open`` property so you can assert state during tests, and
``register_helper`` so bespoke IO helpers can be bound to the active connection
without leaking the raw ``DuckDBPyConnection``.

```python
from duckplus import DuckCon

manager = DuckCon(extra_extensions=("excel",))
with manager as con:
    assert con.sql("SELECT 42").fetchone() == (42,)
```

Providing ``extra_extensions`` ensures DuckDB community extensions are installed
and loaded before queries run. The {doc}`core/duckcon` chapter covers extension
management, the extension metadata returned by :meth:`DuckCon.extensions
<duckplus.duckcon.DuckCon.extensions>`, and strategies for reusing connections in
long-running services.

## Working with immutable relations

`Relation` wraps the ``DuckDBPyRelation`` object exposed by DuckDB, adding column
metadata and typed expression validation. The wrapper never mutates the
underlying relation; helpers such as :meth:`~duckplus.relation.Relation.add` and
:meth:`~duckplus.relation.Relation.transform` return new immutable relations so
pipelines stay easy to reason about.

```python
from duckplus import DuckCon, Relation
from duckplus.typed import ducktype
# Or opt into the experimental static API: from duckplus import static_ducktype

manager = DuckCon()
with manager as con:
    base = Relation.from_relation(
        manager,
        con.sql("SELECT 3::INTEGER AS value, 5::INTEGER AS other"),
    )

    value = ducktype.Numeric("value")
    other = ducktype.Numeric("other")

    enriched = base.add(total=value + other)
    assert enriched.columns == ("value", "other", "total")
```

Column metadata from ``base`` ensures the ``total`` expression only references
existing columns. The {doc}`core/relations` chapter documents every helper,
including aggregation, filtering, joins, schema validation utilities, exporters,
and diagnostic helpers such as :meth:`Relation.null_ratios
<duckplus.relation.Relation.null_ratios>` and :meth:`Relation.row_count
<duckplus.relation.Relation.row_count>`.

## Typed expression DSL

DuckPlus ships a fluent DSL for building SQL expressions without dropping down
to raw strings. Typed expressions track dependencies so relation helpers can
validate column references ahead of time.

```python
from duckplus.typed import ducktype
# Experimental alternative: from duckplus import static_ducktype

amount = ducktype.Numeric("amount")
discount = ducktype.Numeric("discount")

# Compose expressions as Python objects.
net_total = (amount * (1 - discount)).alias("net_total")

# Window helpers use idiomatic syntax.
running_total = amount.sum().over(partition_by=("region",), order_by=("date",))
```

Typed expressions integrate with both relation helpers and the
:mod:`duckplus.typed.select` builder described in {doc}`core/typed_expressions`.

## Next steps

1. Skim the {doc}`core/duckcon` reference to learn how to configure database
   settings, register custom helpers, and troubleshoot extension loading.
2. Follow up with {doc}`core/relations` for a catalogue of immutable data
   transformations and exporter patterns.
3. Explore {doc}`core/typed_expressions` to understand how the DSL tracks column
   dependencies, aliases, window frames, and integration with DuckDB functions.
4. Wrap up with {doc}`io/overview` and {doc}`io/file_append` to load and persist
   data efficiently.

Whenever you are unsure which helper to reach for, run ``help(duckplus.DuckCon)``
or ``help(duckplus.Relation)`` in a Python shell—the docstrings mirror the
structure of this guide.

## Documentation navigation

The rest of the guide is organised into deep dives. Jump directly to:

- {doc}`core/duckcon` for connection patterns and extension management.
- {doc}`core/relations` for immutable relation helpers.
- {doc}`core/typed_expressions` for the typed DSL, select builder, and window
  helpers.
- {doc}`io/overview` and {doc}`io/file_append` for file-backed IO wrappers.
- {doc}`community_extensions` for extension coverage and roadmap.
- {doc}`practitioner_demos` for end-to-end notebooks and language server demos.
