# Schema diff utilities

> **Note**
> The latest schema documentation is part of the versioned Sphinx site at
> {doc}`versions/1.2/schema_management <versions/1.2/schema_management>`.

DuckPlus surfaces lightweight helpers to audit column metadata between relations
or file-based datasets. The utilities expose the same column-order guarantees as
other DuckPlus IO wrappers and surface type drift warnings to highlight breaking
changes.

```python
from duckplus import DuckCon, Relation
from duckplus import schema

manager = DuckCon()
with manager as connection:
    reference = Relation.from_relation(
        manager,
        connection.sql("SELECT 1::INTEGER AS id, 'alpha'::VARCHAR AS name"),
    )
    candidate = Relation.from_relation(
        manager,
        connection.sql("SELECT 1::INTEGER AS id, 10::INTEGER AS score"),
    )

diff = schema.diff_relations(reference, candidate)
if diff.type_drift:
    ...  # react to column-level drift warnings
```

The returned :class:`~duckplus.schema.SchemaDiff` exposes the following
attributes:

* ``missing_from_candidate`` – columns required by the baseline relation but
  absent on the candidate.
* ``unexpected_in_candidate`` – columns that only exist on the candidate.
* ``type_drift`` – a tuple of :class:`~duckplus.schema.ColumnTypeDrift` entries
  describing per-column type changes. When drift is detected, DuckPlus emits a
  ``UserWarning`` summarising the affected columns. Pass ``warn=False`` to
  ``diff_relations`` when warnings should be suppressed.

`diff_files` mirrors the relation helper while accepting file paths instead of
relations. Set ``file_format`` to ``"csv"``, ``"parquet"``, or ``"json"`` and
optionally pass reader keyword arguments per side when schemas require the same
normalisation logic as other IO helpers:

```python
with manager:
    file_diff = schema.diff_files(
        manager,
        "data/reference.csv",
        "data/candidate.csv",
        file_format="csv",
        warn=False,
    )
```

Both helpers rely on active :class:`~duckplus.duckcon.DuckCon` connections and
reuse the stored relation metadata so comparisons remain deterministic.
