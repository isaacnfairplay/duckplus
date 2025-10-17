# Querying local files with DuckPlus

The DuckDB demo begins by scanning CSV and Parquet files from disk. DuckPlus
offers the same experience while returning immutable :class:`~duckplus.relation.Relation`
wrappers, which makes follow-on transformations safe to compose.

## CSV walkthrough

```python
from pathlib import Path

from duckplus import DuckCon
from duckplus import io as duckio
from duckplus.typed import ducktype

manager = DuckCon()
with manager:
    circles = duckio.read_csv(
        manager,
        Path("data/circles.csv"),
        header=True,
        auto_detect=True,
    )
    radius = ducktype.Numeric("radius")
    derived = circles.add(
        diameter=radius * 2,
        area=radius.pow(2) * 3.14159,
    )
    print(derived.relation.limit(3).fetchall())
```

The helper mirrors DuckDB's ``read_csv`` signature, so the options showcased in
the demo—``auto_detect``, ``sample_size``, ``names``, and ``na_values``—translate
directly to keyword arguments. DuckPlus validates conflicting aliases (for
example, providing both ``delimiter`` and ``delim``) before the query executes.

## Parquet walkthrough

```python
from duckplus import DuckCon
from duckplus import io as duckio
from duckplus.typed import ducktype

manager = DuckCon()
with manager:
    trips = duckio.read_parquet(
        manager,
        "data/yellow_tripdata_2023-01.parquet",
        hive_partitioning=True,
    )
    summary = (
        trips.aggregate()
        .start_agg()
        .agg(ducktype.Numeric("total_amount").sum(), alias="total_fare")
        .agg(ducktype.Numeric("trip_distance").avg(), alias="avg_distance")
        .by("passenger_count")
    )
    print(summary.relation.order("passenger_count").limit(5).fetchall())
```

DuckPlus resolves DuckDB's Parquet reader options in the same way as the demo,
including ``binary_as_string`` and ``file_row_number``. Because relations are
immutable, repeated aggregations and filters never mutate the original file
scan, preserving the safety guarantees emphasised in the earlier release guides.
