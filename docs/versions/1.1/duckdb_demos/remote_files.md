# Querying remote files and object storage

DuckDB's online demo highlights that the engine can read HTTP and S3 resources
without downloading them first. DuckPlus keeps the workflow familiar and adds
extension guards so the immutable relations stay predictable.

```python
from duckplus import DuckCon
from duckplus import io as duckio
from duckplus.typed import ducktype

manager = DuckCon()
with manager as connection:
    connection.execute("INSTALL httpfs; LOAD httpfs;")

    nyc = duckio.read_parquet(
        manager,
        "https://storage.googleapis.com/duckdb-blobs/nyc-taxi/trips.parquet",
    )
    weekday = nyc.add(
        weekday=ducktype.Numeric.raw(
            "EXTRACT('dow' FROM pickup_datetime)",
            dependencies=["pickup_datetime"],
        ),
    )
    top_fares = weekday.aggregate(
        ("weekday",),
        total=ducktype.Numeric("total_amount").sum(),
    )
    print(top_fares.relation.order("total", desc=True).limit(5).fetchall())
```

The ``httpfs`` extension only needs to be installed once per machine. When the
remote demo uses AWS or Azure credentials, pass DuckDB's ``CONFIGURE`` options
through ``connection.execute`` before calling the reader helpers.
