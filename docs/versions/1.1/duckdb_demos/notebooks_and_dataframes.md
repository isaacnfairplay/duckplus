# Notebook and DataFrame integration

The DuckDB demo registers Pandas DataFrames directly into SQL queries. DuckPlus
keeps the workflow minimal by exposing the managed connection while the
:class:`~duckplus.relation.Relation` wrapper protects downstream transformations.

```python
import pandas as pd

from duckplus import DuckCon, Relation
from duckplus.typed import ducktype

sales_frame = pd.DataFrame(
    {
        "region": ["north", "north", "south"],
        "amount": [1000, 1200, 800],
    }
)

manager = DuckCon()
with manager as connection:
    connection.register("sales", sales_frame)
    base = Relation.from_sql(manager, "SELECT * FROM sales")

    summary = base.aggregate(
        ("region",),
        total=ducktype.Numeric("amount").sum(),
        average=ducktype.Numeric("amount").avg(),
    )
    print(summary.relation.fetchall())
```

Because relations remain immutable, notebooks can keep the original registered
DataFrame untouched while layering multiple projections, filters, and export
calls. This mirrors the DuckDB demo's emphasis on interactivity while preserving
type safety through DuckPlus' typed expressions.
