# DuckPlus

DuckPlus provides a typed expression DSL and ergonomic helpers on top of DuckDB. The
library wraps DuckDB relations with immutable helpers so data practitioners can
compose transformations safely while preserving column metadata.

## Features
- **Typed expressions** – Build SQL expressions fluently using Python type
  wrappers that understand dependencies and aliases.
- **Relation helpers** – Chain immutable helpers for column management,
  filtering, aggregation, joins, IO operations, and more.
- **Extension support** – Enable DuckDB bundled and community extensions with
  simple helper methods.
- **Documentation** – Extensive guides live in `docs/` and are published at
  [isaacnfairplay.github.io/duckplus](https://isaacnfairplay.github.io/duckplus/latest/).

## Installation
```bash
pip install duckplus
```

## Quick start
```python
from duckplus import DuckCon
from duckplus.typed import ducktype

with DuckCon() as con:
    relation = con.sql("SELECT 1 AS id, 42 AS value")
    enriched = (
        relation
        .add(double_value=ducktype.Numeric("value") * 2)
        .transform(value=int)
    )
    print(enriched.to_df())
```

## Development
To work on DuckPlus locally, create a virtual environment and install the
development dependencies:

```bash
pip install -e .[dev]
```

Before opening a pull request, run the project quality checks:

```bash
mypy duckplus
pylint duckplus
pytest
```

## Contributing
Please review the roadmap in [`TODO.md`](TODO.md) to understand current
priorities. Contributions are welcome via issues and pull requests. When adding
features or bug fixes, include tests and documentation updates.

## License
DuckPlus is released under the MIT License.
