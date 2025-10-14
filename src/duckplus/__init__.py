"""Duck+ public API."""

from __future__ import annotations

from importlib import metadata
from pathlib import Path
import tomllib

from . import ducktypes  # re-export typed column markers
from .cli import main as cli_main
from .connect import DuckConnection, attach_nanodbc, connect, query_nanodbc
from .core import (
    AggregateArgument,
    AggregateExpression,
    AggregateOrder,
    ColumnExpression,
    ColumnDefinition,
    AsofOrder,
    AsofSpec,
    FilterExpression,
    DuckRel,
    DuckSchema,
    ExpressionPredicate,
    col,
    column,
    equals,
    greater_than,
    greater_than_or_equal,
    JoinProjection,
    JoinSpec,
    less_than,
    less_than_or_equal,
    not_equals,
    PartitionSpec,
)
from .relation.core import Relation, RelationColumnSet
from .html import to_html
from .io import (
    append_csv,
    append_parquet,
    append_ndjson,
    read_csv,
    read_json,
    read_parquet,
    write_csv,
    write_parquet,
)
from .materialize import (
    ArrowMaterializeStrategy,
    Materialized,
    ParquetMaterializeStrategy,
)
from .odbc import (
    AccessStrategy,
    CustomODBCStrategy,
    DuckDBDsnStrategy,
    ExcelStrategy,
    IBMiAccessStrategy,
    MySQLStrategy,
    PostgresStrategy,
    SQLServerStrategy,
)
from .secrets import SecretDefinition, SecretManager, SecretRecord, SecretRegistry
from .table import DuckTable


def _load_version() -> str:
    """Return the installed distribution version.

    When Duck+ is imported from a source checkout (e.g., in tests), fall back to
    the `pyproject.toml` version so developers see the same identifier that will
    be published to PyPI.
    """

    try:
        return metadata.version("duckplus")
    except metadata.PackageNotFoundError:
        root = Path(__file__).resolve().parents[2]
        pyproject = root / "pyproject.toml"
        if not pyproject.exists():
            return "0.0.0"
        data = tomllib.loads(pyproject.read_text())
        project = data.get("project", {})
        version = project.get("version")
        return version or "0.0.0"


__version__ = _load_version()

__all__ = [
    "ArrowMaterializeStrategy",
    "append_csv",
    "append_parquet",
    "append_ndjson",
    "AggregateArgument",
    "AggregateExpression",
    "AggregateOrder",
    "ColumnExpression",
    "ColumnDefinition",
    "ducktypes",
    "AsofOrder",
    "AsofSpec",
    "FilterExpression",
    "CustomODBCStrategy",
    "DuckConnection",
    "DuckRel",
    "Relation",
    "RelationColumnSet",
    "DuckSchema",
    "DuckDBDsnStrategy",
    "DuckTable",
    "ExpressionPredicate",
    "ExcelStrategy",
    "cli_main",
    "AccessStrategy",
    "col",
    "column",
    "equals",
    "greater_than",
    "greater_than_or_equal",
    "JoinProjection",
    "JoinSpec",
    "IBMiAccessStrategy",
    "PartitionSpec",
    "Materialized",
    "ParquetMaterializeStrategy",
    "MySQLStrategy",
    "less_than",
    "less_than_or_equal",
    "not_equals",
    "PostgresStrategy",
    "read_csv",
    "read_json",
    "read_parquet",
    "SecretDefinition",
    "SecretManager",
    "SecretRecord",
    "SecretRegistry",
    "to_html",
    "write_csv",
    "write_parquet",
    "attach_nanodbc",
    "SQLServerStrategy",
    "query_nanodbc",
    "connect",
    "__version__",
]
