"""Duck+ public API."""

from __future__ import annotations

from .cli import main as cli_main
from .connect import DuckConnection, connect
from .core import (
    AsofOrder,
    AsofSpec,
    ColumnPredicate,
    DuckRel,
    ExpressionPredicate,
    JoinProjection,
    JoinSpec,
    PartitionSpec,
)
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
from .secrets import SecretDefinition, SecretManager, SecretRecord, SecretRegistry
from .table import DuckTable

__all__ = [
    "ArrowMaterializeStrategy",
    "append_csv",
    "append_parquet",
    "append_ndjson",
    "AsofOrder",
    "AsofSpec",
    "ColumnPredicate",
    "DuckConnection",
    "DuckRel",
    "DuckTable",
    "ExpressionPredicate",
    "cli_main",
    "JoinProjection",
    "JoinSpec",
    "PartitionSpec",
    "Materialized",
    "ParquetMaterializeStrategy",
    "read_csv",
    "read_json",
    "read_parquet",
    "SecretDefinition",
    "SecretManager",
    "SecretRecord",
    "SecretRegistry",
    "write_csv",
    "write_parquet",
    "to_html",
    "connect",
]
