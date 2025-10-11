"""Duck+ public API."""

from __future__ import annotations

from .connect import DuckConnection, connect
from .core import (
    AsofOrder,
    AsofSpec,
    ColumnPredicate,
    DuckRel,
    ExpressionPredicate,
    JoinProjection,
    JoinSpec,
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
    "AsofOrder",
    "AsofSpec",
    "ColumnPredicate",
    "DuckConnection",
    "DuckRel",
    "DuckTable",
    "ExpressionPredicate",
    "JoinProjection",
    "JoinSpec",
    "Materialized",
    "ParquetMaterializeStrategy",
    "SecretDefinition",
    "SecretManager",
    "SecretRecord",
    "SecretRegistry",
    "connect",
]
