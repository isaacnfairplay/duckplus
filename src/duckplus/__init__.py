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
from .table import DuckTable
from .materialize import (
    ArrowMaterializeStrategy,
    Materialized,
    ParquetMaterializeStrategy,
)

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
    "connect",
]
